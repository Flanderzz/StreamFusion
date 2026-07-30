package io.github.jordepic.streamfusion.planner;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.planner.hint.StateTtlHint;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCalc;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalChangelogNormalize;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCorrelate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExpand;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGlobalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGlobalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGroupWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalIntervalJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLimit;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLocalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLocalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLookupJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalMiniBatchAssigner;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalOverAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRank;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSortLimit;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTemporalJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTemporalSort;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalUnion;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWatermarkAssigner;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowDeduplicate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowRank;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowTableFunction;
import org.apache.flink.table.planner.plan.optimize.program.FlinkOptimizeProgram;
import org.apache.flink.table.planner.plan.optimize.program.StreamOptimizeContext;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.trait.MiniBatchInterval;
import org.apache.flink.table.planner.plan.trait.MiniBatchIntervalTraitDef$;
import org.apache.flink.table.planner.plan.trait.MiniBatchMode;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.table.planner.plan.utils.RankProcessStrategy;
import org.apache.flink.table.planner.utils.ShortcutUtils;

/**
 * Optimizer program appended after the host engine's physical optimization. It rewrites the
 * optimized streaming physical plan, replacing supported operators with native ones and leaving
 * everything else for the host engine to execute, the planner-level counterpart to how batch
 * accelerators inject a post-optimization rewrite.
 *
 * <p>Only operators the native side reproduces exactly are substituted, so results are unchanged
 * and unsupported plans fall back cleanly.
 *
 * <p>Which operators those are is declared by {@link #REGISTRY}, one {@link Substitution} per host
 * shape in the order they are offered a node. Order is still semantic — two entries can share a
 * shape, and the entries split around the insert-only guard — but each entry now carries its own
 * config gate, changelog-safety and fallback reason rather than encoding them in its position.
 */
public final class PhysicalPlanScan implements FlinkOptimizeProgram<StreamOptimizeContext> {

  private final List<String> operatorTypes = new ArrayList<>();
  private final List<String> fallbackReasons = new ArrayList<>();
  private int substitutions;

  // When set (-Dstreamfusion.logFallbackReasons=true), each fallback reason is logged at plan time,
  // mirroring Comet's COMET_LOG_FALLBACK_REASONS. Reasons are always collected for fallbackReasons().
  private static final boolean LOG_FALLBACK_REASONS =
      Boolean.getBoolean("streamfusion.logFallbackReasons");

  // The Flink distribution intentionally does not ship every connector or format. Connector-specific
  // rewrites live in optional StreamFusion extension JARs and must never be linked by the core image.
  private static final String KAFKA_EXTENSION =
      "io.github.jordepic.streamfusion.planner.KafkaTables";
  private static final String KAFKA_OFFSETS_INITIALIZER =
      "org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer";
  private static final String FLUSS_EXTENSION =
      "io.github.jordepic.streamfusion.planner.FlussTables";
  private static final String FLUSS_TABLE_SOURCE = "org.apache.fluss.flink.source.FlinkTableSource";
  private static final String PARQUET_EXTENSION =
      "io.github.jordepic.streamfusion.planner.ParquetSourceMatcher";

  private static final boolean KAFKA_AVAILABLE =
      extensionAvailable(KAFKA_EXTENSION, KAFKA_OFFSETS_INITIALIZER);
  private static final boolean FLUSS_AVAILABLE =
      extensionAvailable(FLUSS_EXTENSION, FLUSS_TABLE_SOURCE);
  private static final boolean PARQUET_AVAILABLE = extensionAvailable(PARQUET_EXTENSION);

  // The row/local window-aggregate path matches several variants (tumbling/hopping/cumulative
  // local) with extra gates, so a precise per-condition reason would be unreliable; keep a coarse
  // operator-level reason naming the requirements.
  private static final String WINDOW_AGGREGATE_REASON =
      "window aggregate: needs an event-time TUMBLE/HOP/CUMULATE (zero offset) over a"
          + " local-time-zone or plain TIMESTAMP rowtime, one value column whose type matches the aggregate"
          + " (bigint/int/double for SUM/AVG, also smallint/tinyint/float for MIN/MAX/COUNT),"
          + " and bigint/int/string/boolean/date keys (docs/aggregate-type-support.md)";

  private static final List<Substitution<?>> REGISTRY = buildRegistry();

  @Override
  public RelNode optimize(RelNode root, StreamOptimizeContext context) {
    record(root);
    // Master switch: with native acceleration off, substitute nothing — the query runs on the host.
    if (!NativeConfig.nativeEnabled()) {
      return root;
    }
    // Pass 1 substitutes native (columnar) operators.
    RelNode substituted = rewrite(root, new PlanContext(this, KAFKA_AVAILABLE));
    // Whole-query all-or-nothing: every native operator but a source/sink is Arrow → Arrow.
    // If any operator other than a source (a leaf) or the sink (the plan root) is still row-wise, the
    // query cannot run as one columnar island, so accelerate nothing — it runs as stock Flink. The only
    // row-wise operator allowed is a rowwise source/sink, bridged by a transpose at the perimeter.
    if (substitutions > 0 && !fullyColumnar(substituted, true)) {
      substitutions = 0; // reasons stay recorded for reporting; nothing is substituted
      return root;
    }
    // Pass 2 inserts a row↔columnar transpose at each perimeter edge (rowwise source/sink ↔ island).
    // Pass 3 deduplicates identical native sources into one shared instance, so a multi-view query
    // reads and decodes its topic once — the columnar counterpart of Flink's sub-plan reuse, which
    // the digest barriers deliberately keep away from native nodes.
    return shareIdenticalSources(insertTransitions(substituted));
  }

  // ---------------------------------------------------------------------------- substitution chain

  /**
   * Every substitution the scan can make, in the order a node is offered to them. Entries marked
   * {@link Substitution#changelogSafe()} sit before the insert-only guard; the rest are only offered
   * insert-only nodes. Two entries sharing a shape are tried in list order (a rank is deduplication
   * before Top-N; a Calc is filter-only before general).
   *
   * <p>An optional connector's entries are built only when its extension is linked: naming those
   * matchers from an unconditional initializer would resolve them at class load, turning a Flink
   * distribution without the connector into a linkage error instead of a clean fallback.
   */
  private static List<Substitution<?>> buildRegistry() {
    List<Substitution<?>> entries = new ArrayList<>();

    // A sink is terminal, so the changelog guard (which protects operator substitution within a
    // stream) does not apply; it is eligible as long as its input is insert-only.
    if (KAFKA_AVAILABLE) {
      entries.add(kafkaSinkSubstitution());
    }
    if (PARQUET_AVAILABLE) {
      entries.add(parquetSinkSubstitution());
    }

    // A non-windowed GROUP BY both emits and consumes a changelog, so it is exempt from the
    // insert-only guard — its input may be insert-only or itself a changelog.
    entries.add(
        Substitution.of(
                StreamPhysicalGroupAggregate.class,
                "groupAggregate",
                PhysicalPlanScan::planGroupAggregate)
            .matching(GroupAggregateMatcher::matches)
            .reason(GroupAggregateMatcher::unsupportedReason)
            .changelogSafe());

    // The global half of a two-phase non-windowed GROUP BY. It merges the local half's partials into
    // the final per-key result and emits a changelog exactly like the single-phase GROUP BY above —
    // so it reuses the same native group-aggregate operator, fed positional partial columns (COUNT
    // merges as a SUM over its partial counts). Exempt from the insert-only guard for the same reason.
    entries.add(
        Substitution.of(
                StreamPhysicalGlobalGroupAggregate.class,
                "groupAggregate",
                PhysicalPlanScan::planGlobalGroupAggregate)
            .matching(GlobalGroupAggregateMatcher::matches)
            .reason(GlobalGroupAggregateMatcher::unsupportedReason)
            .changelogSafe());

    // The MiniBatchAssigner emits the mini-batch marker that drives the local aggregate's bundle
    // flush. Substitute a native columnar assigner that forwards Arrow and emits the same marker
    // watermark, so the whole island shares one mini-batch cadence — matching Flink's
    // ProcTimeMiniBatchAssignerOperator (proc-time: markers generated from the clock) or
    // RowTimeMiniBatchAssginerOperator (row-time: upstream event-time watermarks filtered to the
    // interval) + MapBundleOperator wiring.
    entries.add(
        Substitution.of(
                StreamPhysicalMiniBatchAssigner.class, PhysicalPlanScan::planMiniBatchAssigner)
            .changelogSafe());

    // A regular (non-windowed) join emits a changelog and consumes one on either side, so it is
    // exempt from the insert-only guard (like the GROUP BY above).
    entries.add(
        Substitution.of(
                StreamPhysicalJoin.class, "updatingJoin", PhysicalPlanScan::planRegularJoin)
            .matching(RegularJoinMatcher::matches)
            .reason(RegularJoinMatcher::unsupportedReason)
            .changelogSafe());

    // Row-time deduplication is a rowtime-ordered rank-1 the host plans as a row-time deduplicate:
    // keep-first (ASC — insert-only and watermark-released, except under mini-batch where Flink
    // plans it as its bundled retracting function) or keep-last (DESC, retracting, emits eagerly).
    // Either way it requires an insert-only input. Offered before Top-N — both are
    // StreamPhysicalRank, but a rowtime-ordered rank is deduplication, which TopNMatcher declines.
    entries.add(
        Substitution.of(StreamPhysicalRank.class, "deduplicate", PhysicalPlanScan::planDeduplicate)
            .matching(
                rank ->
                    DeduplicateMatcher.matches(rank)
                        && ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) rank.getInput()))
            .explaining(DeduplicateMatcher::isTimeOrder)
            .reason(DeduplicateMatcher::unsupportedReason)
            .changelogSafe());

    // A streaming Top-N emits a changelog (it deletes a row when one is displaced), so it is exempt
    // from the insert-only guard. An insert-only input uses the append-only ranker; a changelog
    // input uses the retracting ranker (Flink's RetractableTopNFunction), which keeps the full buffer
    // so a deleted top-N row can be replaced by promoting rank N+1.
    entries.add(
        Substitution.of(StreamPhysicalRank.class, "topN", PhysicalPlanScan::planTopN)
            .matching(TopNMatcher::matches)
            .reason(TopNMatcher::unsupportedReason)
            .changelogSafe());

    // A global FETCH/LIMIT — ORDER BY … LIMIT n (StreamPhysicalSortLimit) or plain LIMIT n
    // (StreamPhysicalLimit). Both lower to a global (no-partition) ROW_NUMBER rank, so they reuse the
    // native columnar Top-N operator with an empty partition key: the sort-limit carries the order
    // keys and emits a changelog as the top set changes; the plain limit has no sort keys, so the
    // ranker keeps the first n rows by arrival (the newest beyond n never enters — insert-only). Like
    // the Top-N above it emits a changelog, so it is changelog-safe and requires an insert-only input
    // (only the append-only ranker is implemented; a retracting input falls back). It always reports:
    // a sort-limit emits a changelog, so it would otherwise slip past the insert-only guard
    // unreported, leaving a non-accelerating query unable to explain itself (ticket 29).
    entries.add(
        Substitution.of(StreamPhysicalSortLimit.class, PhysicalPlanScan::planLimit).changelogSafe());
    entries.add(
        Substitution.of(StreamPhysicalLimit.class, PhysicalPlanScan::planLimit).changelogSafe());

    // A CDC changelog source (Debezium/OGG) emits a changelog itself: the native decode operator turns
    // each message into physical rows carrying their RowKind on $row_kind$ (an update fans out to
    // UPDATE_BEFORE + UPDATE_AFTER), reproducing Flink's CDC source exactly. Like the GROUP BY/join/Top-N
    // above, it is therefore exempt from the insert-only guard. (Append decode formats — JSON via
    // the native source, CSV/raw via the insert-only decode branch below — are insert-only and handled
    // after the guard.)
    if (KAFKA_AVAILABLE) {
      entries.add(cdcDecodeSubstitution());
      entries.add(cdcWatermarkReport());
    }

    // A Calc transforms each row independently — a per-row projection plus an optional deterministic
    // filter — and the native operator carries the `$row_kind$` tag through unchanged, so it is
    // changelog-safe and (like the GROUP BY/join/Top-N/CDC above) exempt from the insert-only guard:
    // it matches the host's Calc over a retracting stream row for row.
    entries.add(
        Substitution.of(StreamPhysicalCalc.class, "filter", PhysicalPlanScan::planFilterCalc)
            .matching(FilterCalcMatcher::matches)
            .reason(PhysicalPlanScan::calcReason)
            .changelogSafe());
    entries.add(
        Substitution.of(StreamPhysicalCalc.class, "calc", PhysicalPlanScan::planCalc)
            .matching(CalcMatcher::matches)
            .reason(PhysicalPlanScan::calcReason)
            .changelogSafe());

    // Changelog normalization (upsert / duplicate-bearing source → regular changelog): keep the last
    // row per unique key, emitting INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE. Both consumes and emits a
    // changelog, so (like the GROUP BY) it is exempt from the insert-only guard. The keyed
    // shuffle (by the unique key) stays columnar where the input sits on a columnar producer.
    entries.add(
        Substitution.of(
                StreamPhysicalChangelogNormalize.class,
                "changelogNormalize",
                PhysicalPlanScan::planChangelogNormalize)
            .matching(ChangelogNormalizeMatcher::matches)
            .reason(ChangelogNormalizeMatcher::unsupportedReason)
            .changelogSafe());

    // INNER UNNEST of an array (Flink's Correlate over $UNNEST_ROWS$): fan each row out to one row
    // per element of its array column, appending the element. Stateless and changelog-transparent
    // (the `$row_kind$` tag rides through), so — like Expand — it is exempt from the insert-only
    // guard.
    entries.add(
        Substitution.of(StreamPhysicalCorrelate.class, "unnest", PhysicalPlanScan::planUnnest)
            .matching(UnnestMatcher::matches)
            .reason(UnnestMatcher::unsupportedReason)
            .changelogSafe());

    // GROUPING SETS / CUBE / ROLLUP expansion: fan each row out to one row per grouping set (copy
    // grouped-in columns, null grouped-out ones, stamp the expand id), feeding the downstream native
    // GROUP BY over the keys plus the expand-id column. Stateless and changelog-transparent (the
    // `$row_kind$` tag rides through), so — like the Calc/union — it is exempt from the insert-only
    // guard and runs over either insert-only or changelog input.
    entries.add(
        Substitution.of(StreamPhysicalExpand.class, "expand", PhysicalPlanScan::planExpand)
            .matching(ExpandMatcher::matches)
            .reason(ExpandMatcher::unsupportedReason)
            .changelogSafe());

    // A UNION ALL is a pure stream merge — every input record flows through unchanged, with no
    // per-row work and no shuffle. It never touches the `$row_kind$` tag, so (like the Calc/GROUP
    // BY/join above) it is changelog-transparent and exempt from the insert-only guard: it
    // matches the host's union row for row over either insert-only or retracting inputs. The native
    // node carries no operator — it lowers to a UnionTransformation over the inputs' Arrow streams.
    entries.add(
        Substitution.of(StreamPhysicalUnion.class, "union", PhysicalPlanScan::planUnion)
            .matching(UnionMatcher::matches)
            .reason(UnionMatcher::unsupportedReason)
            .changelogSafe());

    // ---- everything below the insert-only guard: native operators here emit insert-only rows ----

    if (PARQUET_AVAILABLE) {
      entries.add(parquetSourceSubstitution());
    }
    if (FLUSS_AVAILABLE) {
      entries.add(flussSourceSubstitution());
    }
    if (KAFKA_AVAILABLE) {
      entries.add(kafkaSourceSubstitution());
      entries.add(kafkaDecodeSubstitution());
      entries.add(appendWatermarkReport());
    }

    // Substitute a watermark assigner only when its (already-rewritten) input is columnar — i.e. it
    // sits on a native source/calc. Otherwise it is a pass-through that would be wrapped in two
    // transposes for no gain, so leave it on the host.
    entries.add(
        Substitution.of(
                StreamPhysicalWatermarkAssigner.class,
                "watermark",
                PhysicalPlanScan::planWatermarkAssigner)
            .matching(
                wm ->
                    wm.getInputs().get(0) instanceof ColumnarOutput
                        && WatermarkAssignerMatcher.matches(wm)));

    // Event-time sort (ORDER BY rowtime): buffer rows, release them in rowtime order as the watermark
    // advances. Insert-only. Its single (gather) exchange becomes a native columnar exchange with no
    // key (an empty key list, like the non-partitioned OVER), so the whole thing stays columnar.
    entries.add(
        Substitution.of(
                StreamPhysicalTemporalSort.class,
                "temporalSort",
                PhysicalPlanScan::planTemporalSort)
            .matching(TemporalSortMatcher::matches)
            .reason(TemporalSortMatcher::unsupportedReason));

    // A windowing table function assigns each row to its window(s) and appends
    // window_start/window_end/window_time — a stateless per-row map, so it is columnar in and out and
    // never appears fused into a window aggregate (Flink collapses TVF + windowed GROUP BY into one
    // node); it survives standalone only feeding a window join/Top-N. Its rewritten input is wrapped
    // by the transition pass at the perimeter (the TVF does not shuffle, so no keyed exchange here).
    entries.add(
        Substitution.of(
                StreamPhysicalWindowTableFunction.class,
                "windowTableFunction",
                PhysicalPlanScan::planWindowTableFunction)
            .matching(WindowTableFunctionMatcher::matches)
            .reason(WindowTableFunctionMatcher::unsupportedReason));

    // Window Top-N over a windowing-TVF input: per window and partition key, keep the top-N rows by
    // the order key and emit them when a watermark closes the window. Append-only; the keyed shuffle
    // (or single gather when there is no partition key) stays columnar via columnarInput.
    entries.add(
        Substitution.of(
                StreamPhysicalWindowRank.class, "windowRank", PhysicalPlanScan::planWindowRank)
            .matching(WindowRankMatcher::matches)
            .reason(WindowRankMatcher::unsupportedReason));

    // Window deduplication: the limit=1 case of window Top-N (keep-first/last by rowtime per window
    // and key), reusing the same native window-rank operator with a single rowtime sort column.
    entries.add(
        Substitution.of(
                StreamPhysicalWindowDeduplicate.class,
                "windowRank",
                PhysicalPlanScan::planWindowDeduplicate)
            .matching(WindowDeduplicateMatcher::matches)
            .reason(WindowDeduplicateMatcher::unsupportedReason));

    entries.add(
        Substitution.of(
                StreamPhysicalWindowAggregate.class,
                "windowAggregate",
                PhysicalPlanScan::planWindowAggregate)
            .matching(
                agg ->
                    WindowAggregateMatcher.matches(
                        agg.windowing(),
                        agg.grouping(),
                        agg.aggCalls(),
                        agg.getInput().getRowType()))
            .reason(agg -> WINDOW_AGGREGATE_REASON));
    entries.add(
        Substitution.of(
                StreamPhysicalWindowAggregate.class, PhysicalPlanScan::planSessionWindowAggregate)
            .matching(
                agg ->
                    WindowAggregateMatcher.matchesSession(
                        agg.windowing(),
                        agg.grouping(),
                        agg.aggCalls(),
                        agg.getInput().getRowType())));

    // The legacy SESSION group-window aggregate (GROUP BY k, SESSION(rowtime, INTERVAL g)) — a
    // different operator from the windowing-TVF window aggregate, but its output layout matches the
    // native session operator's, so it routes to the same operator.
    entries.add(
        Substitution.of(
                StreamPhysicalGroupWindowAggregate.class,
                PhysicalPlanScan::planGroupWindowSession)
            .matching(GroupWindowSessionMatcher::matches)
            .reason(GroupWindowSessionMatcher::unsupportedReason));

    // The local half of a two-phase non-windowed GROUP BY: a stateless per-batch pre-aggregate that
    // emits partials for the global half to merge. Insert-only (append-only partials), so it sits
    // after the guard. Its input feeds directly (no shuffle precedes a local — the keyed exchange sits
    // between the local and the global); the transition pass transposes below only if rowwise.
    entries.add(
        Substitution.of(
                StreamPhysicalLocalGroupAggregate.class,
                "localGroupAggregate",
                PhysicalPlanScan::planLocalGroupAggregate)
            .matching(LocalGroupAggregateMatcher::matches)
            .reason(
                agg ->
                    "local group aggregate: needs SUM/MIN/MAX/COUNT over bigint/int/double values"
                        + " with no widening of the partial, or AVG over any AvgAggFunction numeric,"
                        + " and bigint/int/string/boolean/date/timestamp/decimal grouping keys"));

    entries.add(
        Substitution.of(
                StreamPhysicalLocalWindowAggregate.class,
                "localWindowAggregate",
                PhysicalPlanScan::planLocalWindowAggregate)
            .matching(agg -> localWindowVariant(agg) != null)
            .reason(agg -> WINDOW_AGGREGATE_REASON));

    entries.add(
        Substitution.of(StreamPhysicalOverAggregate.class, "over", PhysicalPlanScan::planOver)
            .matching(OverAggregateMatcher::matches)
            .reason(OverAggregateMatcher::unsupportedReason));

    entries.add(
        Substitution.of(
                StreamPhysicalIntervalJoin.class,
                "intervalJoin",
                PhysicalPlanScan::planIntervalJoin)
            .matching(IntervalJoinMatcher::matches)
            .reason(IntervalJoinMatcher::unsupportedReason));

    entries.add(
        Substitution.of(
                StreamPhysicalWindowJoin.class, "windowJoin", PhysicalPlanScan::planWindowJoin)
            .matching(WindowJoinMatcher::matches)
            .reason(WindowJoinMatcher::unsupportedReason));

    entries.add(
        Substitution.of(
                StreamPhysicalTemporalJoin.class,
                "temporalJoin",
                PhysicalPlanScan::planTemporalJoin)
            .matching(TemporalJoinMatcher::matches)
            .reason(TemporalJoinMatcher::unsupportedReason));

    entries.add(
        Substitution.of(
                StreamPhysicalLookupJoin.class, "lookupJoin", PhysicalPlanScan::planLookupJoin)
            .matching(LookupJoinMatcher::matches)
            .reason(LookupJoinMatcher::unsupportedReason));

    entries.add(
        Substitution.of(
                StreamPhysicalGlobalWindowAggregate.class,
                "globalWindowAggregate",
                PhysicalPlanScan::planGlobalWindowAggregate)
            .matching(GlobalWindowAggregateMatcher::matches)
            .reason(GlobalWindowAggregateMatcher::unsupportedReason));

    return List.copyOf(entries);
  }

  private RelNode rewrite(RelNode node, PlanContext ctx) {
    List<RelNode> inputs = new ArrayList<>(node.getInputs().size());
    boolean changed = false;
    for (RelNode input : node.getInputs()) {
      RelNode rewritten = rewrite(input, ctx);
      inputs.add(rewritten);
      changed |= rewritten != input;
    }
    RelNode current = changed ? node.copy(node.getTraitSet(), inputs) : node;

    RelNode changelogSafe = apply(current, ctx, true);
    if (changelogSafe != null) {
      return changelogSafe;
    }
    // Native operators emit insert-only rows; substituting into a retracting or updating stream
    // would drop changelog semantics, so only insert-only nodes are eligible. A changelog-emitting
    // candidate reaching this point was declined by its matcher above — record why before bailing,
    // or its reason (unlike an insert-only candidate's, noted at the end) would be lost.
    if (!(current instanceof StreamPhysicalRel)
        || !ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) current)) {
      noteFallback(current);
      return current;
    }
    RelNode insertOnly = apply(current, ctx, false);
    if (insertOnly != null) {
      return insertOnly;
    }
    // A recognized operator shape we reached here is one its matcher declined — record why, so a
    // query that does not accelerate can explain itself (ticket 29) instead of falling back silently.
    noteFallback(current);
    return current;
  }

  /**
   * Offers {@code current} to every entry on one side of the insert-only guard, returning the first
   * outcome that settles it — a native replacement, or the node itself where an entry owned it and
   * reported why it declined. Null means no entry claimed it.
   */
  private static RelNode apply(RelNode current, PlanContext ctx, boolean changelogSafe) {
    for (Substitution<?> substitution : REGISTRY) {
      if (substitution.isChangelogSafe() != changelogSafe) {
        continue;
      }
      RelNode outcome = substitution.apply(current, ctx);
      if (outcome != null) {
        return outcome;
      }
    }
    return null;
  }

  // -------------------------------------------------------------------- optional-connector entries

  private static Substitution<StreamPhysicalSink> kafkaSinkSubstitution() {
    return Substitution.of(StreamPhysicalSink.class, "kafkaSink", PhysicalPlanScan::planKafkaSink)
        .matching(KafkaSinkMatcher::appliesTo)
        .changelogSafe();
  }

  private static RelNode planKafkaSink(StreamPhysicalSink sink, PlanContext ctx) {
    KafkaSinkMatcher.Planned planned = KafkaSinkMatcher.plan(sink);
    if (planned.fallbackReason != null) {
      ctx.decline("kafka sink: " + planned.fallbackReason);
      return null;
    }
    if (!planned.upsert
        && !ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) sink.getInputs().get(0))) {
      ctx.decline("kafka sink: the input is a changelog, not an insert-only stream");
      return null;
    }
    return new StreamPhysicalNativeKafkaSink(
        sink.getCluster(),
        sink.getTraitSet(),
        sink.getInputs().get(0),
        sink.getRowType(),
        planned);
  }

  private static Substitution<StreamPhysicalSink> parquetSinkSubstitution() {
    return Substitution.of(StreamPhysicalSink.class, PhysicalPlanScan::planParquetSink)
        .matching(ParquetSinkMatcher::appliesTo)
        .changelogSafe();
  }

  private static RelNode planParquetSink(StreamPhysicalSink sink, PlanContext ctx) {
    if (!ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) sink.getInputs().get(0))) {
      ctx.decline("parquet sink: the input is a changelog, not an insert-only stream");
      return null;
    }
    if (!NativeConfig.operatorEnabled("parquetSink")) {
      ctx.decline(Substitution.disabledReason("parquetSink"));
      return null;
    }
    ParquetSinkMatcher.Planned planned = ParquetSinkMatcher.plan(sink);
    if (planned.fallbackReason != null) {
      ctx.decline("parquet sink: " + planned.fallbackReason);
      return null;
    }
    return new StreamPhysicalNativeParquetSink(
        sink.getCluster(),
        sink.getTraitSet(),
        sink.getInputs().get(0),
        sink.getRowType(),
        planned);
  }

  private static Substitution<StreamPhysicalTableSourceScan> parquetSourceSubstitution() {
    return Substitution.of(
            StreamPhysicalTableSourceScan.class,
            "parquetSource",
            (scan, ctx) ->
                new StreamPhysicalNativeParquetSource(
                    scan.getCluster(),
                    scan.getTraitSet(),
                    scan.getRowType(),
                    ParquetSourceMatcher.path(scan),
                    ParquetSourceMatcher.utcTimestamp(scan)))
        .matching(ParquetSourceMatcher::matches);
  }

  /**
   * A Fluss scan the native source cannot serve yields rather than stopping: it records its reason
   * and lets the remaining source entries look at the same scan.
   */
  private static Substitution<StreamPhysicalTableSourceScan> flussSourceSubstitution() {
    return Substitution.of(StreamPhysicalTableSourceScan.class, PhysicalPlanScan::planFlussSource)
        .matching(
            scan -> {
              Map<String, String> options = FilesystemTables.options(scan);
              boolean connectorOption = options != null && "fluss".equals(options.get("connector"));
              return (connectorOption || isFlussTableSource(scan))
                  && NativeConfig.operatorEnabled("flussSource");
            })
        .yieldingOnDecline();
  }

  private static RelNode planFlussSource(StreamPhysicalTableSourceScan scan, PlanContext ctx) {
    String fallback = FlussTables.fallbackReason(scan);
    if (fallback != null) {
      ctx.decline("fluss source: " + fallback);
      return null;
    }
    return new StreamPhysicalNativeFlussSource(
        scan.getCluster(), scan.getTraitSet(), scan.getRowType(), scan);
  }

  /**
   * The native rdkafka source consumes and decodes in one place: the installed format provider's
   * decoder runs inside the poll, so the source emits typed batches — and, because it therefore
   * holds decoded rowtimes, it regenerates a pushed WATERMARK per split (Flink's own min
   * combination and idleness over batch-max timestamps).
   */
  private static Substitution<StreamPhysicalTableSourceScan> kafkaSourceSubstitution() {
    return Substitution.of(
            StreamPhysicalTableSourceScan.class,
            (scan, ctx) ->
                new StreamPhysicalNativeKafkaSource(
                    scan.getCluster(),
                    scan.getTraitSet(),
                    scan.getRowType(),
                    FilesystemTables.options(scan),
                    ScanWatermarkSpec.of(scan)))
        .matching(
            scan ->
                KafkaTables.isNativeKafka(scan) && NativeConfig.operatorEnabled("kafkaSource"));
  }

  /**
   * Shallow native-decode path (the default for every value format): Flink's KafkaSource consumes raw
   * bytes, a native operator decodes them to Arrow, skipping Flink's RowData decode. JSON/CSV/raw/Avro
   * and protobuf all route here; CDC changelog formats route to {@link #cdcDecodeSubstitution()}.
   */
  private static Substitution<StreamPhysicalTableSourceScan> kafkaDecodeSubstitution() {
    return Substitution.of(StreamPhysicalTableSourceScan.class, PhysicalPlanScan::planKafkaDecode)
        .matching(
            scan ->
                KafkaTables.isNativeKafkaDecode(scan)
                    && NativeConfig.operatorEnabled("kafkaDecode"));
  }

  private static Substitution<StreamPhysicalTableSourceScan> cdcDecodeSubstitution() {
    return Substitution.of(StreamPhysicalTableSourceScan.class, PhysicalPlanScan::planKafkaDecode)
        .matching(
            scan ->
                KafkaTables.isCdcDecode(scan) && NativeConfig.operatorEnabled("kafkaDecode"))
        .changelogSafe();
  }

  private static RelNode planKafkaDecode(StreamPhysicalTableSourceScan scan, PlanContext ctx) {
    return new StreamPhysicalNativeKafkaDecode(
        scan.getCluster(), scan.getTraitSet(), scan.getRowType(), FilesystemTables.options(scan));
  }

  /**
   * A watermarked CDC table that did not route to the native decode stays on Flink. Reports only —
   * it substitutes nothing, so it always yields to the entries after it.
   */
  private static Substitution<RelNode> cdcWatermarkReport() {
    return Substitution.of(RelNode.class, PhysicalPlanScan::reportCdcWatermark)
        .changelogSafe()
        .yieldingOnDecline();
  }

  private static RelNode reportCdcWatermark(RelNode node, PlanContext ctx) {
    String fallback = KafkaTables.cdcWatermarkFallback(node);
    if (fallback != null) {
      ctx.decline(fallback);
    }
    return null;
  }

  /**
   * A watermarked table that didn't route to a native source or decode stays on Flink (an
   * unreproducible watermark shape, or a table only the decode-operator path — which regenerates no
   * watermarks — could take). Records the precise reason rather than silently stalling event-time
   * timers. Reports only, so it always yields.
   */
  private static Substitution<RelNode> appendWatermarkReport() {
    return Substitution.of(RelNode.class, PhysicalPlanScan::reportAppendWatermark)
        .yieldingOnDecline();
  }

  private static RelNode reportAppendWatermark(RelNode node, PlanContext ctx) {
    String fallback = KafkaTables.appendWatermarkFallback(node);
    if (fallback != null) {
      ctx.decline(fallback);
    }
    return null;
  }

  // ------------------------------------------------------------------------------ core substitutions

  private static RelNode planGroupAggregate(
      StreamPhysicalGroupAggregate agg, PlanContext ctx) {
    int[] keyColumns = GroupAggregateMatcher.keyColumns(agg);
    // A STATE_TTL hint overrides the job-wide retention for this aggregate alone (Flink's
    // StateMetadata precedence); null means no hint, resolved at translate time.
    Long stateTtlHint = StateTtlHint.getStateTtlFromHintOnSingleRel(agg.hints());
    // The aggregate is columnar (Arrow in/out). Keep the keyed shuffle columnar where the input
    // sits on a columnar producer (a native exchange splits the batch by the grouping keys);
    // otherwise the transition pass inserts a transpose at the host exchange boundary. Same key
    // co-location argument as the window aggregate (divergences/10).
    return new StreamPhysicalNativeColumnarGroupAggregate(
        agg.getCluster(),
        agg.getTraitSet(),
        ctx.columnarInput(agg.getInputs().get(0), keyColumns),
        agg.getRowType(),
        GroupAggregateMatcher.kinds(agg),
        GroupAggregateMatcher.valueTypeCodes(agg),
        GroupAggregateMatcher.valueColumns(agg),
        keyColumns,
        GroupAggregateMatcher.filterColumns(agg),
        new int[0], // single-phase: no AVG-merge count partials
        new int[0], // single-phase: distinct values fold per row, no view columns
        -1, // single-phase: liveness counts rows ±1, no count1 partial
        GroupAggregateMatcher.generateUpdateBefore(agg),
        stateTtlHint == null ? -1 : stateTtlHint);
  }

  private static RelNode planGlobalGroupAggregate(
      StreamPhysicalGlobalGroupAggregate agg, PlanContext ctx) {
    int[] keyColumns = GlobalGroupAggregateMatcher.keyColumns(agg);
    // TTL lives on the stateful global half only (the local is a transient per-bundle buffer);
    // a STATE_TTL hint on the aggregate overrides the job-wide retention, as single-phase.
    Long stateTtlHint = StateTtlHint.getStateTtlFromHintOnSingleRel(agg.hints());
    return new StreamPhysicalNativeColumnarGroupAggregate(
        agg.getCluster(),
        agg.getTraitSet(),
        ctx.columnarInput(agg.getInputs().get(0), keyColumns),
        agg.getRowType(),
        GlobalGroupAggregateMatcher.kinds(agg),
        GlobalGroupAggregateMatcher.valueTypeCodes(agg),
        GlobalGroupAggregateMatcher.valueColumns(agg),
        keyColumns,
        new int[0], // the global merge applies no FILTER — the local half already filtered
        GlobalGroupAggregateMatcher.countColumns(agg),
        GlobalGroupAggregateMatcher.distinctViewColumns(agg),
        GlobalGroupAggregateMatcher.recordCountColumn(agg),
        GlobalGroupAggregateMatcher.generateUpdateBefore(agg),
        stateTtlHint == null ? -1 : stateTtlHint);
  }

  private static RelNode planMiniBatchAssigner(
      StreamPhysicalMiniBatchAssigner assigner, PlanContext ctx) {
    MiniBatchInterval interval =
        assigner
            .getTraitSet()
            .getTrait(MiniBatchIntervalTraitDef$.MODULE$.INSTANCE())
            .getMiniBatchInterval();
    if (interval.getMode() != MiniBatchMode.ProcTime
        && interval.getMode() != MiniBatchMode.RowTime) {
      ctx.decline("miniBatchAssigner: unsupported mini-batch mode " + interval.getMode());
      return null;
    }
    if (!NativeConfig.operatorEnabled("miniBatchAssigner")) {
      ctx.decline(Substitution.disabledReason("miniBatchAssigner"));
      return null;
    }
    return new StreamPhysicalNativeMiniBatchAssigner(
        assigner.getCluster(),
        assigner.getTraitSet(),
        assigner.getInputs().get(0),
        assigner.getRowType(),
        interval.getInterval(),
        interval.getMode() == MiniBatchMode.RowTime);
  }

  private static RelNode planRegularJoin(StreamPhysicalJoin join, PlanContext ctx) {
    int[] leftKeys = RegularJoinMatcher.leftKeys(join);
    int[] rightKeys = RegularJoinMatcher.rightKeys(join);
    // A STATE_TTL hint sets each side's retention independently (0 = left, 1 = right —
    // Flink's FlinkHints.LEFT_INPUT convention), overriding the job-wide retention for that
    // side alone; -1 means no hint, resolved at translate time.
    Map<Integer, Long> hintTtls = StateTtlHint.getStateTtlFromHintOnBiRel(join.getHints());
    // Columnar (Arrow in/out); keep each side's keyed shuffle columnar where it sits on a
    // columnar producer, else the transition pass transposes at the boundary.
    return new StreamPhysicalNativeColumnarUpdatingJoin(
        join.getCluster(),
        join.getTraitSet(),
        ctx.columnarInput(join.getLeft(), leftKeys),
        ctx.columnarInput(join.getRight(), rightKeys),
        join.getRowType(),
        leftKeys,
        rightKeys,
        RegularJoinMatcher.joinTypeCode(join),
        RegularJoinMatcher.nonEquiPredicate(join),
        RegularJoinMatcher.joinKeyIsUnique(join, 0) && RegularJoinMatcher.joinKeyIsUnique(join, 1),
        hintTtls.getOrDefault(0, -1L),
        hintTtls.getOrDefault(1, -1L));
  }

  private static RelNode planDeduplicate(StreamPhysicalRank rank, PlanContext ctx) {
    int[] partitionColumns = DeduplicateMatcher.partitionColumns(rank);
    // Columnar (Arrow in/out); the partitioned shuffle stays columnar where the input sits on a
    // columnar producer, else the transition pass transposes at the boundary. Watermark-released
    // keep-first is insert-only; keep-last — and rowtime keep-first under mini-batch — emits a
    // retract changelog (the native rel inherits the host rank's changelog trait, so the
    // boundary transpose carries $row_kind$).
    return new StreamPhysicalNativeDeduplicate(
        rank.getCluster(),
        rank.getTraitSet(),
        ctx.columnarInput(rank.getInput(), partitionColumns),
        rank.getRowType(),
        partitionColumns,
        DeduplicateMatcher.rowtimeColumn(rank),
        DeduplicateMatcher.keepLast(rank),
        DeduplicateMatcher.generateUpdateBefore(rank),
        DeduplicateMatcher.isProctime(rank));
  }

  private static RelNode planTopN(StreamPhysicalRank rank, PlanContext ctx) {
    // An update-fast rank (unique-keyed input with a monotonic sort key) receives a changelog
    // WITHOUT retractions — the upstream is planned to emit only +I/+U, and rank rows are
    // replaced by their unique key (the retracting ranker's full-row retraction model would
    // accumulate every version). It routes to the update-fast ranker, which mirrors Flink's
    // UpdatableTopNFunction/FastTop1Function state shape.
    if (rank.rankStrategy() instanceof RankProcessStrategy.UpdateFastStrategy) {
      if (TopNMatcher.offset(rank) > 0) {
        ctx.decline("Top-N: update-fast rank with OFFSET runs on the host");
        return null;
      }
      int[] updateFastPartitions = TopNMatcher.partitionColumns(rank);
      return new StreamPhysicalNativeColumnarTopN(
          rank.getCluster(),
          rank.getTraitSet(),
          ctx.columnarInput(rank.getInput(), updateFastPartitions),
          rank.getRowType(),
          updateFastPartitions,
          TopNMatcher.sortIndices(rank),
          TopNMatcher.sortAscending(rank),
          TopNMatcher.sortNullsFirst(rank),
          0,
          TopNMatcher.limit(rank),
          TopNMatcher.outputRankNumber(rank),
          false,
          ((RankProcessStrategy.UpdateFastStrategy) rank.rankStrategy()).getPrimaryKeys());
    }
    int[] partitionColumns = TopNMatcher.partitionColumns(rank);
    long offset = TopNMatcher.offset(rank);
    // A changelog input or an OFFSET routes to the retracting ranker (full buffer + rank window);
    // the append-only bounded ranker handles only the insert-only, no-offset case.
    boolean retracting =
        offset > 0 || !ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) rank.getInput());
    // Columnar (Arrow in/out); keep the partitioned shuffle columnar where the input sits on a
    // columnar producer, else the transition pass transposes at the boundary.
    return new StreamPhysicalNativeColumnarTopN(
        rank.getCluster(),
        rank.getTraitSet(),
        ctx.columnarInput(rank.getInput(), partitionColumns),
        rank.getRowType(),
        partitionColumns,
        TopNMatcher.sortIndices(rank),
        TopNMatcher.sortAscending(rank),
        TopNMatcher.sortNullsFirst(rank),
        offset,
        TopNMatcher.limit(rank),
        TopNMatcher.outputRankNumber(rank),
        retracting,
        null);
  }

  private static RelNode planLimit(Sort sort, PlanContext ctx) {
    boolean insertOnlyInput = ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) sort.getInput());
    if (LimitMatcher.matches(sort) && insertOnlyInput) {
      if (!NativeConfig.operatorEnabled("limit")) {
        ctx.decline(Substitution.disabledReason("limit"));
        return null;
      }
      int[] partitionColumns = new int[0]; // global limit — a single gather, no partition
      long offset = LimitMatcher.offset(sort);
      return new StreamPhysicalNativeColumnarTopN(
          sort.getCluster(),
          sort.getTraitSet(),
          ctx.columnarInput(sort.getInput(), partitionColumns),
          sort.getRowType(),
          partitionColumns,
          LimitMatcher.sortIndices(sort),
          LimitMatcher.sortAscending(sort),
          LimitMatcher.sortNullsFirst(sort),
          offset,
          LimitMatcher.limit(sort),
          false, // a global LIMIT never projects a rank column
          offset > 0, // an OFFSET uses the retracting ranker; no-offset the append-only one
          null);
    }
    // A retracting input is the one reason not in unsupportedReason.
    ctx.decline(
        insertOnlyInput
            ? LimitMatcher.unsupportedReason(sort)
            : "limit: needs an insert-only input (the append-only ranker is implemented)");
    return null;
  }

  private static RelNode planFilterCalc(Calc calc, PlanContext ctx) {
    RexExpression condition = FilterCalcMatcher.encodedCondition(calc);
    return new StreamPhysicalNativeFilter(
        calc.getCluster(),
        calc.getTraitSet(),
        calc.getInputs().get(0),
        calc.getRowType(),
        FilterCalcMatcher.projection(calc),
        condition.kinds(),
        condition.payload(),
        condition.childCounts(),
        condition.longs(),
        condition.doubles(),
        condition.strings(),
        condition.udfBinding());
  }

  private static RelNode planCalc(Calc calc, PlanContext ctx) {
    RelNode input = calc.getInputs().get(0);
    RexExpression encoded = CalcMatcher.encode(calc);
    // Nested projection pushdown: when the input is rowwise (about to be transposed) and the calc
    // reads only some of its columns / struct sub-fields, prune the entry transpose to just those
    // and remap the calc's top-level column references to the compacted positions. The transpose
    // then converts only the read fields of each wide source row to Arrow. (A columnar producer is
    // left alone — its batch is already built; nested access stays by name, so it needs no remap.)
    CalcProjectionPruner.Pruned pruned = CalcProjectionPruner.compute(calc);
    if (ctx.kafkaExtension() && pruned != null && input instanceof StreamPhysicalNativeKafkaDecode) {
      // The native decode is itself a (Rust) row→Arrow transpose: pushing the projection into it
      // makes the decoder build only the read columns/fields straight from the bytes, so a wide
      // record's unread fields are never decoded. Only for decoders that honor a pruned schema.
      StreamPhysicalNativeKafkaDecode decode = (StreamPhysicalNativeKafkaDecode) input;
      if (KafkaTables.decodeHonorsProjection(decode.options())) {
        return new StreamPhysicalNativeCalc(
            calc.getCluster(),
            calc.getTraitSet(),
            decode.withProjection(pruned.inputType),
            calc.getRowType(),
            encoded.remapInputs(pruned.remap));
      }
    }
    if (ctx.kafkaExtension() && pruned != null && input instanceof StreamPhysicalNativeKafkaSource) {
      // The fully-native rdkafka source decodes in Rust too: push the projection in so the in-Rust
      // decode builds only the read columns/fields straight from the bytes (the columnar-source analog
      // of pruning the entry transpose). Only for formats whose decoder honors a pruned schema.
      StreamPhysicalNativeKafkaSource source = (StreamPhysicalNativeKafkaSource) input;
      // A watermarked source must keep decoding its rowtime column (the per-split watermark reads
      // it), so a projection that drops it is not pushed — the Calc still runs natively, unpruned.
      if (KafkaTables.decodeHonorsProjection(source.options())
          && source.projectionKeepsRowtime(pruned.inputType)) {
        return new StreamPhysicalNativeCalc(
            calc.getCluster(),
            calc.getTraitSet(),
            source.withProjection(pruned.inputType),
            calc.getRowType(),
            encoded.remapInputs(pruned.remap));
      }
    }
    if (pruned != null && !emitsColumnar(input)) {
      // A rowwise input is about to be transposed: prune that entry transpose to the read fields.
      boolean carryRowKind =
          input instanceof StreamPhysicalRel
              && !ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) input);
      RelNode prunedTranspose =
          new StreamPhysicalRowDataToArrow(
              input.getCluster(), input.getTraitSet(), input, carryRowKind, pruned.inputType);
      return new StreamPhysicalNativeCalc(
          calc.getCluster(),
          calc.getTraitSet(),
          prunedTranspose,
          calc.getRowType(),
          encoded.remapInputs(pruned.remap));
    }
    // The mini-batch assigner is a pass-through (it forwards batches untouched), so it must not
    // hide a rowwise input from the pruning above: push the pruned entry transpose through it.
    // Without this, a mini-batch plan pays an UNPRUNED transpose of the full wide source row —
    // measured at 7x the transpose work on Nexmark q3.
    if (pruned != null && input instanceof StreamPhysicalNativeMiniBatchAssigner) {
      StreamPhysicalNativeMiniBatchAssigner assigner = (StreamPhysicalNativeMiniBatchAssigner) input;
      RelNode below = assigner.getInput(0);
      if (!emitsColumnar(below)) {
        boolean carryRowKind =
            below instanceof StreamPhysicalRel
                && !ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) below);
        RelNode prunedTranspose =
            new StreamPhysicalRowDataToArrow(
                below.getCluster(), below.getTraitSet(), below, carryRowKind, pruned.inputType);
        return new StreamPhysicalNativeCalc(
            calc.getCluster(),
            calc.getTraitSet(),
            assigner.withInput(prunedTranspose, pruned.inputType),
            calc.getRowType(),
            encoded.remapInputs(pruned.remap));
      }
    }
    return new StreamPhysicalNativeCalc(
        calc.getCluster(), calc.getTraitSet(), input, calc.getRowType(), encoded);
  }

  private static RelNode planChangelogNormalize(
      StreamPhysicalChangelogNormalize normalize, PlanContext ctx) {
    int[] keyColumns = ChangelogNormalizeMatcher.keyColumns(normalize);
    return new StreamPhysicalNativeChangelogNormalize(
        normalize.getCluster(),
        normalize.getTraitSet(),
        ctx.columnarInput(normalize.getInputs().get(0), keyColumns),
        normalize.getRowType(),
        keyColumns,
        ChangelogNormalizeMatcher.generateUpdateBefore(normalize));
  }

  private static RelNode planUnnest(StreamPhysicalCorrelate correlate, PlanContext ctx) {
    RelNode unnest =
        new StreamPhysicalNativeUnnest(
            correlate.getCluster(),
            correlate.getTraitSet(),
            correlate.getInputs().get(0),
            correlate.getRowType(),
            UnnestMatcher.arrayColumn(correlate),
            UnnestMatcher.withOrdinality(correlate),
            UnnestMatcher.isLeft(correlate),
            UnnestMatcher.isMultiset(correlate));
    RexExpression condition = UnnestMatcher.encodedCondition(correlate);
    if (condition == null) {
      return unnest;
    }
    // A filter pushed into the correlate (… WHERE element > x) is applied as a native filter
    // over the unnest output, with an identity projection (the unnest already produced the
    // correlate's output columns). The condition's refs were shifted to index that output.
    int arity = correlate.getRowType().getFieldCount();
    int[] identity = new int[arity];
    for (int i = 0; i < arity; i++) {
      identity[i] = i;
    }
    return new StreamPhysicalNativeFilter(
        correlate.getCluster(),
        correlate.getTraitSet(),
        unnest,
        correlate.getRowType(),
        identity,
        condition.kinds(),
        condition.payload(),
        condition.childCounts(),
        condition.longs(),
        condition.doubles(),
        condition.strings(),
        condition.udfBinding());
  }

  private static RelNode planExpand(StreamPhysicalExpand expand, PlanContext ctx) {
    return new StreamPhysicalNativeExpand(
        expand.getCluster(),
        expand.getTraitSet(),
        expand.getInputs().get(0),
        expand.getRowType(),
        ExpandMatcher.numExpandRows(expand),
        ExpandMatcher.numOutputColumns(expand),
        expand.expandIdIndex(),
        ExpandMatcher.expandIdIsLong(expand),
        ExpandMatcher.copyIndices(expand),
        ExpandMatcher.expandIdValues(expand));
  }

  private static RelNode planUnion(StreamPhysicalUnion union, PlanContext ctx) {
    return new StreamPhysicalNativeUnion(
        union.getCluster(), union.getTraitSet(), union.getInputs(), union.getRowType());
  }

  private static RelNode planWatermarkAssigner(
      StreamPhysicalWatermarkAssigner wm, PlanContext ctx) {
    return new StreamPhysicalNativeWatermarkAssigner(
        wm.getCluster(),
        wm.getTraitSet(),
        wm.getInputs().get(0),
        wm.getRowType(),
        WatermarkAssignerMatcher.rowtimeColumn(wm),
        WatermarkAssignerMatcher.delayMillis(wm));
  }

  private static RelNode planTemporalSort(StreamPhysicalTemporalSort sort, PlanContext ctx) {
    return new StreamPhysicalNativeTemporalSort(
        sort.getCluster(),
        sort.getTraitSet(),
        ctx.columnarInput(sort.getInputs().get(0), new int[0]),
        sort.getRowType(),
        TemporalSortMatcher.rowtimeColumn(sort));
  }

  private static RelNode planWindowTableFunction(
      StreamPhysicalWindowTableFunction tvf, PlanContext ctx) {
    return new StreamPhysicalNativeWindowTableFunction(
        tvf.getCluster(),
        tvf.getTraitSet(),
        tvf.getInputs().get(0),
        tvf.getRowType(),
        WindowTableFunctionMatcher.timeColumn(tvf),
        WindowTableFunctionMatcher.windowMillis(tvf),
        WindowTableFunctionMatcher.slideMillis(tvf),
        WindowTableFunctionMatcher.cumulative(tvf),
        WindowTableFunctionMatcher.isProctime(tvf));
  }

  private static RelNode planWindowRank(StreamPhysicalWindowRank rank, PlanContext ctx) {
    int[] partitionColumns = WindowRankMatcher.partitionColumns(rank);
    return new StreamPhysicalNativeWindowRank(
        rank.getCluster(),
        rank.getTraitSet(),
        ctx.columnarInput(rank.getInputs().get(0), partitionColumns),
        rank.getRowType(),
        WindowRankMatcher.windowStartColumn(rank),
        WindowRankMatcher.windowEndColumn(rank),
        partitionColumns,
        WindowRankMatcher.sortIndices(rank),
        WindowRankMatcher.sortAscending(rank),
        WindowRankMatcher.sortNullsFirst(rank),
        WindowRankMatcher.limit(rank),
        WindowRankMatcher.outputRankNumber(rank),
        WindowRankMatcher.isProctime(rank),
        WindowRankMatcher.windowMillis(rank),
        WindowRankMatcher.slideMillis(rank),
        WindowRankMatcher.cumulative(rank));
  }

  private static RelNode planWindowDeduplicate(
      StreamPhysicalWindowDeduplicate dedup, PlanContext ctx) {
    int[] partitionColumns = WindowDeduplicateMatcher.partitionColumns(dedup);
    return new StreamPhysicalNativeWindowRank(
        dedup.getCluster(),
        dedup.getTraitSet(),
        ctx.columnarInput(dedup.getInputs().get(0), partitionColumns),
        dedup.getRowType(),
        WindowDeduplicateMatcher.windowStartColumn(dedup),
        WindowDeduplicateMatcher.windowEndColumn(dedup),
        partitionColumns,
        WindowDeduplicateMatcher.sortIndices(dedup),
        WindowDeduplicateMatcher.sortAscending(dedup),
        WindowDeduplicateMatcher.sortNullsFirst(dedup),
        1,
        false,
        WindowDeduplicateMatcher.isProctime(dedup),
        WindowDeduplicateMatcher.windowMillis(dedup),
        WindowDeduplicateMatcher.slideMillis(dedup),
        WindowDeduplicateMatcher.cumulative(dedup));
  }

  private static RelNode planWindowAggregate(StreamPhysicalWindowAggregate agg, PlanContext ctx) {
    int[] keyColumns = WindowAggregateMatcher.keyColumns(agg.grouping());
    // Always columnar: the keyed shuffle stays Arrow where it sits on a columnar
    // producer (a native exchange splits the batch by the grouping keys), otherwise the transition
    // pass inserts a row→Arrow transpose at the boundary. The exchange only co-locates each key's
    // rows on one channel — the window re-groups by key itself — so its hash need not match Flink's.
    return new StreamPhysicalNativeColumnarWindowAggregate(
        agg.getCluster(),
        agg.getTraitSet(),
        ctx.columnarInput(agg.getInputs().get(0), keyColumns),
        agg.getRowType(),
        WindowAggregateMatcher.isCumulative(agg.windowing()),
        WindowAggregateMatcher.windowSize(agg.windowing()),
        WindowAggregateMatcher.windowSlide(agg.windowing()),
        WindowAggregateMatcher.timeColumn(agg.windowing()),
        WindowAggregateMatcher.valueColumns(agg.aggCalls()),
        keyColumns,
        WindowAggregateMatcher.valueTypeCodes(agg.aggCalls(), agg.getInput().getRowType()),
        WindowAggregateMatcher.kinds(agg.aggCalls()),
        WindowAggregateMatcher.isProctime(agg.windowing()),
        WindowAggregateMatcher.isLtz(agg.windowing()));
  }

  private static RelNode planSessionWindowAggregate(
      StreamPhysicalWindowAggregate agg, PlanContext ctx) {
    int[] keyColumns = WindowAggregateMatcher.keyColumns(agg.grouping());
    // Always columnar: the keyed shuffle stays Arrow where it sits on a columnar
    // producer, otherwise the transition pass transposes at the boundary.
    return new StreamPhysicalNativeColumnarSessionWindowAggregate(
        agg.getCluster(),
        agg.getTraitSet(),
        ctx.columnarInput(agg.getInputs().get(0), keyColumns),
        agg.getRowType(),
        WindowAggregateMatcher.gapMillis(agg.windowing()),
        WindowAggregateMatcher.timeColumn(agg.windowing()),
        WindowAggregateMatcher.valueColumns(agg.aggCalls()),
        keyColumns,
        WindowAggregateMatcher.valueTypeCodes(agg.aggCalls(), agg.getInput().getRowType()),
        WindowAggregateMatcher.kinds(agg.aggCalls()),
        WindowAggregateMatcher.isProctime(agg.windowing()),
        WindowAggregateMatcher.isLtz(agg.windowing()));
  }

  private static RelNode planGroupWindowSession(
      StreamPhysicalGroupWindowAggregate agg, PlanContext ctx) {
    int[] keyColumns = WindowAggregateMatcher.keyColumns(agg.grouping());
    return new StreamPhysicalNativeColumnarSessionWindowAggregate(
        agg.getCluster(),
        agg.getTraitSet(),
        ctx.columnarInput(agg.getInputs().get(0), keyColumns),
        agg.getRowType(),
        GroupWindowSessionMatcher.gapMillis(agg),
        GroupWindowSessionMatcher.timeColumn(agg),
        WindowAggregateMatcher.valueColumns(agg.aggCalls()),
        keyColumns,
        WindowAggregateMatcher.valueTypeCodes(agg.aggCalls(), agg.getInput().getRowType()),
        WindowAggregateMatcher.kinds(agg.aggCalls()),
        false, // event-time (proctime sessions are not on this path)
        GroupWindowSessionMatcher.isLtz(agg));
  }

  private static RelNode planLocalGroupAggregate(
      StreamPhysicalLocalGroupAggregate agg, PlanContext ctx) {
    return new StreamPhysicalNativeColumnarLocalGroupAggregate(
        agg.getCluster(),
        agg.getTraitSet(),
        agg.getInputs().get(0),
        agg.getRowType(),
        LocalGroupAggregateMatcher.kinds(agg),
        LocalGroupAggregateMatcher.valueTypeCodes(agg),
        LocalGroupAggregateMatcher.valueColumns(agg),
        LocalGroupAggregateMatcher.filterColumns(agg),
        LocalGroupAggregateMatcher.keyColumns(agg),
        LocalGroupAggregateMatcher.distinctViewSources(agg));
  }

  /**
   * Which shape of local window pre-aggregate a node is, or null when the native operator handles
   * none of them. Tumbling, hopping and cumulative locals pre-aggregate per slice off a rowtime;
   * every non-AVG aggregate has a single-field mergeable partial (the custom SUMs mirror Flink's
   * nullable-sum buffer), so the two-phase split admits the same value types as the single-phase
   * path. AVG stays single-phase: its (sum, count) buffer spans two partial columns.
   */
  private enum LocalWindowVariant {
    /** Pre-aggregates per slice; the global re-buckets slices into windows. */
    TUMBLING,
    /** As tumbling, but carries a synthetic count1 column for empty-window detection. */
    HOPPING,
    /** As tumbling over a cumulative window; the partials are the plain user aggregates. */
    CUMULATIVE,
    /** The input already carries window_start/window_end, so there is no rowtime to slice (q5). */
    ATTACHED
  }

  private static LocalWindowVariant localWindowVariant(StreamPhysicalLocalWindowAggregate agg) {
    RelDataType input = agg.getInput().getRowType();
    if (WindowAggregateMatcher.matchesHoppingLocal(
        agg.windowing(), agg.grouping(), agg.aggCalls(), input)) {
      return LocalWindowVariant.HOPPING;
    }
    boolean sliceable =
        WindowAggregateMatcher.matches(agg.windowing(), agg.grouping(), agg.aggCalls(), input)
            && !WindowAggregateMatcher.containsAvg(agg.aggCalls());
    if (sliceable && WindowAggregateMatcher.isTumbling(agg.windowing())) {
      return LocalWindowVariant.TUMBLING;
    }
    if (sliceable && WindowAggregateMatcher.isCumulative(agg.windowing())) {
      return LocalWindowVariant.CUMULATIVE;
    }
    if (WindowAggregateMatcher.matchesAttachedLocal(
        agg.windowing(), agg.grouping(), agg.aggCalls(), input)) {
      return LocalWindowVariant.ATTACHED;
    }
    return null;
  }

  private static RelNode planLocalWindowAggregate(
      StreamPhysicalLocalWindowAggregate agg, PlanContext ctx) {
    boolean attached = localWindowVariant(agg) == LocalWindowVariant.ATTACHED;
    RelDataType localInput = agg.getInput().getRowType();
    // Hopping carries a trailing synthetic count1 column for empty-window detection, so its kinds
    // and value columns get a matching extra entry (counts rows). But the planner only injects it
    // when the user aggregates don't already provide a row count: a COUNT(*) doubles as count1, so
    // the local emits no separate column. Detect it by the partial count in the local's output
    // (its row type is [grouping?, partials.., slice_end]) rather than assuming hopping always
    // adds one — otherwise a hopping COUNT(*) local emits a column the global does not expect.
    int partialColumns = agg.getRowType().getFieldCount() - agg.grouping().length - 1;
    boolean syntheticCount = partialColumns > agg.aggCalls().size();
    int[] kinds =
        syntheticCount
            ? WindowAggregateMatcher.hoppingLocalKinds(agg.aggCalls())
            : WindowAggregateMatcher.kinds(agg.aggCalls());
    int[] valueColumns =
        syntheticCount
            ? WindowAggregateMatcher.hoppingLocalValueColumns(agg.aggCalls())
            : WindowAggregateMatcher.valueColumns(agg.aggCalls());
    int[] valueTypes =
        syntheticCount
            ? WindowAggregateMatcher.hoppingLocalValueTypes(agg.aggCalls(), localInput)
            : WindowAggregateMatcher.valueTypeCodes(agg.aggCalls(), localInput);
    // Window-attached mode reads the window from columns (no rowtime slice); the two modes are
    // mutually exclusive, so the unused indices are -1.
    int timeColumn = attached ? -1 : WindowAggregateMatcher.timeColumn(agg.windowing());
    int windowStartColumn =
        attached ? WindowAggregateMatcher.windowStartColumn(agg.windowing()) : -1;
    int windowEndColumn = attached ? WindowAggregateMatcher.windowEndColumn(agg.windowing()) : -1;
    // Always columnar: the local pre-aggregate emits Arrow partials. Its input feeds
    // directly (no shuffle precedes a local); the transition pass inserts a row→Arrow transpose
    // when the producer is rowwise.
    return new StreamPhysicalNativeColumnarLocalWindowAggregate(
        agg.getCluster(),
        agg.getTraitSet(),
        agg.getInputs().get(0),
        agg.getRowType(),
        WindowAggregateMatcher.sliceSize(agg.windowing()),
        timeColumn,
        windowStartColumn,
        windowEndColumn,
        valueColumns,
        WindowAggregateMatcher.keyColumns(agg.grouping()),
        valueTypes,
        kinds,
        WindowAggregateMatcher.isLtz(agg.windowing()));
  }

  private static RelNode planOver(StreamPhysicalOverAggregate over, PlanContext ctx) {
    int[] keyColumns = OverAggregateMatcher.keyColumns(over);
    // Always columnar: the keyed shuffle becomes a native exchange (split by the
    // partition keys); the transition pass transposes below it only when the producer is rowwise.
    return new StreamPhysicalNativeOverAggregate(
        over.getCluster(),
        over.getTraitSet(),
        ctx.columnarInput(over.getInputs().get(0), keyColumns),
        over.getRowType(),
        OverAggregateMatcher.timeColumn(over),
        OverAggregateMatcher.valueColumnIndices(over),
        keyColumns,
        OverAggregateMatcher.valueTypeCodes(over),
        OverAggregateMatcher.kinds(over),
        OverAggregateMatcher.frameKind(over),
        OverAggregateMatcher.frameOffset(over),
        OverAggregateMatcher.isProctime(over));
  }

  private static RelNode planIntervalJoin(StreamPhysicalIntervalJoin join, PlanContext ctx) {
    int[] leftKeys = IntervalJoinMatcher.leftKeys(join);
    int[] rightKeys = IntervalJoinMatcher.rightKeys(join);
    // Keep each input's keyed shuffle columnar where it sits on a columnar producer (a native
    // exchange splits the batch by that side's join key); otherwise the boundary gets a
    // row→Arrow transpose. The join re-groups by key in its own state, so the exchange hash need
    // not match Flink's (divergences/10). The join is always columnar (Arrow pairs out).
    return new StreamPhysicalNativeIntervalJoin(
        join.getCluster(),
        join.getTraitSet(),
        ctx.columnarInput(join.getLeft(), leftKeys),
        ctx.columnarInput(join.getRight(), rightKeys),
        join.getRowType(),
        leftKeys,
        rightKeys,
        IntervalJoinMatcher.leftTime(join),
        IntervalJoinMatcher.rightTime(join),
        IntervalJoinMatcher.lowerMillis(join),
        IntervalJoinMatcher.upperMillis(join),
        IntervalJoinMatcher.joinTypeCode(join),
        IntervalJoinMatcher.nonEquiPredicate(join),
        IntervalJoinMatcher.isProctime(join));
  }

  private static RelNode planWindowJoin(StreamPhysicalWindowJoin join, PlanContext ctx) {
    int[] leftKeys = WindowJoinMatcher.leftKeys(join);
    int[] rightKeys = WindowJoinMatcher.rightKeys(join);
    // Shuffle each input by its join key (columnar where it sits on a columnar producer), the
    // same coupling as the interval join. The window join then matches per window in its state.
    return new StreamPhysicalNativeWindowJoin(
        join.getCluster(),
        join.getTraitSet(),
        ctx.columnarInput(join.getLeft(), leftKeys),
        ctx.columnarInput(join.getRight(), rightKeys),
        join.getRowType(),
        leftKeys,
        rightKeys,
        WindowJoinMatcher.leftWindowStart(join),
        WindowJoinMatcher.leftWindowEnd(join),
        WindowJoinMatcher.rightWindowStart(join),
        WindowJoinMatcher.rightWindowEnd(join),
        WindowJoinMatcher.joinTypeCode(join),
        WindowJoinMatcher.nonEquiPredicate(join),
        WindowJoinMatcher.isProctime(join),
        WindowJoinMatcher.windowMillis(join),
        WindowJoinMatcher.slideMillis(join),
        WindowJoinMatcher.cumulative(join));
  }

  private static RelNode planTemporalJoin(StreamPhysicalTemporalJoin join, PlanContext ctx) {
    int[] leftKeys = TemporalJoinMatcher.leftKeys(join);
    int[] rightKeys = TemporalJoinMatcher.rightKeys(join);
    // Shuffle each input by its join key (columnar where it sits on a columnar producer); the
    // versioned join then groups by key in its own state, like the interval/window join.
    return new StreamPhysicalNativeTemporalJoin(
        join.getCluster(),
        join.getTraitSet(),
        ctx.columnarInput(join.getLeft(), leftKeys),
        ctx.columnarInput(join.getRight(), rightKeys),
        join.getRowType(),
        leftKeys,
        rightKeys,
        TemporalJoinMatcher.leftTime(join),
        TemporalJoinMatcher.rightTime(join),
        TemporalJoinMatcher.joinTypeCode(join),
        TemporalJoinMatcher.nonEquiPredicate(join));
  }

  private static RelNode planLookupJoin(StreamPhysicalLookupJoin join, PlanContext ctx) {
    // A lookup join is stateless (no keyed shuffle); the probe input passes through as-is, and the
    // dimension is a (sync or async) lookup the operator performs — not an input.
    return new StreamPhysicalNativeLookupJoin(
        join.getCluster(),
        join.getTraitSet(),
        join.getInput(),
        join.getRowType(),
        LookupJoinMatcher.temporalTable(join),
        LookupJoinMatcher.lookupKeys(join),
        join.calcOnTemporalTable().isDefined() ? join.calcOnTemporalTable().get() : null,
        join.finalPreFilterCondition().isDefined() ? join.finalPreFilterCondition().get() : null,
        join.finalRemainingCondition().isDefined() ? join.finalRemainingCondition().get() : null,
        LookupJoinMatcher.isLeftOuterJoin(join),
        join.asyncOptions().isDefined() ? join.asyncOptions().get() : null);
  }

  private static RelNode planGlobalWindowAggregate(
      StreamPhysicalGlobalWindowAggregate agg, PlanContext ctx) {
    int[] keyColumns = GlobalWindowAggregateMatcher.keyColumns(agg);
    // Always columnar: the columnar local emits Arrow partials, a native exchange
    // splits them by key, and the columnar global merges — the whole two-phase pipeline flows
    // Arrow. (columnarInput keeps the partial shuffle Arrow; the local is always a columnar
    // producer now, so no transpose arises here.)
    return new StreamPhysicalNativeColumnarGlobalWindowAggregate(
        agg.getCluster(),
        agg.getTraitSet(),
        ctx.columnarInput(agg.getInputs().get(0), keyColumns),
        agg.getRowType(),
        GlobalWindowAggregateMatcher.windowMillis(agg),
        GlobalWindowAggregateMatcher.slideMillis(agg),
        GlobalWindowAggregateMatcher.cumulative(agg),
        keyColumns,
        GlobalWindowAggregateMatcher.valueTypes(agg),
        GlobalWindowAggregateMatcher.kinds(agg),
        WindowAggregateMatcher.isLtz(agg.windowing()));
  }

  // ---------------------------------------------------------------------------- island composition

  /**
   * Rewires every group of semantically identical native Kafka sources to one shared instance under
   * a {@link StreamPhysicalNativeShare} carrying the branch count (the same DAG shape Flink's
   * sub-plan reuse produces for the rowwise plan, and the source dedup Arroyo's named nodes and
   * RisingWave's share operator perform). The share operator declares the count on each batch, so
   * every branch takes its own retained view instead of the single-owner root.
   */
  private RelNode shareIdenticalSources(RelNode root) {
    // The DAG this pass builds only survives translation through Flink's digest-based sub-plan
    // reuse (SameRelObjectShuttle splits shared instances; SubplanReuseUtil re-merges them by
    // digest). With reuse disabled the clones would each keep an over-declared consumer count, so
    // leave the branches reading independently.
    if (!NativeConfig.shareSources()
        || !ShortcutUtils.unwrapTableConfig(root)
            .get(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED)) {
      return root;
    }
    Map<String, List<RelNode>> groups = new LinkedHashMap<>();
    collectShareableScans(root, groups);
    Map<RelNode, RelNode> replacements = new IdentityHashMap<>();
    for (List<RelNode> group : groups.values()) {
      if (group.size() < 2) {
        continue;
      }
      long token = NativeRelDigests.nextId();
      RelNode shared = ((ShareableScan) group.get(0)).withShareToken(token);
      RelNode share =
          new StreamPhysicalNativeShare(
              shared.getCluster(), shared.getTraitSet(), shared, group.size(), token);
      for (RelNode member : group) {
        replacements.put(member, share);
      }
    }
    return replacements.isEmpty() ? root : replaceInputs(root, replacements);
  }

  private static void collectShareableScans(RelNode node, Map<String, List<RelNode>> groups) {
    if (node instanceof ShareableScan) {
      // Class-qualified so two different source kinds can never group, whatever their keys.
      String key = node.getClass().getName() + '|' + ((ShareableScan) node).sharingKey();
      groups.computeIfAbsent(key, k -> new ArrayList<>()).add(node);
      return;
    }
    for (RelNode input : node.getInputs()) {
      collectShareableScans(input, groups);
    }
  }

  /** Rebuilds the tree with each replaced node swapped for its (shared) replacement instance. */
  private static RelNode replaceInputs(RelNode node, Map<RelNode, RelNode> replacements) {
    RelNode replacement = replacements.get(node);
    if (replacement != null) {
      return replacement;
    }
    List<RelNode> inputs = new ArrayList<>(node.getInputs().size());
    boolean changed = false;
    for (RelNode input : node.getInputs()) {
      RelNode rebuilt = replaceInputs(input, replacements);
      inputs.add(rebuilt);
      changed |= rebuilt != input;
    }
    return changed ? node.copy(node.getTraitSet(), inputs) : node;
  }

  /**
   * Whether the substituted tree is one fully-columnar island: every operator is native except a
   * row-wise source (a leaf) or the sink (the plan root). Any other row-wise operator means the query
   * cannot be a single columnar island, so the whole thing falls back to stock Flink.
   */
  private static boolean fullyColumnar(RelNode node, boolean isRoot) {
    boolean allowed =
        node instanceof ColumnarInput
            || node instanceof ColumnarOutput
            || node.getInputs().isEmpty() // source / leaf
            || isRoot; // sink (terminal)
    if (!allowed) {
      return false;
    }
    for (RelNode input : node.getInputs()) {
      if (!fullyColumnar(input, false)) {
        return false;
      }
    }
    return true;
  }

  /** Inserts transpose rels at every columnar↔rowwise edge of the (already substituted) tree. */
  private RelNode insertTransitions(RelNode node) {
    List<RelNode> inputs = new ArrayList<>(node.getInputs().size());
    boolean changed = false;
    for (RelNode input : node.getInputs()) {
      RelNode transitioned = insertTransitions(input);
      RelNode adapted = adapt(node, transitioned);
      inputs.add(adapted);
      changed |= adapted != input;
    }
    return changed ? node.copy(node.getTraitSet(), inputs) : node;
  }

  /** Wraps {@code producer} in a transpose if its output carrier differs from what {@code consumer} expects. */
  private RelNode adapt(RelNode consumer, RelNode producer) {
    boolean consumerWantsColumnar = consumesColumnar(consumer);
    boolean producerEmitsColumnar = emitsColumnar(producer);
    if (consumerWantsColumnar && !producerEmitsColumnar) {
      // Carry RowKind across the transpose only on a changelog edge; an insert-only producer needs
      // no per-row tag (the native consumer reads an absent column as all-INSERT).
      boolean carryRowKind =
          producer instanceof StreamPhysicalRel
              && !ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) producer);
      return new StreamPhysicalRowDataToArrow(
          producer.getCluster(), producer.getTraitSet(), producer, carryRowKind);
    }
    if (!consumerWantsColumnar && producerEmitsColumnar) {
      return new StreamPhysicalArrowToRowData(
          producer.getCluster(), producer.getTraitSet(), producer);
    }
    return producer;
  }

  /** Whether a rel produces Arrow batches (a native columnar operator, a columnar source, or a transpose). */
  private static boolean emitsColumnar(RelNode node) {
    return node instanceof ColumnarOutput;
  }

  /** Whether a rel consumes Arrow batches (a native columnar operator, a columnar sink, or a transpose). */
  private static boolean consumesColumnar(RelNode node) {
    return node instanceof ColumnarInput;
  }

  // ------------------------------------------------------------------------------------- reporting

  /**
   * Records why a candidate node fell back, from the first registry entry that explains its shape.
   * The reason lives on the entry, so a matcher's decline and its explanation cannot drift apart.
   */
  private void noteFallback(RelNode node) {
    for (Substitution<?> substitution : REGISTRY) {
      String reason = substitution.reasonFor(node);
      if (reason != null) {
        recordFallback(reason);
        return;
      }
    }
  }

  void countSubstitution() {
    substitutions++;
  }

  void recordFallback(String reason) {
    fallbackReasons.add(reason);
    if (LOG_FALLBACK_REASONS) {
      System.err.println("[streamfusion] falls back to host — " + reason);
    }
  }

  /** The precise expression reason a Calc fell back, from the encoder. */
  private static String calcReason(Calc calc) {
    String reason =
        FilterCalcMatcher.convertibleRow(calc.getInput().getRowType())
            ? RexExpression.reasonForCalc(calc)
            : "unsupported input column type";
    return "Calc: " + (reason != null ? reason : "unsupported Calc expression");
  }

  // ------------------------------------------------------------------------- extension availability

  private static boolean extensionAvailable(String extensionClass, String... prerequisites) {
    if (!classAvailable(extensionClass)) {
      return false;
    }
    for (String prerequisite : prerequisites) {
      if (!classAvailable(prerequisite)) {
        return false;
      }
    }
    return true;
  }

  private static boolean classAvailable(String className) {
    try {
      Class.forName(className, false, PhysicalPlanScan.class.getClassLoader());
      return true;
    } catch (ClassNotFoundException | LinkageError e) {
      return false;
    }
  }

  private static boolean isFlussTableSource(StreamPhysicalTableSourceScan scan) {
    try {
      TableSourceTable table = scan.getTable().unwrap(TableSourceTable.class);
      if (table == null) {
        return false;
      }
      DynamicTableSource source = table.tableSource();
      return source != null
          && "org.apache.fluss.flink.source.FlinkTableSource".equals(source.getClass().getName());
    } catch (LinkageError | RuntimeException e) {
      return false;
    }
  }

  private void record(RelNode node) {
    operatorTypes.add(node.getClass().getSimpleName());
    for (RelNode input : node.getInputs()) {
      record(input);
    }
  }

  /** Operator types seen in the optimized physical plans, in traversal order. */
  public List<String> operatorTypes() {
    return operatorTypes;
  }

  /** Number of plan nodes replaced with native operators across optimization passes. */
  public int substitutions() {
    return substitutions;
  }

  /**
   * Why candidate nodes fell back to the host (e.g. {@code "Calc: unsupported function/operator:
   * ABS"}), in traversal order. Collected for visibility into a query that did not accelerate, the
   * way Comet surfaces fallback reasons in extended explain (ticket 29).
   */
  public List<String> fallbackReasons() {
    return fallbackReasons;
  }

  /**
   * A native-acceleration section for appending to Flink's {@code explainSql} output: how many
   * operators ran natively and, for those that did not, why — Comet's flat "fallback reasons" explain
   * format. Reflects the plans optimized since this scan was installed.
   */
  public String explainSummary() {
    StringBuilder out = new StringBuilder("== Native acceleration (StreamFusion) ==\n");
    out.append(substitutions).append(" operator(s) ran natively.\n");
    if (fallbackReasons.isEmpty()) {
      out.append("No operators fell back to Flink.\n");
    } else {
      out.append(fallbackReasons.size()).append(" operator(s) fell back to Flink:\n");
      for (String reason : fallbackReasons) {
        out.append("  - ").append(reason).append('\n');
      }
    }
    return out.toString();
  }
}
