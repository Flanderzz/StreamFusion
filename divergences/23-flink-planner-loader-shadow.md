# 23 — A temporary planner-loader shadow, not a new Flink planner fork

## What Comet and Flink normally do

Comet uses Spark's public extension points to add its optimizer rules after Spark has assembled the
session planner. Flink 2.2 has no equivalent public hook for an external distribution to append a
planner program stage. Its `PlannerModule` loads the private planner JAR through an isolated
component classloader and discovers the stock `DefaultPlannerFactory`.

## What we do instead

The StreamFusion distribution places a small JAR first in `$FLINK_HOME/lib` that shadows only
`org.apache.flink.table.planner.loader.PlannerModule`. It preserves Flink's component-classloader
model, extracts Flink's original planner JAR, and adds an embedded StreamFusion runtime payload in
front of it. The shim instantiates `StreamFusionPlannerFactory`. For streaming it subclasses
Flink's released `StreamPlanner` and `StreamCommonSubGraphBasedOptimizer`, overriding only the
optimizer construction and `postOptimize` seam. Batch and disabled acceleration use Flink's stock
`DefaultPlannerFactory`.

The hook calls Flink's normal post-optimization validation, then its `ScanReuser` to union compatible
source projections before substituting native operators across all expanded sink roots. Flink's
final clone/reuse pass still runs afterward. Native digest barriers remain in place; explicitly
shared sources and their share operator use a group token, with a consumer count computed after
fallback decisions. This follows Arroyo's named source deduplication and retained batch fan-out,
while retaining Flink's source-ability and multi-sink optimization semantics.

The former program-only hook saw one root at a time and could not share Paimon projections across
sinks. It remains available through `NativePlanner.install` for already constructed stock table
environments, which have no public API for replacing their optimizer. The deployed planner defers
that program to the complete-plan hook. No cross-query scan registry or reflective mutation of a
table environment is needed. The upstream SQL agent constructs the same streaming planner so the
unchanged Paimon source-reuse assertion exercises the production seam.

The runtime payload is also installed as a normal Flink library. Task code therefore sees the same
native runtime package as the planner component, while the payload embedded in the loader keeps the
planner classloader self-contained.

## Why deviate

This is a deployment bridge while the upstream extension API is unavailable. It is much narrower
than a planner fork: StreamFusion does not copy Flink's planner, execution nodes, or optimizer;
Flink still performs all standard planning and execution. The only injected behavior is the native
rewrite, after which unsupported plan shapes retain their normal Flink nodes.

The cost is a version-sensitive private-class seam. The shim supports exactly the tested Flink
**2.2.0 and 2.2.1** planner ABIs and fails closed for unknown or unversioned artifacts. It
rejects incompatible packaged versions at startup. A public upstream planner-extension API would
replace this file and remove the class-name shadow.
