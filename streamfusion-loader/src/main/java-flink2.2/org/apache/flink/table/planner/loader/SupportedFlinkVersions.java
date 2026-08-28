package org.apache.flink.table.planner.loader;

import java.util.Set;

/**
 * The Flink patch versions this loader build has been validated against.
 *
 * <p>The loader shadows a Flink-internal class name, so it fails closed rather than cross an
 * unverified planner ABI. The set is per Flink line and must only list versions the parity and
 * upstream suites have actually run against.
 */
final class SupportedFlinkVersions {

  static final Set<String> VERSIONS = Set.of("2.2.0", "2.2.1");

  private SupportedFlinkVersions() {}
}
