package org.apache.flink.table.planner.loader;

import java.util.Set;

/**
 * The Flink patch versions this loader build has been validated against. See the 2.2 copy for the
 * contract.
 */
final class SupportedFlinkVersions {

  static final Set<String> VERSIONS = Set.of("2.1.3");

  private SupportedFlinkVersions() {}
}
