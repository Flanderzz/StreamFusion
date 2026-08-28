package tech.streamfusion;

import org.apache.flink.runtime.util.EnvironmentInformation;

/**
 * The Flink line the tests are executing against.
 *
 * <p>Read from the running Flink rather than a build property so a test can never be gated on a
 * version different from the one it actually loaded.
 */
final class HostFlinkVersion {

  private HostFlinkVersion() {}

  static String current() {
    return EnvironmentInformation.getVersion();
  }

  static boolean atLeast(int major, int minor) {
    String[] parts = current().split("[.-]");
    int hostMajor = Integer.parseInt(parts[0]);
    int hostMinor = Integer.parseInt(parts[1]);
    return hostMajor != major ? hostMajor > major : hostMinor >= minor;
  }
}
