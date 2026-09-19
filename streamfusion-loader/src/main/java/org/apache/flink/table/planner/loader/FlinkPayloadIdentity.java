package org.apache.flink.table.planner.loader;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;
import org.apache.flink.table.api.TableException;

/** Checks payload metadata before any implementation class can cross the planner ABI boundary. */
final class FlinkPayloadIdentity {
  static final String LINE_ATTRIBUTE = "StreamFusion-Flink-Line";
  static final String MODULE_ATTRIBUTE = "StreamFusion-Module";

  private FlinkPayloadIdentity() {}

  static String loaderLine() throws IOException {
    Properties properties = new Properties();
    try (InputStream input =
        PlannerModule.class.getResourceAsStream("streamfusion-loader.properties")) {
      if (input == null) throw new TableException("StreamFusion loader has no target Flink line.");
      properties.load(input);
    }
    String line = properties.getProperty("flink.line");
    if (line == null || !line.matches("[0-9]+\\.[0-9]+")) {
      throw new TableException("StreamFusion loader has an invalid target Flink line: " + line);
    }
    return line;
  }

  static Attributes attributes(URL payload) throws IOException {
    try (JarInputStream jar = new JarInputStream(payload.openStream())) {
      Manifest manifest = jar.getManifest();
      return manifest == null ? new Attributes() : manifest.getMainAttributes();
    }
  }

  static void verify(URL payload, Attributes attributes, String loaderLine) {
    String payloadLine = attributes.getValue(LINE_ATTRIBUTE);
    if (!loaderLine.equals(payloadLine)) {
      throw new TableException(
          "StreamFusion loader targets Flink "
              + loaderLine
              + ", but payload '"
              + payload
              + "' targets "
              + (payloadLine == null
                  ? "an unknown Flink line (missing marker)"
                  : "Flink " + payloadLine)
              + ". Install loader, core and extensions built for the same Flink line.");
    }
  }
}
