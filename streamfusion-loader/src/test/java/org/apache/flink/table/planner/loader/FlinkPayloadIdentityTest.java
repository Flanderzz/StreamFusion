package org.apache.flink.table.planner.loader;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import org.apache.flink.table.api.TableException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkPayloadIdentityTest {
  @TempDir Path directory;

  @ParameterizedTest
  @ValueSource(strings = {"1.18", "2.2"})
  void acceptsItsOwnPayloadLine(String loaderLine) throws Exception {
    var payload = payload(loaderLine).toUri().toURL();
    var attributes = FlinkPayloadIdentity.attributes(payload);

    assertDoesNotThrow(() -> FlinkPayloadIdentity.verify(payload, attributes, loaderLine));
  }

  @ParameterizedTest
  @ValueSource(strings = {"1.18", "2.2"})
  void rejectsTheOtherPayloadLineAndNamesBoth(String loaderLine) throws Exception {
    String payloadLine = loaderLine.equals("1.18") ? "2.2" : "1.18";
    var payload = payload(payloadLine).toUri().toURL();
    var attributes = FlinkPayloadIdentity.attributes(payload);

    var failure =
        assertThrows(
            TableException.class,
            () -> FlinkPayloadIdentity.verify(payload, attributes, loaderLine));

    assertTrue(failure.getMessage().contains("loader targets Flink " + loaderLine));
    assertTrue(failure.getMessage().contains("targets Flink " + payloadLine));
  }

  @Test
  void rejectsAnUnmarkedPayload() throws Exception {
    var payload = payload(null).toUri().toURL();
    var attributes = FlinkPayloadIdentity.attributes(payload);

    var failure =
        assertThrows(
            TableException.class, () -> FlinkPayloadIdentity.verify(payload, attributes, "2.2"));

    assertTrue(failure.getMessage().contains("loader targets Flink 2.2"));
    assertTrue(failure.getMessage().contains("missing marker"));
  }

  private Path payload(String line) throws Exception {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    manifest
        .getMainAttributes()
        .putValue(FlinkPayloadIdentity.MODULE_ATTRIBUTE, "streamfusion-core");
    if (line != null)
      manifest.getMainAttributes().putValue(FlinkPayloadIdentity.LINE_ATTRIBUTE, line);
    Path path = directory.resolve("core.jar");
    try (var ignored = new JarOutputStream(Files.newOutputStream(path), manifest)) {}
    return path;
  }
}
