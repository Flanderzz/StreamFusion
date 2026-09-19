package tech.streamfusion.operator;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.junit.jupiter.api.Test;

class NativeJsonRuntimeTest {
  @Test
  void olderJacksonDeclinesUnverifiedBufferEmulation() {
    assertFalse(NativeJsonRuntime.available());
    assertFalse(NativeJsonRuntime.probe(JsonFactory::new, JsonFactory::new));
  }

  @Test
  void missingRuntimeFailsClosed() {
    assertFalse(
        NativeJsonRuntime.probe(
            () -> {
              throw new NoClassDefFoundError("shaded Jackson");
            },
            JsonFactory::new));
  }
}
