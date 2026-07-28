package io.github.jordepic.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class NativeBridgeTest {

  /**
   * A native panic must arrive as an exception on this thread. Reaching the assertions at all is
   * most of the point: an unguarded panic unwinds out of the native frame and aborts the process,
   * so the failure mode this replaces would take the whole JVM — and every other task in it — down
   * before any assertion ran.
   */
  @Test
  void nativePanicSurfacesAsAnExceptionAndLeavesTheJvmUsable() {
    NativeException failure = assertThrows(NativeException.class, Native::panicForTest);
    assertTrue(
        failure.getMessage().contains("deliberate panic from panicForTest"),
        "panic message not carried across the boundary: " + failure.getMessage());

    // The JVM survived, and the boundary is still usable rather than left with a pending exception.
    assertNotNull(Native.version());
    assertThrows(NativeException.class, Native::panicForTest);
  }

  @Test
  void nativeLibraryReportsVersion() {
    String version = Native.version();
    assertNotNull(version);
    assertTrue(version.matches("\\d+\\.\\d+\\.\\d+"), "unexpected version: " + version);
  }

  @Test
  void nativeRuntimeDrivesAsyncWorkToCompletion() {
    assertEquals(42, Native.blockingAnswer());
  }

  /** The live-handle sentinel the harness leak check polls must see creates and drain on close. */
  @Test
  void liveHandleBreakdownTracksCreateAndClose() {
    assertEquals("", Native.liveNativeHandles());
    long sorter = Native.createTemporalSorter(0, -1);
    String breakdown = Native.liveNativeHandles();
    assertTrue(breakdown.contains("TemporalSorter=1"), "unexpected breakdown: " + breakdown);
    Native.closeTemporalSorter(sorter);
    assertEquals("", Native.liveNativeHandles());
  }
}
