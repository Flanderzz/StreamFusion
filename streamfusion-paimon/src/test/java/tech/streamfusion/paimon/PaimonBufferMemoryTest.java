package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.junit.jupiter.api.Test;

class PaimonBufferMemoryTest {
  @Test
  void reservationsCompeteWithOtherOperatorsAndAreReleasedAfterSpillAndClose() throws Exception {
    MemoryManager manager =
        MemoryManagerBuilder.newBuilder().setMemorySize(128 * 1024).setPageSize(32 * 1024).build();
    Object other = new Object();
    try (PaimonBufferMemory memory = new PaimonBufferMemory(96 * 1024, manager)) {
      assertTrue(memory.update(64 * 1024));
      assertFalse(manager.verifyEmpty());
      manager.reserveMemory(other, 64 * 1024);
      assertFalse(memory.update(80 * 1024), "another operator has the remaining managed memory");
      assertEquals(64 * 1024, memory.reservedBytes());
      assertTrue(memory.update(16 * 1024), "spilling returns the previous reservation");
      manager.reserveMemory(other, 32 * 1024);
      manager.releaseAllMemory(other);
      assertTrue(memory.update(96 * 1024));
      assertFalse(memory.update(97 * 1024), "the operator's assigned fraction is also a cap");
    }
    assertTrue(manager.verifyEmpty());
    manager.shutdown();
  }
}
