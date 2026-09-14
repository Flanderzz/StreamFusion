package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

class PaimonSnapshotCallbackTest {
  @Test
  void partialExportReleasesBuffersAndPreservesTheOriginalStorageException() throws Exception {
    var type = RowType.of(new IntType(), new BigIntType(), new TinyIntType(), new VarCharType());
    var failure = new IOException("storage failed after exporting a batch");
    try (var allocator = new RootAllocator()) {
      try (var root =
              RowDataArrowConverter.write(
                  List.of(GenericRowData.of(1, 1L, (byte) 0, StringData.fromString("v"))),
                  type,
                  allocator);
          var input = ArrowSchema.allocateNew(allocator);
          var output = ArrowSchema.allocateNew(allocator);
          var array = ArrowArray.allocateNew(allocator);
          var schema = ArrowSchema.allocateNew(allocator)) {
        Data.exportSchema(allocator, root.getSchema(), NativeAllocator.DICTIONARIES, input);
        Data.exportSchema(
            allocator,
            new Schema(
                List.of(root.getSchema().getFields().get(3), root.getSchema().getFields().get(2))),
            NativeAllocator.DICTIONARIES,
            output);
        long handle =
            NativePaimon.createSnapshotMerger(
                new FailingProvider(root, failure),
                input.memoryAddress(),
                output.memoryAddress(),
                1,
                1,
                2,
                1 << 20,
                new int[0],
                true,
                false,
                false);
        try {
          assertSame(
              failure,
              assertThrows(
                  IOException.class,
                  () ->
                      NativePaimon.snapshotMergerNext(
                          handle, array.memoryAddress(), schema.memoryAddress())));
        } finally {
          NativePaimon.closeSnapshotMerger(handle);
        }
      }
      assertEquals(0, allocator.getAllocatedMemory());
    }
    assertEquals("", NativePaimon.liveNativeHandles());
  }

  public static final class FailingProvider {
    private final VectorSchemaRoot root;
    private final IOException failure;

    FailingProvider(VectorSchemaRoot root, IOException failure) {
      this.root = root;
      this.failure = failure;
    }

    public boolean nextRunBatch(int run, long array, long schema) throws IOException {
      Data.exportVectorSchemaRoot(
          root.getVector(0).getAllocator(),
          root,
          NativeAllocator.DICTIONARIES,
          ArrowArray.wrap(array),
          ArrowSchema.wrap(schema));
      throw failure;
    }
  }
}
