package tech.streamfusion.paimon;

import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.data.columnar.ColumnarRowData;
import org.apache.flink.table.data.columnar.vector.VectorizedColumnBatch;
import org.apache.flink.table.types.logical.RowType;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.FlinkRowWrapper;
import org.apache.paimon.io.BundleRecords;
import tech.streamfusion.arrow.ArrowConversion;

/**
 * An Arrow batch offered to Paimon's writer as one bundle of insert records. The batch is borrowed:
 * the operator that owns the root keeps it alive until the write call returns and closes it after.
 *
 * <p>Iterating the bundle yields one {@link Cursor} per row: a zero-copy view over the Arrow columns
 * behind Paimon's own Flink row adapter, so Paimon reads exactly the values it would have read from
 * a stock row. The direct write path only walks rows to count them and hands the whole batch to the
 * native encoder once; Paimon's buffered spill path, entered when one task holds more writers than
 * {@code write-max-writers-to-spill}, serializes the views row by row just as it would stock rows.
 */
public final class ArrowBatchBundle implements BundleRecords {

  private final VectorSchemaRoot root;
  private final ColumnarRowData view;
  private final Cursor cursor;

  public ArrowBatchBundle(VectorSchemaRoot root, RowType rowType) {
    this.root = root;
    this.view =
        new ColumnarRowData(
            new VectorizedColumnBatch(
                ArrowConversion.createArrowReader(root, rowType).getColumnVectors()));
    this.cursor = new Cursor(this, view);
  }

  VectorSchemaRoot root() {
    return root;
  }

  @Override
  public long rowCount() {
    return root.getRowCount();
  }

  @Override
  public Iterator<InternalRow> iterator() {
    return new Iterator<>() {
      private int next;

      @Override
      public boolean hasNext() {
        return next < root.getRowCount();
      }

      @Override
      public InternalRow next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        view.setRowId(next++);
        return cursor;
      }
    };
  }

  /** One row of an Arrow bundle, readable through Paimon's row adapter and traceable to its bundle. */
  public static final class Cursor extends FlinkRowWrapper {

    private final ArrowBatchBundle bundle;

    private Cursor(ArrowBatchBundle bundle, ColumnarRowData view) {
      super(view);
      this.bundle = bundle;
    }

    ArrowBatchBundle bundle() {
      return bundle;
    }
  }
}
