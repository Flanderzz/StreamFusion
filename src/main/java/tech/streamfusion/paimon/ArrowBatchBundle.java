package tech.streamfusion.paimon;

import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.types.RowKind;

/**
 * An Arrow batch offered to Paimon's writer as one bundle of insert records. The batch is borrowed:
 * the operator that owns the root keeps it alive until the write call returns and closes it after.
 *
 * <p>Iterating the bundle yields one {@link Cursor} per row. The cursor carries no field values, only
 * the bundle it belongs to: it exists so a Paimon writer that walks a bundle row by row still counts
 * every row and still hands the whole batch to the native encoder exactly once. Any attempt to read a
 * field from it is a programming error and fails loudly.
 */
public final class ArrowBatchBundle implements BundleRecords {

  private final VectorSchemaRoot root;
  private final Cursor cursor = new Cursor(this);

  public ArrowBatchBundle(VectorSchemaRoot root) {
    this.root = root;
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
      private int remaining = root.getRowCount();

      @Override
      public boolean hasNext() {
        return remaining > 0;
      }

      @Override
      public InternalRow next() {
        if (remaining == 0) {
          throw new NoSuchElementException();
        }
        remaining--;
        return cursor;
      }
    };
  }

  /** A field-less row standing in for one row of an Arrow bundle. */
  public static final class Cursor implements InternalRow {

    private final ArrowBatchBundle bundle;

    private Cursor(ArrowBatchBundle bundle) {
      this.bundle = bundle;
    }

    ArrowBatchBundle bundle() {
      return bundle;
    }

    @Override
    public int getFieldCount() {
      return bundle.root.getSchema().getFields().size();
    }

    @Override
    public RowKind getRowKind() {
      return RowKind.INSERT;
    }

    @Override
    public void setRowKind(RowKind kind) {
      if (kind != RowKind.INSERT) {
        throw unsupported();
      }
    }

    private static UnsupportedOperationException unsupported() {
      return new UnsupportedOperationException(
          "StreamFusion Arrow bundle rows carry no fields; the batch is encoded natively as a whole");
    }

    @Override
    public boolean isNullAt(int pos) {
      throw unsupported();
    }

    @Override
    public boolean getBoolean(int pos) {
      throw unsupported();
    }

    @Override
    public byte getByte(int pos) {
      throw unsupported();
    }

    @Override
    public short getShort(int pos) {
      throw unsupported();
    }

    @Override
    public int getInt(int pos) {
      throw unsupported();
    }

    @Override
    public long getLong(int pos) {
      throw unsupported();
    }

    @Override
    public float getFloat(int pos) {
      throw unsupported();
    }

    @Override
    public double getDouble(int pos) {
      throw unsupported();
    }

    @Override
    public BinaryString getString(int pos) {
      throw unsupported();
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
      throw unsupported();
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
      throw unsupported();
    }

    @Override
    public byte[] getBinary(int pos) {
      throw unsupported();
    }

    @Override
    public Variant getVariant(int pos) {
      throw unsupported();
    }

    @Override
    public Blob getBlob(int pos) {
      throw unsupported();
    }

    @Override
    public InternalArray getArray(int pos) {
      throw unsupported();
    }

    @Override
    public InternalVector getVector(int pos) {
      throw unsupported();
    }

    @Override
    public InternalMap getMap(int pos) {
      throw unsupported();
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
      throw unsupported();
    }
  }
}
