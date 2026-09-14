#include "bridge.h"
#include <algorithm>
#include <cstdio>
#include <limits>
#include <orc/OrcFile.hh>
#include <stdexcept>
#include <unordered_map>

namespace {
thread_local char last_error[2048]{};
void check(int status) {
  if (status != 0)
    throw std::runtime_error("Arrow C Data error " + std::to_string(status));
}
struct Schema {
  ArrowSchema value{};
  explicit Schema(const ArrowSchema *source) {
    int status = ArrowSchemaDeepCopy(source, &value);
    if (status && value.release)
      value.release(&value);
    check(status);
  }
  ~Schema() {
    if (value.release)
      value.release(&value);
  }
};
struct View {
  ArrowArrayView value{};
  View(const ArrowSchema *schema, const ArrowArray *array) {
    ArrowError error{};
    int status = ArrowArrayViewInitFromSchema(&value, schema, &error);
    if (!status)
      status = ArrowArrayViewSetArray(&value, array, &error);
    if (status) {
      ArrowArrayViewReset(&value);
      throw std::runtime_error(error.message);
    }
  }
  ~View() { ArrowArrayViewReset(&value); }
};
ArrowSchemaView schema_view(const ArrowSchema *schema) {
  ArrowSchemaView result{};
  ArrowError error{};
  if (ArrowSchemaViewInit(&result, schema, &error))
    throw std::runtime_error(error.message);
  return result;
}
int64_t units_per_second(ArrowTimeUnit unit) {
  switch (unit) {
  case NANOARROW_TIME_UNIT_SECOND:
    return 1;
  case NANOARROW_TIME_UNIT_MILLI:
    return 1000;
  case NANOARROW_TIME_UNIT_MICRO:
    return 1000000;
  case NANOARROW_TIME_UNIT_NANO:
    return 1000000000;
  default:
    throw std::runtime_error("Unsupported timestamp unit");
  }
}
void validate(const ArrowSchema *schema, const orc::Type &type) {
  auto view = schema_view(schema);
  bool valid = false;
  switch (type.getKind()) {
  case orc::BOOLEAN:
    valid = view.type == NANOARROW_TYPE_BOOL;
    break;
  case orc::BYTE:
    valid = view.type == NANOARROW_TYPE_INT8;
    break;
  case orc::SHORT:
    valid = view.type == NANOARROW_TYPE_INT16;
    break;
  case orc::INT:
    valid =
        view.type == NANOARROW_TYPE_INT32 || view.type == NANOARROW_TYPE_TIME32;
    break;
  case orc::LONG:
    valid = view.type == NANOARROW_TYPE_INT64;
    break;
  case orc::DATE:
    valid = view.type == NANOARROW_TYPE_DATE32;
    break;
  case orc::FLOAT:
    valid = view.type == NANOARROW_TYPE_FLOAT;
    break;
  case orc::DOUBLE:
    valid = view.type == NANOARROW_TYPE_DOUBLE;
    break;
  case orc::STRING:
  case orc::CHAR:
  case orc::VARCHAR:
    valid = view.type == NANOARROW_TYPE_STRING;
    break;
  case orc::BINARY:
    valid = view.type == NANOARROW_TYPE_BINARY ||
            view.type == NANOARROW_TYPE_FIXED_SIZE_BINARY;
    break;
  case orc::TIMESTAMP:
  case orc::TIMESTAMP_INSTANT:
    valid = view.type == NANOARROW_TYPE_TIMESTAMP;
    break;
  case orc::DECIMAL:
    valid =
        view.type == NANOARROW_TYPE_DECIMAL128 &&
        view.decimal_precision == static_cast<int32_t>(type.getPrecision()) &&
        view.decimal_scale == static_cast<int32_t>(type.getScale());
    break;
  case orc::STRUCT:
    valid = view.type == NANOARROW_TYPE_STRUCT &&
            schema->n_children == static_cast<int64_t>(type.getSubtypeCount());
    break;
  case orc::LIST:
    valid = view.type == NANOARROW_TYPE_LIST && schema->n_children == 1;
    break;
  case orc::MAP:
    valid = view.type == NANOARROW_TYPE_MAP && schema->n_children == 1 &&
            schema->children[0]->n_children == 2;
    break;
  default:
    break;
  }
  if (!valid)
    throw std::runtime_error("Arrow schema does not match ORC type " +
                             type.toString());
  if (type.getKind() == orc::MAP) {
    for (int i = 0; i < 2; ++i)
      validate(schema->children[0]->children[i], *type.getSubtype(i));
  } else {
    for (uint64_t i = 0; i < type.getSubtypeCount(); ++i)
      validate(schema->children[i], *type.getSubtype(i));
  }
}
// Preserve the field IDs used by Paimon's Java schema evolution and statistics
// readers.
void attributes(const ArrowSchema *schema, const orc::Type &type) {
  ArrowStringView id{};
  check(ArrowMetadataGetValue(schema->metadata,
                              ArrowCharView("PARQUET:field_id"), &id));
  if (id.data)
    const_cast<orc::Type &>(type).setAttribute(
        "paimon.id", std::string(id.data, id.size_bytes));
  if (type.getKind() == orc::MAP) {
    for (int i = 0; i < 2; ++i)
      attributes(schema->children[0]->children[i], *type.getSubtype(i));
  } else {
    for (uint64_t i = 0; i < type.getSubtypeCount(); ++i)
      attributes(schema->children[i], *type.getSubtype(i));
  }
}
// Primitive values copy column-wise; strings borrow Arrow buffers only until
// Writer::add returns. Nested offsets retain sliced inputs, including slices
// within map entries.
void write_column(const ArrowArrayView &view, const ArrowSchema *schema,
                  const orc::Type &type, orc::ColumnVectorBatch &out,
                  int64_t offset, int64_t length, int64_t destination = 0,
                  const char *mask = nullptr) {
  out.resize(destination + length);
  out.numElements = destination + length;
  if (destination == 0)
    out.hasNulls = false;
  for (int64_t i = 0; i < length; ++i) {
    out.notNull[destination + i] =
        (!mask || mask[i]) && !ArrowArrayViewIsNull(&view, offset + i);
    out.hasNulls |= !out.notNull[destination + i];
  }
  auto field = schema_view(schema);
  switch (type.getKind()) {
  case orc::BOOLEAN:
  case orc::BYTE:
  case orc::SHORT:
  case orc::INT:
  case orc::LONG:
  case orc::DATE: {
    auto &values = dynamic_cast<orc::LongVectorBatch &>(out).data;
    for (int64_t i = 0; i < length; ++i)
      values[destination + i] =
          out.notNull[destination + i]
              ? ArrowArrayViewGetIntUnsafe(&view, offset + i)
              : 0;
    break;
  }
  case orc::FLOAT:
  case orc::DOUBLE: {
    auto &values = dynamic_cast<orc::DoubleVectorBatch &>(out).data;
    for (int64_t i = 0; i < length; ++i)
      values[destination + i] =
          out.notNull[destination + i]
              ? ArrowArrayViewGetDoubleUnsafe(&view, offset + i)
              : 0;
    break;
  }
  case orc::STRING:
  case orc::VARCHAR:
  case orc::CHAR:
  case orc::BINARY: {
    auto &values = dynamic_cast<orc::StringVectorBatch &>(out);
    for (int64_t i = 0; i < length; ++i) {
      auto bytes = out.notNull[destination + i]
                       ? ArrowArrayViewGetBytesUnsafe(&view, offset + i)
                       : ArrowBufferView{};
      values.data[destination + i] = const_cast<char *>(bytes.data.as_char);
      values.length[destination + i] = bytes.size_bytes;
    }
    break;
  }
  case orc::DECIMAL: {
    ArrowDecimal decimal;
    ArrowDecimalInit(&decimal, 128, field.decimal_precision,
                     field.decimal_scale);
    for (int64_t i = 0; i < length; ++i) {
      ArrowDecimalSetInt(&decimal, 0);
      if (out.notNull[destination + i])
        ArrowArrayViewGetDecimalUnsafe(&view, offset + i, &decimal);
      if (type.getPrecision() <= 18) {
        dynamic_cast<orc::Decimal64VectorBatch &>(out).values[destination + i] =
            ArrowDecimalGetIntUnsafe(&decimal);
      } else {
        dynamic_cast<orc::Decimal128VectorBatch &>(out)
            .values[destination + i] = orc::Int128(
            static_cast<int64_t>(decimal.words[decimal.high_word_index]),
            decimal.words[decimal.low_word_index]);
      }
    }
    break;
  }
  case orc::TIMESTAMP:
  case orc::TIMESTAMP_INSTANT: {
    auto &values = dynamic_cast<orc::TimestampVectorBatch &>(out);
    int64_t units = units_per_second(field.time_unit);
    for (int64_t i = 0; i < length; ++i) {
      int64_t value = out.notNull[destination + i]
                          ? ArrowArrayViewGetIntUnsafe(&view, offset + i)
                          : 0;
      int64_t seconds = value / units, remainder = value % units;
      if (remainder < 0) {
        --seconds;
        remainder += units;
      }
      // Java ORC truncates epoch milliseconds to seconds. Its last negative
      // second aliases the first positive second (ORC's historical timestamp
      // encoding); statistics do too.
      if (seconds == -1 && remainder * (1000000000 / units) > 999999)
        seconds = 0;
      values.data[destination + i] = seconds;
      values.nanoseconds[destination + i] = remainder * (1000000000 / units);
    }
    break;
  }
  case orc::STRUCT: {
    auto &values = dynamic_cast<orc::StructVectorBatch &>(out);
    for (uint64_t i = 0; i < type.getSubtypeCount(); ++i) {
      write_column(*view.children[i], schema->children[i], *type.getSubtype(i),
                   *values.fields[i], view.offset + offset, length, destination,
                   out.notNull.data() + destination);
    }
    break;
  }
  case orc::LIST:
  case orc::MAP: {
    auto fill = [&](auto &values, auto append) {
      int64_t child = destination == 0 ? 0 : values.offsets[destination];
      values.offsets[destination] = child;
      for (int64_t i = 0; i < length; ++i) {
        int64_t start =
            ArrowArrayViewListChildOffset(&view, view.offset + offset + i);
        int64_t end =
            ArrowArrayViewListChildOffset(&view, view.offset + offset + i + 1);
        if (out.notNull[destination + i] && end != start) {
          append(start, end - start, child);
          child += end - start;
        }
        values.offsets[destination + i + 1] = child;
      }
    };
    if (type.getKind() == orc::LIST) {
      auto &values = dynamic_cast<orc::ListVectorBatch &>(out);
      if (destination == 0)
        values.elements->numElements = 0;
      fill(values, [&](int64_t start, int64_t count, int64_t child) {
        write_column(*view.children[0], schema->children[0],
                     *type.getSubtype(0), *values.elements, start, count, child,
                     nullptr);
      });
    } else {
      auto &values = dynamic_cast<orc::MapVectorBatch &>(out);
      if (destination == 0) {
        values.keys->numElements = 0;
        values.elements->numElements = 0;
      }
      auto &entries = *view.children[0];
      fill(values, [&](int64_t start, int64_t count, int64_t child) {
        write_column(*entries.children[0], schema->children[0]->children[0],
                     *type.getSubtype(0), *values.keys, entries.offset + start,
                     count, child, nullptr);
        write_column(*entries.children[1], schema->children[0]->children[1],
                     *type.getSubtype(1), *values.elements,
                     entries.offset + start, count, child, nullptr);
      });
    }
    break;
  }
  default:
    throw std::runtime_error("Unsupported ORC type " + type.toString());
  }
}
void read_column(const orc::ColumnVectorBatch &in, const orc::Type &type,
                 const ArrowSchema *schema, ArrowArray *out, int64_t offset,
                 int64_t length) {
  check(ArrowArrayReserve(out, length));
  auto field = schema_view(schema);
  if (type.getKind() == orc::STRUCT) {
    const auto &values = dynamic_cast<const orc::StructVectorBatch &>(in);
    for (uint64_t c = 0; c < type.getSubtypeCount(); ++c) {
      read_column(*values.fields[c], *type.getSubtype(c), schema->children[c],
                  out->children[c], offset, length);
    }
    auto *validity = ArrowArrayValidityBitmap(out);
    for (int64_t i = 0; i < length; ++i) {
      bool valid = !in.hasNulls || in.notNull[offset + i];
      check(ArrowBitmapAppend(validity, valid, 1));
      out->null_count += !valid;
    }
    out->length += length;
    return;
  }
  for (int64_t i = offset; i < offset + length; ++i) {
    if (in.hasNulls && !in.notNull[i]) {
      check(ArrowArrayAppendNull(out, 1));
      continue;
    }
    switch (type.getKind()) {
    case orc::BOOLEAN:
    case orc::BYTE:
    case orc::SHORT:
    case orc::INT:
    case orc::LONG:
    case orc::DATE:
      check(ArrowArrayAppendInt(
          out, dynamic_cast<const orc::LongVectorBatch &>(in).data[i]));
      break;
    case orc::FLOAT:
    case orc::DOUBLE:
      check(ArrowArrayAppendDouble(
          out, dynamic_cast<const orc::DoubleVectorBatch &>(in).data[i]));
      break;
    case orc::STRING:
    case orc::VARCHAR:
    case orc::CHAR:
    case orc::BINARY: {
      const auto &values = dynamic_cast<const orc::StringVectorBatch &>(in);
      ArrowBufferView bytes{};
      bytes.data.as_char = values.data[i];
      bytes.size_bytes = values.length[i];
      if (type.getKind() == orc::CHAR) {
        while (bytes.size_bytes > 0 &&
               bytes.data.as_char[bytes.size_bytes - 1] == ' ')
          --bytes.size_bytes;
      }
      check(ArrowArrayAppendBytes(out, bytes));
      break;
    }
    case orc::DECIMAL: {
      ArrowDecimal decimal;
      ArrowDecimalInit(&decimal, 128, field.decimal_precision,
                       field.decimal_scale);
      if (type.getScale() != static_cast<uint64_t>(field.decimal_scale))
        throw std::runtime_error(
            "ORC decimal scale evolution requires Java reader");
      if (type.getPrecision() <= 18 && type.getPrecision() != 0) {
        ArrowDecimalSetInt(
            &decimal,
            dynamic_cast<const orc::Decimal64VectorBatch &>(in).values[i]);
      } else {
        auto value =
            dynamic_cast<const orc::Decimal128VectorBatch &>(in).values[i];
        decimal.words[decimal.high_word_index] =
            static_cast<uint64_t>(value.getHighBits());
        decimal.words[decimal.low_word_index] = value.getLowBits();
      }
      check(ArrowArrayAppendDecimal(out, &decimal));
      break;
    }
    case orc::TIMESTAMP:
    case orc::TIMESTAMP_INSTANT: {
      const auto &values = dynamic_cast<const orc::TimestampVectorBatch &>(in);
      int64_t units = units_per_second(field.time_unit);
      __int128 result = static_cast<__int128>(values.data[i]) * units +
                        values.nanoseconds[i] / (1000000000 / units);
      if (result > INT64_MAX || result < INT64_MIN)
        throw std::runtime_error("ORC timestamp exceeds Arrow range");
      check(ArrowArrayAppendInt(out, static_cast<int64_t>(result)));
      break;
    }
    case orc::LIST: {
      const auto &values = dynamic_cast<const orc::ListVectorBatch &>(in);
      read_column(*values.elements, *type.getSubtype(0), schema->children[0],
                  out->children[0], values.offsets[i],
                  values.offsets[i + 1] - values.offsets[i]);
      check(ArrowArrayFinishElement(out));
      break;
    }
    case orc::MAP: {
      const auto &values = dynamic_cast<const orc::MapVectorBatch &>(in);
      int64_t start = values.offsets[i], count = values.offsets[i + 1] - start;
      auto *entries = out->children[0];
      auto *entry_schema = schema->children[0];
      read_column(*values.keys, *type.getSubtype(0), entry_schema->children[0],
                  entries->children[0], start, count);
      read_column(*values.elements, *type.getSubtype(1),
                  entry_schema->children[1], entries->children[1], start,
                  count);
      entries->length += count;
      check(ArrowArrayFinishElement(out));
      break;
    }
    default:
      throw std::runtime_error("Unsupported ORC type " + type.toString());
    }
  }
}
class Output final : public orc::OutputStream {
  SfWrite write_;
  void *context_;
  uint64_t length_ = 0;
  const std::string name_ = "StreamFusion host output";

public:
  Output(SfWrite write, void *context) : write_(write), context_(context) {}
  uint64_t getLength() const override { return length_; }
  uint64_t getNaturalWriteSize() const override { return 1024 * 1024; }
  const std::string &getName() const override { return name_; }
  void write(const void *bytes, size_t length) override {
    if (write_(context_, bytes, length))
      throw std::runtime_error("Host ORC output failed");
    length_ += length;
  }
  void close() override {
  } // Java owns recoverable streams and their commit protocol.
  void flush() override {}
};
class Input final : public orc::InputStream {
  SfRead read_;
  void *context_;
  uint64_t length_;
  const std::string name_ = "StreamFusion host input";

public:
  Input(SfRead read, void *context, uint64_t length)
      : read_(read), context_(context), length_(length) {}
  uint64_t getLength() const override { return length_; }
  uint64_t getNaturalReadSize() const override { return 64 * 1024; }
  const std::string &getName() const override { return name_; }
  void read(void *bytes, uint64_t length, uint64_t offset) override {
    if (offset > length_ || length > length_ - offset)
      throw std::runtime_error("ORC read exceeds file length");
    if (read_(context_, offset, length, bytes))
      throw std::runtime_error("Host ORC input failed");
  }
  std::future<void> readAsync(void *bytes, uint64_t length,
                              uint64_t offset) override {
    // All Java callbacks stay on the attached Flink task thread.
    std::promise<void> done;
    try {
      read(bytes, length, offset);
      done.set_value();
    } catch (...) {
      done.set_exception(std::current_exception());
    }
    return done.get_future();
  }
};
class Pool final : public orc::MemoryPool {
  std::unordered_map<char *, uint64_t> allocations_;

public:
  uint64_t bytes = 0;
  char *malloc(uint64_t size) override {
    char *ptr = orc::getDefaultPool()->malloc(size);
    try {
      allocations_.emplace(ptr, size);
    } catch (...) {
      orc::getDefaultPool()->free(ptr);
      throw;
    }
    bytes += size;
    return ptr;
  }
  void free(char *ptr) override {
    auto entry = allocations_.find(ptr);
    if (entry != allocations_.end()) {
      bytes -= entry->second;
      allocations_.erase(entry);
    }
    orc::getDefaultPool()->free(ptr);
  }
};
orc::WriterOptions writer_options(const char *const *keys,
                                  const char *const *values, size_t count) {
  orc::WriterOptions options;
  options.setTimezoneName("GMT");
  options.setCompression(orc::CompressionKind_ZLIB);
  for (size_t i = 0; i < count; ++i) {
    std::string key(keys[i]), value(values[i]);
    if (key == "compression") {
      static const std::unordered_map<std::string, orc::CompressionKind>
          codecs = {{"NONE", orc::CompressionKind_NONE},
                    {"ZLIB", orc::CompressionKind_ZLIB},
                    {"SNAPPY", orc::CompressionKind_SNAPPY},
                    {"LZ4", orc::CompressionKind_LZ4},
                    {"ZSTD", orc::CompressionKind_ZSTD}};
      options.setCompression(codecs.at(value));
    } else if (key == "timezone") {
    } else if (key == "legacy.timestamp-ltz") {
    } else if (key == "stripe.size")
      options.setStripeSize(std::stoull(value));
    else if (key == "compress.size")
      options.setCompressionBlockSize(std::stoull(value));
    else if (key == "row.index.stride")
      options.setRowIndexStride(std::stoull(value));
    else if (key == "dictionary.key.threshold")
      options.setDictionaryKeySizeThreshold(std::stod(value));
    else if (key == "compression.strategy")
      options.setCompressionStrategy(
          value == "SPEED" ? orc::CompressionStrategy_SPEED
                           : orc::CompressionStrategy_COMPRESSION);
    else if (key == "write.format")
      options.setFileVersion(value == "0.11" ? orc::FileVersion::v_0_11()
                                             : orc::FileVersion::v_0_12());
    else if (key == "bloom.filter.fpp")
      options.setBloomFilterFPP(std::stod(value));
    else if (key == "bloom.filter.columns") {
      std::set<uint64_t> columns;
      size_t start = 0;
      while (start < value.size()) {
        size_t end = value.find(',', start);
        columns.insert(std::stoull(value.substr(start, end - start)));
        if (end == std::string::npos)
          break;
        start = end + 1;
      }
      options.setColumnsUseBloomFilter(columns);
    } else
      throw std::runtime_error("Unknown native ORC option " + key);
  }
  // Java accepts compression blocks smaller than C++'s default 64 KiB
  // allocation block. Each compression block must consist of whole allocation
  // blocks in the C++ writer.
  if (options.getCompressionBlockSize() % options.getMemoryBlockSize() != 0)
    options.setMemoryBlockSize(options.getCompressionBlockSize());
  return options;
}
struct Writer {
  Schema schema;
  std::unique_ptr<orc::Type> type;
  Output output;
  Pool pool;
  std::unique_ptr<orc::Writer> writer;
  std::unique_ptr<orc::ColumnVectorBatch> batch;
  bool finished = false;
  Writer(const ArrowSchema *source, const char *description,
         const char *const *keys, const char *const *values, size_t count,
         SfWrite write, void *context)
      : schema(source), type(orc::Type::buildTypeFromString(description)),
        output(write, context) {
    validate(&schema.value, *type);
    attributes(&schema.value, *type);
    auto options = writer_options(keys, values, count);
    options.setMemoryPool(&pool);
    writer = orc::createWriter(*type, &output, options);
    batch = writer->createRowBatch(1024);
  }
};
uint64_t add_size(uint64_t left, uint64_t right) {
  return right > UINT64_MAX - left ? UINT64_MAX : left + right;
}
uint64_t variable_memory(const orc::Type &type,
                         const orc::Statistics *statistics) {
  uint64_t result = 0;
  if (type.getKind() == orc::STRING || type.getKind() == orc::CHAR ||
      type.getKind() == orc::VARCHAR || type.getKind() == orc::BINARY) {
    if (!statistics || type.getColumnId() >= statistics->getNumberOfColumns())
      return UINT64_MAX;
    const auto *column = statistics->getColumnStatistics(type.getColumnId());
    if (column->getNumberOfValues() == 0)
      return 0;
    uint64_t payload;
    if (const auto *text =
            dynamic_cast<const orc::StringColumnStatistics *>(column)) {
      if (!text->hasTotalLength())
        return UINT64_MAX;
      payload = text->getTotalLength();
    } else if (const auto *binary =
                   dynamic_cast<const orc::BinaryColumnStatistics *>(column)) {
      if (!binary->hasTotalLength())
        return UINT64_MAX;
      payload = binary->getTotalLength();
    } else
      return UINT64_MAX;
    // The compressed-stripe estimate omits decoded dictionaries. Account
    // conservatively for their payload, offsets and a second copy in the
    // outgoing Arrow buffers.
    result = add_size(payload, payload);
    uint64_t count = column->getNumberOfValues();
    result =
        add_size(result, count > UINT64_MAX / 16 ? UINT64_MAX : count * 16);
  }
  for (uint64_t c = 0; c < type.getSubtypeCount(); ++c)
    result = add_size(result, variable_memory(*type.getSubtype(c), statistics));
  return result;
}
bool utc_zone(const std::string &zone) {
  return zone == "UTC" || zone == "GMT" || zone == "Etc/UTC" ||
         zone == "Etc/GMT";
}
bool has_kind(const orc::Type &type, orc::TypeKind kind) {
  if (type.getKind() == kind)
    return true;
  for (uint64_t c = 0; c < type.getSubtypeCount(); ++c)
    if (has_kind(*type.getSubtype(c), kind))
      return true;
  return false;
}
struct Reader {
  Schema schema;
  std::unique_ptr<orc::Reader> reader;
  std::unique_ptr<orc::RowReader> rows;
  std::unique_ptr<orc::ColumnVectorBatch> batch;
  std::vector<uint64_t> columns;
  uint64_t memory;
  bool compatible = true;
  Reader(const ArrowSchema *source, const char *const *names, size_t count,
         uint64_t length, uint64_t batch_size, const char *instant_zone,
         SfRead read, void *context)
      : schema(source) {
    reader = orc::createReader(std::make_unique<Input>(read, context, length),
                               orc::ReaderOptions());
    // Only the codecs covered by Java/native cross-reading enter a native
    // split.
    compatible = reader->getCompression() != orc::CompressionKind_LZO;
    std::list<uint64_t> include;
    const auto &type = reader->getType();
    for (size_t i = 0; i < count; ++i) {
      bool found = false;
      for (uint64_t c = 0; c < type.getSubtypeCount(); ++c) {
        if (type.getFieldName(c) == names[i]) {
          include.push_back(c);
          found = true;
          break;
        }
      }
      if (!found)
        throw std::runtime_error(std::string("Missing ORC field ") + names[i]);
    }
    memory = reader->getMemoryUseByFieldId(include);
    orc::RowReaderOptions options;
    options.include(include).setTimezoneName("GMT");
    rows = reader->createRowReader(options);
    const auto &selected = rows->getSelectedType();
    for (size_t i = 0; i < count; ++i) {
      for (uint64_t c = 0; c < selected.getSubtypeCount(); ++c) {
        if (selected.getFieldName(c) == names[i]) {
          columns.push_back(c);
          break;
        }
      }
    }
    if (columns.size() != count)
      throw std::runtime_error("Invalid ORC projection");
    for (size_t i = 0; i < count; ++i)
      validate(schema.value.children[i], *selected.getSubtype(columns[i]));
    uint64_t variable = 0;
    if (reader->getNumberOfStripeStatistics() != reader->getNumberOfStripes()) {
      variable = variable_memory(selected, nullptr);
    } else {
      for (uint64_t stripe = 0; stripe < reader->getNumberOfStripes();
           ++stripe) {
        auto statistics = reader->getStripeStatistics(stripe, false);
        variable =
            std::max(variable, variable_memory(selected, statistics.get()));
      }
    }
    if (has_kind(selected, orc::TIMESTAMP)) {
      for (uint64_t stripe = 0; stripe < reader->getNumberOfStripes(); ++stripe)
        compatible &= utc_zone(reader->getStripe(stripe)->getWriterTimezone());
    }
    if (instant_zone && *instant_zone &&
        has_kind(selected, orc::TIMESTAMP_INSTANT))
      compatible &= utc_zone(instant_zone);
    memory = compatible ? add_size(memory, variable) : UINT64_MAX;
    batch = rows->createRowBatch(batch_size);
  }
};
template <typename F, typename R> R guarded(F operation, R failure) noexcept {
  try {
    return operation();
  } catch (const std::exception &error) {
    std::snprintf(last_error, sizeof(last_error), "%s", error.what());
  } catch (...) {
    std::snprintf(last_error, sizeof(last_error), "%s",
                  "Unknown ORC C++ exception");
  }
  return failure;
}
} // namespace
extern "C" const char *sf_orc_error() { return last_error; }
extern "C" void *sf_orc_writer_new(const ArrowSchema *schema,
                                   const char *description,
                                   const char *const *keys,
                                   const char *const *values, size_t count,
                                   SfWrite write, void *context) {
  return guarded(
      [&]() -> void * {
        return new Writer(schema, description, keys, values, count, write,
                          context);
      },
      static_cast<void *>(nullptr));
}
extern "C" int sf_orc_writer_write(void *handle, const ArrowArray *array) {
  return guarded(
      [&]() {
        auto &writer = *static_cast<Writer *>(handle);
        if (writer.finished)
          throw std::runtime_error("ORC writer is finished");
        View view(&writer.schema.value, array);
        write_column(view.value, &writer.schema.value, *writer.type,
                     *writer.batch, 0, array->length, 0, nullptr);
        writer.writer->add(*writer.batch);
        return 0;
      },
      -1);
}
extern "C" int sf_orc_writer_finish(void *handle) {
  return guarded(
      [&]() {
        auto &writer = *static_cast<Writer *>(handle);
        if (!writer.finished) {
          writer.writer->close();
          writer.finished = true;
        }
        return 0;
      },
      -1);
}
extern "C" uint64_t sf_orc_writer_size(void *handle) {
  auto &writer = *static_cast<Writer *>(handle);
  return writer.output.getLength() + (writer.finished ? 0 : writer.pool.bytes);
}
extern "C" void sf_orc_writer_free(void *handle) {
  delete static_cast<Writer *>(handle);
}
extern "C" void *sf_orc_reader_new(const ArrowSchema *schema,
                                   const char *const *names, size_t count,
                                   uint64_t length, uint64_t batch_size,
                                   const char *instant_zone, SfRead read,
                                   void *context) {
  return guarded(
      [&]() -> void * {
        return new Reader(schema, names, count, length, batch_size,
                          instant_zone, read, context);
      },
      static_cast<void *>(nullptr));
}
extern "C" int sf_orc_reader_next(void *handle, ArrowArray *array) {
  return guarded(
      [&]() {
        auto &reader = *static_cast<Reader *>(handle);
        if (!reader.compatible)
          throw std::runtime_error(
              "ORC codec or timestamp timezone requires the Java reader");
        if (!reader.rows->next(*reader.batch))
          return 0;
        check(ArrowArrayInitFromSchema(array, &reader.schema.value, nullptr));
        try {
          check(ArrowArrayStartAppending(array));
          auto &batch = dynamic_cast<orc::StructVectorBatch &>(*reader.batch);
          const auto &type = reader.rows->getSelectedType();
          for (size_t i = 0; i < reader.columns.size(); ++i) {
            auto c = reader.columns[i];
            read_column(*batch.fields[c], *type.getSubtype(c),
                        reader.schema.value.children[i], array->children[i], 0,
                        batch.numElements);
          }
          array->length = batch.numElements;
          check(ArrowArrayFinishBuildingDefault(array, nullptr));
        } catch (...) {
          array->release(array);
          throw;
        }
        return 1;
      },
      -1);
}
extern "C" uint64_t sf_orc_reader_memory(void *handle) {
  return static_cast<Reader *>(handle)->memory;
}
extern "C" void sf_orc_reader_free(void *handle) {
  delete static_cast<Reader *>(handle);
}
