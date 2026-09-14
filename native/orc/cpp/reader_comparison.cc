#include <algorithm>
#include <arrow/adapters/orc/adapter.h>
#include <arrow/c/bridge.h>
#include <arrow/io/interfaces.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/buffer.h>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

using SfRead = int (*)(void *, uint64_t, uint64_t, void *);
namespace {
thread_local std::string last_error;

template <typename T> T unwrap(arrow::Result<T> result) {
  if (!result.ok()) throw std::runtime_error(result.status().ToString());
  return std::move(result).ValueOrDie();
}
void check(arrow::Status status) {
  if (!status.ok()) throw std::runtime_error(status.ToString());
}

class HostFile final : public arrow::io::RandomAccessFile {
  SfRead read_;
  void *context_;
  int64_t length_, position_ = 0;
  bool closed_ = false;
public:
  HostFile(SfRead read, void *context, int64_t length)
      : read_(read), context_(context), length_(length) {}
  arrow::Status Close() override { closed_ = true; return arrow::Status::OK(); }
  bool closed() const override { return closed_; }
  arrow::Result<int64_t> Tell() const override { return position_; }
  arrow::Result<int64_t> GetSize() override { return length_; }
  arrow::Status Seek(int64_t offset) override {
    if (offset < 0 || offset > length_) return arrow::Status::IOError("Invalid seek");
    position_ = offset;
    return arrow::Status::OK();
  }
  arrow::Result<int64_t> ReadAt(int64_t offset, int64_t count, void *out) override {
    if (closed_ || offset < 0 || count < 0 || offset > length_)
      return arrow::Status::IOError("Invalid read");
    count = std::min(count, length_ - offset);
    if (count && read_(context_, offset, count, out))
      return arrow::Status::IOError("Host ORC input failed");
    return count;
  }
  arrow::Result<std::shared_ptr<arrow::Buffer>> ReadAt(int64_t offset, int64_t count) override {
    ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(count));
    ARROW_ASSIGN_OR_RAISE(auto actual, ReadAt(offset, count, buffer->mutable_data()));
    ARROW_RETURN_NOT_OK(buffer->Resize(actual));
    return std::shared_ptr<arrow::Buffer>(std::move(buffer));
  }
  arrow::Result<int64_t> Read(int64_t count, void *out) override {
    ARROW_ASSIGN_OR_RAISE(auto actual, ReadAt(position_, count, out));
    position_ += actual;
    return actual;
  }
  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t count) override {
    ARROW_ASSIGN_OR_RAISE(auto buffer, ReadAt(position_, count));
    position_ += buffer->size();
    return buffer;
  }
};

struct Reader {
  std::unique_ptr<arrow::adapters::orc::ORCFileReader> file;
  std::shared_ptr<arrow::RecordBatchReader> batches;
};
}

extern "C" const char *sf_arrow_orc_error() { return last_error.c_str(); }
extern "C" void *sf_arrow_orc_open(SfRead read, void *context, int64_t length,
    int64_t batch_size, const char *const *names, size_t count, ArrowSchema *schema) {
  try {
    auto reader = std::make_unique<Reader>();
    reader->file = unwrap(arrow::adapters::orc::ORCFileReader::Open(
        std::make_shared<HostFile>(read, context, length), arrow::default_memory_pool()));
    std::vector<std::string> projection(names, names + count);
    reader->batches = unwrap(reader->file->GetRecordBatchReader(batch_size, projection));
    check(arrow::ExportSchema(*reader->batches->schema(), schema));
    return reader.release();
  } catch (const std::exception &error) {
    last_error = error.what();
    return nullptr;
  } catch (...) {
    last_error = "Unknown Arrow C++ exception";
    return nullptr;
  }
}
extern "C" int sf_arrow_orc_next(void *handle, ArrowArray *array) {
  try {
    std::shared_ptr<arrow::RecordBatch> batch;
    check(static_cast<Reader *>(handle)->batches->ReadNext(&batch));
    if (!batch) return 0;
    check(arrow::ExportRecordBatch(*batch, array));
    return 1;
  } catch (const std::exception &error) {
    last_error = error.what();
    return -1;
  } catch (...) {
    last_error = "Unknown Arrow C++ exception";
    return -1;
  }
}
extern "C" void sf_arrow_orc_close(void *handle) { delete static_cast<Reader *>(handle); }
