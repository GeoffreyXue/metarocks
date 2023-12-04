
#pragma once

#include "arrow/api.h"
#include "parquet/arrow/writer.h"

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

class ParquetWriter {
public:
  ParquetWriter(const std::string& path);

  void Add(const Slice& key, const Slice& value);

  void Close();

private:
  void WriteRecordBatch(const std::shared_ptr<arrow::RecordBatch>& record_batch);

  std::shared_ptr<arrow::RecordBatchBuilder> builder_;
  std::shared_ptr<parquet::arrow::FileWriter> parquet_writer_;
  std::shared_ptr<arrow::Schema> schema_;
};

}  // namespace ROCKSDB_NAMESPACE