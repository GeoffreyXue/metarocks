
#pragma once

#include <memory>

#include "arrow/api.h"
#include "parquet/arrow/writer.h"

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

class ParquetWriter {
public:
  ParquetWriter(const std::string& path);

  void Add(const Slice& value);

  void Close();

private:
  arrow::Status InitOutputStream(const std::string& path);

  arrow::Status InitWriter(const Slice& value);

  arrow::Status AddRecord(const Slice& value);

  arrow::Status CloseWriter();

  std::shared_ptr<arrow::io::FileOutputStream> outfile_;
  std::shared_ptr<parquet::arrow::FileWriter> parquet_writer_;
};

}  // namespace ROCKSDB_NAMESPACE