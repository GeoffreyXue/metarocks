
#include "parquet_writer.h"

#include <iostream>
#include <memory>

#include "arrow/io/file.h"
#include "arrow/io/type_fwd.h"
#include "arrow/io/api.h"
#include "arrow/json/options.h"
#include "arrow/json/api.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type_fwd.h"

#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

ParquetWriter::ParquetWriter(const std::string& path) {
  arrow::Status status = InitOutputStream(path);
  if (!status.ok()) {
    std::cerr << "Error initializing: " << status.ToString() << std::endl;
  }
  assert(status.ok());
}

void ParquetWriter::Add(const Slice& value) {
  if (parquet_writer_.get() == nullptr) {
    // Init writer
    arrow::Status status = InitWriter(value);
    if (!status.ok()) {
      std::cerr << "Error initing writer: " << status.ToString() << std::endl;
    }
    assert(status.ok());
  }

  {
    arrow::Status status = AddRecord(value);
    if (!status.ok()) {
      std::cerr << "Error adding record: " << status.ToString() << std::endl;
    }
    assert(status.ok());
  }
}

void ParquetWriter::Close() {
  arrow::Status status = CloseWriter();
  if (!status.ok()) {
    std::cerr << "Error closing writer: " << status.ToString() << std::endl;
  }
  assert(status.ok());
}

arrow::Status ParquetWriter::InitOutputStream(const std::string& path) {
  ARROW_ASSIGN_OR_RAISE(outfile_, arrow::io::FileOutputStream::Open(path));

  return arrow::Status();
}

arrow::Status ParquetWriter::InitWriter(const Slice& value) {
  std::string value_string(value.data(), value.size());

  const uint8_t* data = reinterpret_cast<const uint8_t*>(value.data());
  int64_t size = static_cast<int64_t>(value.size());

  std::shared_ptr<arrow::Buffer> buffer = arrow::Buffer::Wrap(data, size);
  std::shared_ptr<arrow::io::BufferReader> buffer_reader = std::make_shared<arrow::io::BufferReader>(buffer);

  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::json::TableReader> reader, 
    arrow::json::TableReader::Make(
      arrow::default_memory_pool(), 
      buffer_reader, 
      arrow::json::ReadOptions::Defaults(), 
      arrow::json::ParseOptions::Defaults()
    ));

  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Table> table, reader->Read());

  std::shared_ptr<arrow::Schema> schema = table->schema();

  ARROW_ASSIGN_OR_RAISE(parquet_writer_,
                        parquet::arrow::FileWriter::Open(
                        *schema, arrow::default_memory_pool(), outfile_));

  return arrow::Status();
}

arrow::Status ParquetWriter::AddRecord(const Slice& value) {
  std::string value_string(value.data(), value.size());

  const uint8_t* data = reinterpret_cast<const uint8_t*>(value.data());
  int64_t size = static_cast<int64_t>(value.size());

  std::shared_ptr<arrow::Buffer> buffer = arrow::Buffer::Wrap(data, size);
  std::shared_ptr<arrow::io::BufferReader> buffer_reader = std::make_shared<arrow::io::BufferReader>(buffer);

  arrow::json::ParseOptions parse_options = arrow::json::ParseOptions::Defaults();
  parse_options.explicit_schema = parquet_writer_->schema();

  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::json::TableReader> reader, 
    arrow::json::TableReader::Make(
      arrow::default_memory_pool(), 
      buffer_reader, 
      arrow::json::ReadOptions::Defaults(), 
      parse_options
  ));

  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Table> table, reader->Read());

  ARROW_RETURN_NOT_OK(parquet_writer_->WriteTable(*table));

  return arrow::Status();
}

arrow::Status ParquetWriter::CloseWriter() {
  ARROW_RETURN_NOT_OK(parquet_writer_->Close());

  return arrow::Status();
}


}  // namespace ROCKSDB_NAMESPACE