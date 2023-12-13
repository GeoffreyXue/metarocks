

#include "parquet_writer.h"
#include <filesystem>

#include "arrow/io/memory.h"
#include "arrow/json/parser.h"
#include "arrow/json/reader.h"
#include "arrow/table.h"
#include "arrow/table_builder.h"
#include "parquet/exception.h"

namespace fs = std::filesystem;

namespace ROCKSDB_NAMESPACE {

ParquetWriter::ParquetWriter(const string& filePath) {
  fs::path path(filePath);

  if (!fs::exists(path.parent_path())) {
    fs::create_directories(path.parent_path());
  }

  PARQUET_ASSIGN_OR_THROW(outfile_, arrow::io::FileOutputStream::Open(filePath));
}

void ParquetWriter::Add(const Slice& value) {
  std::string value_string(value.data(), value.size());
  // init all via first record - schema, parquet_writer, and builder
  if (parquet_writer_.get() == nullptr) {
    Init(value_string);
  }

  Write(value_string);
}

void ParquetWriter::Close() {
  if (parquet_writer_.get() == nullptr) {
    return;
  }
  PARQUET_THROW_NOT_OK(parquet_writer_->Close());
}

void ParquetWriter::Init(const string& value) {
  // cast value into buffer reader
  const uint8_t* data = reinterpret_cast<const uint8_t*>(value.data());
  int64_t size = static_cast<int64_t>(value.size());
  std::shared_ptr<arrow::Buffer> buffer = arrow::Buffer::Wrap(data, size);
  std::shared_ptr<arrow::io::BufferReader> buffer_reader =
      std::make_shared<arrow::io::BufferReader>(buffer);

  // read json, parse into table
  std::shared_ptr<arrow::json::TableReader> reader;
  PARQUET_ASSIGN_OR_THROW(reader,
                          arrow::json::TableReader::Make(
                              arrow::default_memory_pool(), buffer_reader,
                              arrow::json::ReadOptions::Defaults(),
                              arrow::json::ParseOptions::Defaults()));
  std::shared_ptr<arrow::Table> table;
  PARQUET_ASSIGN_OR_THROW(table, reader->Read());

  // read schema, init writer and builder
  schema_ = table->schema();
  PARQUET_ASSIGN_OR_THROW(
      parquet_writer_, parquet::arrow::FileWriter::Open(
                           *schema_, arrow::default_memory_pool(), outfile_));
};

void ParquetWriter::Write(const string& value) {
  // parse data into arrow table
  const uint8_t* data = reinterpret_cast<const uint8_t*>(value.data());
  int64_t size = static_cast<int64_t>(value.size());
  std::shared_ptr<arrow::Buffer> buffer = arrow::Buffer::Wrap(data, size);
  std::shared_ptr<arrow::io::BufferReader> buffer_reader =
      std::make_shared<arrow::io::BufferReader>(buffer);

  // use explicit schema learned from first record
  arrow::json::ParseOptions parse_options =
      arrow::json::ParseOptions::Defaults();
  parse_options.explicit_schema = parquet_writer_->schema();

  std::shared_ptr<arrow::json::TableReader> reader;
  PARQUET_ASSIGN_OR_THROW(
      reader, arrow::json::TableReader::Make(
                  arrow::default_memory_pool(), buffer_reader,
                  arrow::json::ReadOptions::Defaults(), parse_options));
  std::shared_ptr<arrow::Table> table;
  PARQUET_ASSIGN_OR_THROW(table, reader->Read());

  // convert arrow table to record batch, write out
  std::shared_ptr<arrow::RecordBatch> batch;
  PARQUET_ASSIGN_OR_THROW(batch, table->CombineChunksToBatch());
  PARQUET_THROW_NOT_OK(parquet_writer_->WriteRecordBatch(*batch));
}

}  // namespace ROCKSDB_NAMESPACE