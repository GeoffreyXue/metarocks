
#include "arrow/io/file.h"
#include "arrow/result.h"
#include "arrow/status.h"

#include "parquet_writer.h"
#include <iostream>

namespace ROCKSDB_NAMESPACE {

ParquetWriter::ParquetWriter(const std::string& path) {
  arrow::Status status = Init(path);
  if (!status.ok()) {
    std::cerr << "Error initializing: " << status.ToString() << std::endl;
  }
}

void ParquetWriter::Add(const Slice& key, const Slice& value) {
  arrow::Status status = AddRecord(key, value);
  if (!status.ok()) {
    std::cerr << "Error adding record: " << status.ToString() << std::endl;
  }
}

void ParquetWriter::Close() {
  arrow::Status status = CloseWriter();
  if (!status.ok()) {
    std::cerr << "Error closing writer: " << status.ToString() << std::endl;
  }
}

arrow::Status ParquetWriter::Init(const std::string& path) {
  schema_ = arrow::schema({
    arrow::field("key", arrow::utf8()), 
    arrow::field("value", arrow::utf8())
  });
  
  // Parquet Writer
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  ARROW_ASSIGN_OR_RAISE(
    outfile, 
    arrow::io::FileOutputStream::Open(path));

  ARROW_ASSIGN_OR_RAISE(
    parquet_writer_,
    parquet::arrow::FileWriter::Open(*schema_, arrow::default_memory_pool(), outfile)
  );

  // Record Batch Builder
  ARROW_ASSIGN_OR_RAISE(
    builder_,
    arrow::RecordBatchBuilder::Make(schema_, arrow::default_memory_pool())
  );

  return arrow::Status();
}

arrow::Status ParquetWriter::AddRecord(const Slice& key, const Slice& value) {
  ARROW_RETURN_NOT_OK(builder_->GetFieldAs<arrow::StringBuilder>(0)->Append(key.data(), key.size()));
  ARROW_RETURN_NOT_OK(builder_->GetFieldAs<arrow::StringBuilder>(1)->Append(value.data(), value.size()));

  std::shared_ptr<arrow::RecordBatch> record_batch;
  ARROW_ASSIGN_OR_RAISE(
    record_batch,
    builder_->Flush()
  );

  ARROW_RETURN_NOT_OK(WriteRecordBatch(record_batch));

  return arrow::Status();
}

arrow::Status ParquetWriter::WriteRecordBatch(const std::shared_ptr<arrow::RecordBatch>& record_batch) {
  // Write the Arrow RecordBatch directly to the Parquet file
  ARROW_RETURN_NOT_OK(parquet_writer_->WriteTable(
      *arrow::Table::FromRecordBatches(schema_, {record_batch}).ValueOrDie(),
      record_batch->num_rows()));
  
  return arrow::Status();
}

arrow::Status ParquetWriter::CloseWriter() {
  ARROW_RETURN_NOT_OK(parquet_writer_->Close());

  return arrow::Status();
}

}  // namespace ROCKSDB_NAMESPACE