#include "rocksdb/compaction_service.h"

#include "monitoring/instrumented_mutex.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"

#include <thread>

namespace ROCKSDB_NAMESPACE {

CompactionServiceJobStatus ExternalCompactionService::StartV2(
    const CompactionServiceJobInfo& info,
    const std::string& compaction_service_input) {
  std::scoped_lock lock(mutex_);
  start_info_ = info;
  assert(info.db_name == db_path_);
  jobs_.emplace(info.job_id, compaction_service_input);
  CompactionServiceJobStatus s = CompactionServiceJobStatus::kSuccess;
  if (is_override_start_status_) {
    return override_start_status_;
  }
  return s;
}

void OpenAndCompactInThread(
    const OpenAndCompactOptions& options, const std::string& name,
    const std::string& output_directory, const std::string& input,
    std::string* output,
    const CompactionServiceOptionsOverride& override_options, Status* s) {
  *s = DB::OpenAndCompact(
      options, name, output_directory,
      input, output, override_options);
}

CompactionServiceJobStatus ExternalCompactionService::WaitForCompleteV2(
    const CompactionServiceJobInfo& info,
    std::string* compaction_service_result) {
  std::string compaction_input;
  assert(info.db_name == db_path_);
  {
    std::scoped_lock lock(mutex_);
    wait_info_ = info;
    auto i = jobs_.find(info.job_id);
    if (i == jobs_.end()) {
      return CompactionServiceJobStatus::kFailure;
    }
    compaction_input = std::move(i->second);
    jobs_.erase(i);
  }

  if (is_override_wait_status_) {
    return override_wait_status_;
  }

  CompactionServiceOptionsOverride options_override;
  options_override.env = options_->env;
  options_override.file_checksum_gen_factory =
      options_->file_checksum_gen_factory;
  options_override.comparator = options_->comparator;
  options_override.merge_operator = options_->merge_operator;
  options_override.compaction_filter = options_->compaction_filter;
  options_override.compaction_filter_factory =
      options_->compaction_filter_factory;
  options_override.prefix_extractor = options_->prefix_extractor;
  options_override.table_factory = options_->table_factory;
  options_override.sst_partitioner_factory = options_->sst_partitioner_factory;
  options_override.statistics = statistics_;
  if (!listeners_.empty()) {
    options_override.listeners = listeners_;
  }

  if (!table_properties_collector_factories_.empty()) {
    options_override.table_properties_collector_factories =
        table_properties_collector_factories_;
  }

  OpenAndCompactOptions options;
  options.canceled = &canceled_;

  Status s;
  {
    const rocksdb::OpenAndCompactOptions oac_options;
    const std::string name = db_path_;
    const std::string output_directory = db_path_ + "/" + std::to_string(info.job_id);
    const std::string input = compaction_input;
    std::string * output = compaction_service_result;
    const rocksdb::CompactionServiceOptionsOverride override_options = options_override;

    /*s = DB::OpenAndCompact(
      options, name, output_directory,
      input, output, override_options);*/

    std::thread t(OpenAndCompactInThread, oac_options, name, output_directory, input, output, override_options, &s);
    t.join();
  }

  
  if (is_override_wait_result_) {
    *compaction_service_result = override_wait_result_;
  }
  compaction_num_.fetch_add(1);
  if (s.ok()) {
    return CompactionServiceJobStatus::kSuccess;
  } else {
    return CompactionServiceJobStatus::kFailure;
  }
}

ExternalCompactionService::~ExternalCompactionService() {
    // Perform any necessary cleanup or resource release here.
    // No need to explicitly delete members because smart pointers will handle it.
    return;
}


}  // namespace ROCKSDB_NAMESPACE