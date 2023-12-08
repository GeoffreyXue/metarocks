#pragma once

#include <stddef.h>
#include <stdint.h>

#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "rocksdb/compaction_filter.h"
#include "rocksdb/comparator.h"
#include "rocksdb/compression_type.h"
#include "rocksdb/customizable.h"
#include "rocksdb/data_structure.h"
#include "rocksdb/env.h"
#include "rocksdb/file_checksum.h"
#include "rocksdb/listener.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/sst_partitioner.h"
#include "rocksdb/statistics.h"
#include "rocksdb/types.h"
#include "rocksdb/universal_compaction.h"
#include "rocksdb/version.h"
#include "rocksdb/write_buffer_manager.h"

namespace ROCKSDB_NAMESPACE {

enum class CompactionServiceJobStatus : char {
  kSuccess,
  kFailure,
  kUseLocal,
};

struct CompactionServiceJobInfo {
  std::string db_name;
  std::string db_id;
  std::string db_session_id;
  uint64_t job_id;  // job_id is only unique within the current DB and session,
                    // restart DB will reset the job_id. `db_id` and
                    // `db_session_id` could help you build unique id across
                    // different DBs and sessions.

  Env::Priority priority;

  CompactionServiceJobInfo(std::string db_name_, std::string db_id_,
                           std::string db_session_id_, uint64_t job_id_,
                           Env::Priority priority_)
      : db_name(std::move(db_name_)),
        db_id(std::move(db_id_)),
        db_session_id(std::move(db_session_id_)),
        job_id(job_id_),
        priority(priority_) {}
};

struct CompactionServiceOptionsOverride {
  // Currently pointer configurations are not passed to compaction service
  // compaction so the user needs to set it. It will be removed once pointer
  // configuration passing is supported.
  Env* env = Env::Default();
  std::shared_ptr<FileChecksumGenFactory> file_checksum_gen_factory = nullptr;

  const Comparator* comparator = BytewiseComparator();
  std::shared_ptr<MergeOperator> merge_operator = nullptr;
  const CompactionFilter* compaction_filter = nullptr;
  std::shared_ptr<CompactionFilterFactory> compaction_filter_factory = nullptr;
  std::shared_ptr<const SliceTransform> prefix_extractor = nullptr;
  std::shared_ptr<TableFactory> table_factory;
  std::shared_ptr<SstPartitionerFactory> sst_partitioner_factory = nullptr;

  // Only subsets of events are triggered in remote compaction worker, like:
  // `OnTableFileCreated`, `OnTableFileCreationStarted`,
  // `ShouldBeNotifiedOnFileIO` `OnSubcompactionBegin`,
  // `OnSubcompactionCompleted`, etc. Worth mentioning, `OnCompactionBegin` and
  // `OnCompactionCompleted` won't be triggered. They will be triggered on the
  // primary DB side.
  std::vector<std::shared_ptr<EventListener>> listeners;

  // statistics is used to collect DB operation metrics, the metrics won't be
  // returned to CompactionService primary host, to collect that, the user needs
  // to set it here.
  std::shared_ptr<Statistics> statistics = nullptr;

  // Only compaction generated SST files use this user defined table properties
  // collector.
  std::vector<std::shared_ptr<TablePropertiesCollectorFactory>>
      table_properties_collector_factories;
};

// Exceptions MUST NOT propagate out of overridden functions into RocksDB,
// because RocksDB is not exception-safe. This could cause undefined behavior
// including data loss, unreported corruption, deadlocks, and more.
class CompactionService : public Customizable {
 public:
  static const char* Type() { return "CompactionService"; }

  // Returns the name of this compaction service.
  const char* Name() const override = 0;

  // Start the remote compaction with `compaction_service_input`, which can be
  // passed to `DB::OpenAndCompact()` on the remote side. `info` provides the
  // information the user might want to know, which includes `job_id`.
  virtual CompactionServiceJobStatus StartV2(
      const CompactionServiceJobInfo& /*info*/,
      const std::string& /*compaction_service_input*/) {
    return CompactionServiceJobStatus::kUseLocal;
  }

  // Wait for remote compaction to finish.
  virtual CompactionServiceJobStatus WaitForCompleteV2(
      const CompactionServiceJobInfo& /*info*/,
      std::string* /*compaction_service_result*/) {
    return CompactionServiceJobStatus::kUseLocal;
  }

  ~CompactionService() override = default;
};
} // namespace ROCKSDB_NAMESPACE