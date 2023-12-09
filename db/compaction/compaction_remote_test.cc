//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <iostream>

#include "db.h"
#include "db/compaction/compaction.h"
#include "db/compaction/compaction_job.h"
#include "rocksdb/compaction_service.h"
#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "table/unique_id_impl.h"

namespace ROCKSDB_NAMESPACE {

class CompactionRemoteTest : public DBTestBase {
 public:
  explicit CompactionRemoteTest()
      : DBTestBase("compaction_remote_test", true) {}

 protected:
  void ReopenWithCompactionService(Options* options) {
    options->env = env_;
    primary_statistics_ = CreateDBStatistics();
    options->statistics = primary_statistics_;
    compactor_statistics_ = CreateDBStatistics();

    compaction_service_ = std::make_shared<ExternalCompactionService>(
        dbname_, *options, compactor_statistics_, remote_listeners,
        remote_table_properties_collector_factories);
    options->compaction_service = compaction_service_;
    DestroyAndReopen(*options);
  }

  Statistics* GetCompactorStatistics() { return compactor_statistics_.get(); }

  Statistics* GetPrimaryStatistics() { return primary_statistics_.get(); }

  ExternalCompactionService* GetCompactionService() {
    CompactionService* cs = compaction_service_.get();
    return static_cast_with_check<ExternalCompactionService>(cs);
  }

  void GenerateTestData() {
    // Generate 20 files @ L2
    for (int i = 0; i < 20; i++) {
      for (int j = 0; j < 10; j++) {
        int key_id = i * 10 + j;
        ASSERT_OK(Put(Key(key_id), "value" + std::to_string(key_id)));
      }
      ASSERT_OK(Flush());
    }
    MoveFilesToLevel(2);

    // Generate 10 files @ L1 overlap with all 20 files @ L2
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        int key_id = i * 20 + j * 2;
        ASSERT_OK(Put(Key(key_id), "value_new" + std::to_string(key_id)));
      }
      ASSERT_OK(Flush());
    }
    MoveFilesToLevel(1);
    ASSERT_EQ(FilesPerLevel(), "0,10,20");
  }

  void VerifyTestData() {
    for (int i = 0; i < 200; i++) {
      auto result = Get(Key(i));
      if (i % 2) {
        ASSERT_EQ(result, "value" + std::to_string(i));
      } else {
        ASSERT_EQ(result, "value_new" + std::to_string(i));
      }
    }
  }

  std::vector<std::shared_ptr<EventListener>> remote_listeners;
  std::vector<std::shared_ptr<TablePropertiesCollectorFactory>>
      remote_table_properties_collector_factories;

 private:
  std::shared_ptr<Statistics> compactor_statistics_;
  std::shared_ptr<Statistics> primary_statistics_;
  std::shared_ptr<CompactionService> compaction_service_;
};

TEST_F(CompactionRemoteTest, BasicCompactions) {
  Options options = CurrentOptions();
  ReopenWithCompactionService(&options);

  Statistics* primary_statistics = GetPrimaryStatistics();
  Statistics* compactor_statistics = GetCompactorStatistics();

  for (int i = 0; i < 20; i++) {
    for (int j = 0; j < 10; j++) {
      int key_id = i * 10 + j;
      ASSERT_OK(Put(Key(key_id), "value" + std::to_string(key_id)));
    }
    ASSERT_OK(Flush());
  }

  for (int i = 0; i < 10; i++) {
    for (int j = 0; j < 10; j++) {
      int key_id = i * 20 + j * 2;
      ASSERT_OK(Put(Key(key_id), "value_new" + std::to_string(key_id)));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // verify result
  for (int i = 0; i < 200; i++) {
    auto result = Get(Key(i));
    if (i % 2) {
      ASSERT_EQ(result, "value" + std::to_string(i));
    } else {
      ASSERT_EQ(result, "value_new" + std::to_string(i));
    }
  }
  auto my_cs = GetCompactionService();
  ASSERT_GE(my_cs->GetCompactionNum(), 1);

  // make sure the compaction statistics is only recorded on the remote side
  ASSERT_GE(compactor_statistics->getTickerCount(COMPACT_WRITE_BYTES), 1);
  ASSERT_GE(compactor_statistics->getTickerCount(COMPACT_READ_BYTES), 1);
  ASSERT_EQ(primary_statistics->getTickerCount(COMPACT_WRITE_BYTES), 0);
  // even with remote compaction, primary host still needs to read SST files to
  // `verify_table()`.
  ASSERT_GE(primary_statistics->getTickerCount(COMPACT_READ_BYTES), 1);
  // all the compaction write happens on the remote side
  ASSERT_EQ(primary_statistics->getTickerCount(REMOTE_COMPACT_WRITE_BYTES),
            compactor_statistics->getTickerCount(COMPACT_WRITE_BYTES));
  ASSERT_GE(primary_statistics->getTickerCount(REMOTE_COMPACT_READ_BYTES), 1);
  ASSERT_GT(primary_statistics->getTickerCount(COMPACT_READ_BYTES),
            primary_statistics->getTickerCount(REMOTE_COMPACT_READ_BYTES));
  // compactor is already the remote side, which doesn't have remote
  ASSERT_EQ(compactor_statistics->getTickerCount(REMOTE_COMPACT_READ_BYTES), 0);
  ASSERT_EQ(compactor_statistics->getTickerCount(REMOTE_COMPACT_WRITE_BYTES),
            0);

  // Test failed compaction
  SyncPoint::GetInstance()->SetCallBack(
      "DBImplSecondary::CompactWithoutInstallation::End", [&](void* status) {
        // override job status
        auto s = static_cast<Status*>(status);
        *s = Status::Aborted("ExternalCompactionService failed to compact!");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  Status s;
  for (int i = 0; i < 10; i++) {
    for (int j = 0; j < 10; j++) {
      int key_id = i * 20 + j * 2;
      s = Put(Key(key_id), "value_new" + std::to_string(key_id));
      if (s.IsAborted()) {
        break;
      }
    }
    if (s.IsAborted()) {
      break;
    }
    s = Flush();
    if (s.IsAborted()) {
      break;
    }
    s = dbfull()->TEST_WaitForCompact();
    if (s.IsAborted()) {
      break;
    }
  }
  ASSERT_TRUE(s.IsAborted());

  // Test re-open and successful unique id verification
  std::atomic_int verify_passed{0};
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTable::Open::PassedVerifyUniqueId", [&](void* arg) {
        // override job status
        auto id = static_cast<UniqueId64x2*>(arg);
        assert(*id != kNullUniqueId64x2);
        verify_passed++;
      });
  Reopen(options);
  ASSERT_GT(verify_passed, 0);
  Close();
}

TEST_F(CompactionRemoteTest, ManualCompaction) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  ReopenWithCompactionService(&options);
  GenerateTestData();

  auto my_cs = GetCompactionService();

  std::string start_str = Key(15);
  std::string end_str = Key(45);
  Slice start(start_str);
  Slice end(end_str);
  uint64_t comp_num = my_cs->GetCompactionNum();
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &start, &end));
  ASSERT_GE(my_cs->GetCompactionNum(), comp_num + 1);
  VerifyTestData();

  start_str = Key(120);
  start = start_str;
  comp_num = my_cs->GetCompactionNum();
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &start, nullptr));
  ASSERT_GE(my_cs->GetCompactionNum(), comp_num + 1);
  VerifyTestData();

  end_str = Key(92);
  end = end_str;
  comp_num = my_cs->GetCompactionNum();
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, &end));
  ASSERT_GE(my_cs->GetCompactionNum(), comp_num + 1);
  VerifyTestData();

  comp_num = my_cs->GetCompactionNum();
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_GE(my_cs->GetCompactionNum(), comp_num + 1);
  VerifyTestData();
}

TEST_F(CompactionRemoteTest, CancelCompactionOnRemoteSide) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  ReopenWithCompactionService(&options);
  GenerateTestData();

  auto my_cs = GetCompactionService();

  std::string start_str = Key(15);
  std::string end_str = Key(45);
  Slice start(start_str);
  Slice end(end_str);
  uint64_t comp_num = my_cs->GetCompactionNum();

  // Test cancel compaction at the beginning
  my_cs->SetCanceled(true);
  auto s = db_->CompactRange(CompactRangeOptions(), &start, &end);
  ASSERT_TRUE(s.IsIncomplete());
  // compaction number is not increased
  ASSERT_GE(my_cs->GetCompactionNum(), comp_num);
  VerifyTestData();

  // Test cancel compaction in progress
  ReopenWithCompactionService(&options);
  GenerateTestData();
  my_cs = GetCompactionService();
  my_cs->SetCanceled(false);

  std::atomic_bool cancel_issued{false};
  SyncPoint::GetInstance()->SetCallBack("CompactionJob::Run():Inprogress",
                                        [&](void* /*arg*/) {
                                          cancel_issued = true;
                                          my_cs->SetCanceled(true);
                                        });

  SyncPoint::GetInstance()->EnableProcessing();

  s = db_->CompactRange(CompactRangeOptions(), &start, &end);
  ASSERT_TRUE(s.IsIncomplete());
  ASSERT_TRUE(cancel_issued);
  // compaction number is not increased
  ASSERT_GE(my_cs->GetCompactionNum(), comp_num);
  VerifyTestData();
}

TEST_F(CompactionRemoteTest, FailedToStart) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  ReopenWithCompactionService(&options);

  GenerateTestData();

  auto my_cs = GetCompactionService();
  my_cs->OverrideStartStatus(CompactionServiceJobStatus::kFailure);

  std::string start_str = Key(15);
  std::string end_str = Key(45);
  Slice start(start_str);
  Slice end(end_str);
  Status s = db_->CompactRange(CompactRangeOptions(), &start, &end);
  ASSERT_TRUE(s.IsIncomplete());
}

TEST_F(CompactionRemoteTest, InvalidResult) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  ReopenWithCompactionService(&options);

  GenerateTestData();

  auto my_cs = GetCompactionService();
  my_cs->OverrideWaitResult("Invalid Str");

  std::string start_str = Key(15);
  std::string end_str = Key(45);
  Slice start(start_str);
  Slice end(end_str);
  Status s = db_->CompactRange(CompactRangeOptions(), &start, &end);
  ASSERT_FALSE(s.ok());
}

TEST_F(CompactionRemoteTest, SubCompaction) {
  Options options = CurrentOptions();
  options.max_subcompactions = 10;
  options.target_file_size_base = 1 << 10;  // 1KB
  options.disable_auto_compactions = true;
  ReopenWithCompactionService(&options);

  GenerateTestData();
  VerifyTestData();

  auto my_cs = GetCompactionService();
  int compaction_num_before = my_cs->GetCompactionNum();

  auto cro = CompactRangeOptions();
  cro.max_subcompactions = 10;
  Status s = db_->CompactRange(cro, nullptr, nullptr);
  ASSERT_OK(s);
  VerifyTestData();
  int compaction_num = my_cs->GetCompactionNum() - compaction_num_before;
  // make sure there's sub-compaction by checking the compaction number
  ASSERT_GE(compaction_num, 2);
}

class PartialDeleteCompactionFilter : public CompactionFilter {
 public:
  CompactionFilter::Decision FilterV2(
      int /*level*/, const Slice& key, ValueType /*value_type*/,
      const Slice& /*existing_value*/, std::string* /*new_value*/,
      std::string* /*skip_until*/) const override {
    int i = std::stoi(key.ToString().substr(3));
    if (i > 5 && i <= 105) {
      return CompactionFilter::Decision::kRemove;
    }
    return CompactionFilter::Decision::kKeep;
  }

  const char* Name() const override { return "PartialDeleteCompactionFilter"; }
};

TEST_F(CompactionRemoteTest, CompactionFilter) {
  Options options = CurrentOptions();
  std::unique_ptr<CompactionFilter> delete_comp_filter(
      new PartialDeleteCompactionFilter());
  options.compaction_filter = delete_comp_filter.get();
  ReopenWithCompactionService(&options);

  for (int i = 0; i < 20; i++) {
    for (int j = 0; j < 10; j++) {
      int key_id = i * 10 + j;
      ASSERT_OK(Put(Key(key_id), "value" + std::to_string(key_id)));
    }
    ASSERT_OK(Flush());
  }

  for (int i = 0; i < 10; i++) {
    for (int j = 0; j < 10; j++) {
      int key_id = i * 20 + j * 2;
      ASSERT_OK(Put(Key(key_id), "value_new" + std::to_string(key_id)));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // verify result
  for (int i = 0; i < 200; i++) {
    auto result = Get(Key(i));
    if (i > 5 && i <= 105) {
      ASSERT_EQ(result, "NOT_FOUND");
    } else if (i % 2) {
      ASSERT_EQ(result, "value" + std::to_string(i));
    } else {
      ASSERT_EQ(result, "value_new" + std::to_string(i));
    }
  }
  auto my_cs = GetCompactionService();
  ASSERT_GE(my_cs->GetCompactionNum(), 1);
}

TEST_F(CompactionRemoteTest, Snapshot) {
  Options options = CurrentOptions();
  ReopenWithCompactionService(&options);

  ASSERT_OK(Put(Key(1), "value1"));
  ASSERT_OK(Put(Key(2), "value1"));
  const Snapshot* s1 = db_->GetSnapshot();
  ASSERT_OK(Flush());

  ASSERT_OK(Put(Key(1), "value2"));
  ASSERT_OK(Put(Key(3), "value2"));
  ASSERT_OK(Flush());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  auto my_cs = GetCompactionService();
  ASSERT_GE(my_cs->GetCompactionNum(), 1);
  ASSERT_EQ("value1", Get(Key(1), s1));
  ASSERT_EQ("value2", Get(Key(1)));
  db_->ReleaseSnapshot(s1);
}

TEST_F(CompactionRemoteTest, ConcurrentCompaction) {
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 100;
  options.max_background_jobs = 20;
  ReopenWithCompactionService(&options);
  GenerateTestData();

  ColumnFamilyMetaData meta;
  db_->GetColumnFamilyMetaData(&meta);

  std::vector<std::thread> threads;
  for (const auto& file : meta.levels[1].files) {
    threads.emplace_back(std::thread([&]() {
      std::string fname = file.db_path + "/" + file.name;
      ASSERT_OK(db_->CompactFiles(CompactionOptions(), {fname}, 2));
    }));
  }

  for (auto& thread : threads) {
    thread.join();
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // verify result
  for (int i = 0; i < 200; i++) {
    auto result = Get(Key(i));
    if (i % 2) {
      ASSERT_EQ(result, "value" + std::to_string(i));
    } else {
      ASSERT_EQ(result, "value_new" + std::to_string(i));
    }
  }
  auto my_cs = GetCompactionService();
  ASSERT_EQ(my_cs->GetCompactionNum(), 10);
  ASSERT_EQ(FilesPerLevel(), "0,0,10");
}

TEST_F(CompactionRemoteTest, CompactionInfo) {
  Options options = CurrentOptions();
  ReopenWithCompactionService(&options);

  for (int i = 0; i < 20; i++) {
    for (int j = 0; j < 10; j++) {
      int key_id = i * 10 + j;
      ASSERT_OK(Put(Key(key_id), "value" + std::to_string(key_id)));
    }
    ASSERT_OK(Flush());
  }

  for (int i = 0; i < 10; i++) {
    for (int j = 0; j < 10; j++) {
      int key_id = i * 20 + j * 2;
      ASSERT_OK(Put(Key(key_id), "value_new" + std::to_string(key_id)));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  auto my_cs =
      static_cast_with_check<ExternalCompactionService>(GetCompactionService());
  uint64_t comp_num = my_cs->GetCompactionNum();
  ASSERT_GE(comp_num, 1);

  CompactionServiceJobInfo info = my_cs->GetCompactionInfoForStart();
  ASSERT_EQ(dbname_, info.db_name);
  std::string db_id, db_session_id;
  ASSERT_OK(db_->GetDbIdentity(db_id));
  ASSERT_EQ(db_id, info.db_id);
  ASSERT_OK(db_->GetDbSessionId(db_session_id));
  ASSERT_EQ(db_session_id, info.db_session_id);
  ASSERT_EQ(Env::LOW, info.priority);
  info = my_cs->GetCompactionInfoForWait();
  ASSERT_EQ(dbname_, info.db_name);
  ASSERT_EQ(db_id, info.db_id);
  ASSERT_EQ(db_session_id, info.db_session_id);
  ASSERT_EQ(Env::LOW, info.priority);

  // Test priority USER
  ColumnFamilyMetaData meta;
  db_->GetColumnFamilyMetaData(&meta);
  SstFileMetaData file = meta.levels[1].files[0];
  ASSERT_OK(db_->CompactFiles(CompactionOptions(),
                              {file.db_path + "/" + file.name}, 2));
  info = my_cs->GetCompactionInfoForStart();
  ASSERT_EQ(Env::USER, info.priority);
  info = my_cs->GetCompactionInfoForWait();
  ASSERT_EQ(Env::USER, info.priority);

  // Test priority BOTTOM
  env_->SetBackgroundThreads(1, Env::BOTTOM);
  options.num_levels = 2;
  ReopenWithCompactionService(&options);
  my_cs =
      static_cast_with_check<ExternalCompactionService>(GetCompactionService());

  for (int i = 0; i < 20; i++) {
    for (int j = 0; j < 10; j++) {
      int key_id = i * 10 + j;
      ASSERT_OK(Put(Key(key_id), "value" + std::to_string(key_id)));
    }
    ASSERT_OK(Flush());
  }

  for (int i = 0; i < 4; i++) {
    for (int j = 0; j < 10; j++) {
      int key_id = i * 20 + j * 2;
      ASSERT_OK(Put(Key(key_id), "value_new" + std::to_string(key_id)));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  info = my_cs->GetCompactionInfoForStart();
  ASSERT_EQ(Env::BOTTOM, info.priority);
  info = my_cs->GetCompactionInfoForWait();
  ASSERT_EQ(Env::BOTTOM, info.priority);
}

TEST_F(CompactionRemoteTest, FallbackLocalAuto) {
  Options options = CurrentOptions();
  ReopenWithCompactionService(&options);

  auto my_cs = GetCompactionService();
  Statistics* compactor_statistics = GetCompactorStatistics();
  Statistics* primary_statistics = GetPrimaryStatistics();
  uint64_t compactor_write_bytes =
      compactor_statistics->getTickerCount(COMPACT_WRITE_BYTES);
  uint64_t primary_write_bytes =
      primary_statistics->getTickerCount(COMPACT_WRITE_BYTES);

  my_cs->OverrideStartStatus(CompactionServiceJobStatus::kUseLocal);

  for (int i = 0; i < 20; i++) {
    for (int j = 0; j < 10; j++) {
      int key_id = i * 10 + j;
      ASSERT_OK(Put(Key(key_id), "value" + std::to_string(key_id)));
    }
    ASSERT_OK(Flush());
  }

  for (int i = 0; i < 10; i++) {
    for (int j = 0; j < 10; j++) {
      int key_id = i * 20 + j * 2;
      ASSERT_OK(Put(Key(key_id), "value_new" + std::to_string(key_id)));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // verify result
  for (int i = 0; i < 200; i++) {
    auto result = Get(Key(i));
    if (i % 2) {
      ASSERT_EQ(result, "value" + std::to_string(i));
    } else {
      ASSERT_EQ(result, "value_new" + std::to_string(i));
    }
  }

  ASSERT_EQ(my_cs->GetCompactionNum(), 0);

  // make sure the compaction statistics is only recorded on the local side
  ASSERT_EQ(compactor_statistics->getTickerCount(COMPACT_WRITE_BYTES),
            compactor_write_bytes);
  ASSERT_GT(primary_statistics->getTickerCount(COMPACT_WRITE_BYTES),
            primary_write_bytes);
  ASSERT_EQ(primary_statistics->getTickerCount(REMOTE_COMPACT_READ_BYTES), 0);
  ASSERT_EQ(primary_statistics->getTickerCount(REMOTE_COMPACT_WRITE_BYTES), 0);
}

TEST_F(CompactionRemoteTest, FallbackLocalManual) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  ReopenWithCompactionService(&options);

  GenerateTestData();
  VerifyTestData();

  auto my_cs = GetCompactionService();
  Statistics* compactor_statistics = GetCompactorStatistics();
  Statistics* primary_statistics = GetPrimaryStatistics();
  uint64_t compactor_write_bytes =
      compactor_statistics->getTickerCount(COMPACT_WRITE_BYTES);
  uint64_t primary_write_bytes =
      primary_statistics->getTickerCount(COMPACT_WRITE_BYTES);

  // re-enable remote compaction
  my_cs->ResetOverride();
  std::string start_str = Key(15);
  std::string end_str = Key(45);
  Slice start(start_str);
  Slice end(end_str);
  uint64_t comp_num = my_cs->GetCompactionNum();

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &start, &end));
  ASSERT_GE(my_cs->GetCompactionNum(), comp_num + 1);
  // make sure the compaction statistics is only recorded on the remote side
  ASSERT_GT(compactor_statistics->getTickerCount(COMPACT_WRITE_BYTES),
            compactor_write_bytes);
  ASSERT_EQ(primary_statistics->getTickerCount(REMOTE_COMPACT_WRITE_BYTES),
            compactor_statistics->getTickerCount(COMPACT_WRITE_BYTES));
  ASSERT_EQ(primary_statistics->getTickerCount(COMPACT_WRITE_BYTES),
            primary_write_bytes);

  // return run local again with API WaitForComplete
  my_cs->OverrideWaitStatus(CompactionServiceJobStatus::kUseLocal);
  start_str = Key(120);
  start = start_str;
  comp_num = my_cs->GetCompactionNum();
  compactor_write_bytes =
      compactor_statistics->getTickerCount(COMPACT_WRITE_BYTES);
  primary_write_bytes = primary_statistics->getTickerCount(COMPACT_WRITE_BYTES);

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &start, nullptr));
  ASSERT_EQ(my_cs->GetCompactionNum(),
            comp_num);  // no remote compaction is run
  // make sure the compaction statistics is only recorded on the local side
  ASSERT_EQ(compactor_statistics->getTickerCount(COMPACT_WRITE_BYTES),
            compactor_write_bytes);
  ASSERT_GT(primary_statistics->getTickerCount(COMPACT_WRITE_BYTES),
            primary_write_bytes);
  ASSERT_EQ(primary_statistics->getTickerCount(REMOTE_COMPACT_WRITE_BYTES),
            compactor_write_bytes);

  // verify result after 2 manual compactions
  VerifyTestData();
}

TEST_F(CompactionRemoteTest, RemoteEventListener) {
  class RemoteEventListenerTest : public EventListener {
   public:
    const char* Name() const override { return "RemoteEventListenerTest"; }

    void OnSubcompactionBegin(const SubcompactionJobInfo& info) override {
      auto result = on_going_compactions.emplace(info.job_id);
      ASSERT_TRUE(result.second);  // make sure there's no duplication
      compaction_num++;
      EventListener::OnSubcompactionBegin(info);
    }
    void OnSubcompactionCompleted(const SubcompactionJobInfo& info) override {
      auto num = on_going_compactions.erase(info.job_id);
      ASSERT_TRUE(num == 1);  // make sure the compaction id exists
      EventListener::OnSubcompactionCompleted(info);
    }
    void OnTableFileCreated(const TableFileCreationInfo& info) override {
      ASSERT_EQ(on_going_compactions.count(info.job_id), 1);
      file_created++;
      EventListener::OnTableFileCreated(info);
    }
    void OnTableFileCreationStarted(
        const TableFileCreationBriefInfo& info) override {
      ASSERT_EQ(on_going_compactions.count(info.job_id), 1);
      file_creation_started++;
      EventListener::OnTableFileCreationStarted(info);
    }

    bool ShouldBeNotifiedOnFileIO() override {
      file_io_notified++;
      return EventListener::ShouldBeNotifiedOnFileIO();
    }

    std::atomic_uint64_t file_io_notified{0};
    std::atomic_uint64_t file_creation_started{0};
    std::atomic_uint64_t file_created{0};

    std::set<int> on_going_compactions;  // store the job_id
    std::atomic_uint64_t compaction_num{0};
  };

  auto listener = new RemoteEventListenerTest();
  remote_listeners.emplace_back(listener);

  Options options = CurrentOptions();
  ReopenWithCompactionService(&options);

  for (int i = 0; i < 20; i++) {
    for (int j = 0; j < 10; j++) {
      int key_id = i * 10 + j;
      ASSERT_OK(Put(Key(key_id), "value" + std::to_string(key_id)));
    }
    ASSERT_OK(Flush());
  }

  for (int i = 0; i < 10; i++) {
    for (int j = 0; j < 10; j++) {
      int key_id = i * 20 + j * 2;
      ASSERT_OK(Put(Key(key_id), "value_new" + std::to_string(key_id)));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // check the events are triggered
  ASSERT_TRUE(listener->file_io_notified > 0);
  ASSERT_TRUE(listener->file_creation_started > 0);
  ASSERT_TRUE(listener->file_created > 0);
  ASSERT_TRUE(listener->compaction_num > 0);
  ASSERT_TRUE(listener->on_going_compactions.empty());

  // verify result
  for (int i = 0; i < 200; i++) {
    auto result = Get(Key(i));
    if (i % 2) {
      ASSERT_EQ(result, "value" + std::to_string(i));
    } else {
      ASSERT_EQ(result, "value_new" + std::to_string(i));
    }
  }
}

TEST_F(CompactionRemoteTest, TablePropertiesCollector) {
  const static std::string kUserPropertyName = "TestCount";

  class TablePropertiesCollectorTest : public TablePropertiesCollector {
   public:
    Status Finish(UserCollectedProperties* properties) override {
      *properties = UserCollectedProperties{
          {kUserPropertyName, std::to_string(count_)},
      };
      return Status::OK();
    }

    UserCollectedProperties GetReadableProperties() const override {
      return UserCollectedProperties();
    }

    const char* Name() const override { return "TablePropertiesCollectorTest"; }

    Status AddUserKey(const Slice& /*user_key*/, const Slice& /*value*/,
                      EntryType /*type*/, SequenceNumber /*seq*/,
                      uint64_t /*file_size*/)
    override {
      count_++;
      return Status::OK();
    }

   private:
    uint32_t count_ = 0;
  };

  class TablePropertiesCollectorFactoryTest
      : public TablePropertiesCollectorFactory {
   public:
    TablePropertiesCollector* CreateTablePropertiesCollector(
        TablePropertiesCollectorFactory::Context /*context*/) override {
      return new TablePropertiesCollectorTest();
    }

    const char* Name() const override {
      return "TablePropertiesCollectorFactoryTest";
    }
  };

  auto factory = new TablePropertiesCollectorFactoryTest();
  remote_table_properties_collector_factories.emplace_back(factory);

  const int kNumSst = 3;
  const int kLevel0Trigger = 4;
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = kLevel0Trigger;
  ReopenWithCompactionService(&options);

  // generate a few SSTs locally which should not have user property
  for (int i = 0; i < kNumSst; i++) {
    for (int j = 0; j < 100; j++) {
      ASSERT_OK(Put(Key(i * 10 + j), "value"));
    }
    ASSERT_OK(Flush());
  }

  TablePropertiesCollection fname_to_props;
  ASSERT_OK(db_->GetPropertiesOfAllTables(&fname_to_props));
  for (const auto& file_props : fname_to_props) {
    auto properties = file_props.second->user_collected_properties;
    auto it = properties.find(kUserPropertyName);
    ASSERT_EQ(it, properties.end());
  }

  // trigger compaction
  for (int i = kNumSst; i < kLevel0Trigger; i++) {
    for (int j = 0; j < 100; j++) {
      ASSERT_OK(Put(Key(i * 10 + j), "value"));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_OK(db_->GetPropertiesOfAllTables(&fname_to_props));

  bool has_user_property = false;
  for (const auto& file_props : fname_to_props) {
    auto properties = file_props.second->user_collected_properties;
    auto it = properties.find(kUserPropertyName);
    if (it != properties.end()) {
      has_user_property = true;
      ASSERT_GT(std::stoi(it->second), 0);
    }
  }
  ASSERT_TRUE(has_user_property);
}

}  // namespace ROCKSDB_NAMESPACE

static std::string Key(int i) {
  char buf[100];
  snprintf(buf, sizeof(buf), "key%06d", i);
  return std::string(buf);
}

std::string kDBPath = "/tmp/rocksdb_remote_test";

static void PrintSSTableCounts(rocksdb::DB* db) {
  std::vector<rocksdb::LiveFileMetaData> metadata;
  db->GetLiveFilesMetaData(&metadata);
  std::cout << "SSTable Counts: " << metadata.size() << std::endl;

  rocksdb::ColumnFamilyMetaData meta;
  db->GetColumnFamilyMetaData(&meta);

  std::vector<int> sstable_files_by_level;
  for (const auto& level : meta.levels) {
    sstable_files_by_level.push_back(level.files.size());
  }
  
  std::cout << "SSTable Files by Level: ";
  for (std::vector<int>::size_type i = 0; i < sstable_files_by_level.size(); i++) {
    std::cout << sstable_files_by_level[i] << " ";
  }
  std::cout << std::endl;
}

static void PrintStats(rocksdb::DB* db) {
  std::string stats;
  db->GetProperty("rocksdb.stats", &stats);
  std::cout << "Stats: " << stats << std::endl;
}

int main(int argc, char** argv) {
  std::cout << "Remote Compaction Demo." << std::endl;

  // Create DB
  std::cout << "Creating db at " << kDBPath << std::endl;

  rocksdb::DB* db;

  rocksdb::Env* env = rocksdb::Env::Default();
  rocksdb::Options primary_options;  // for client
  {
    rocksdb::Options options;
    options.env = env;
    options.create_if_missing = true;
    options.fail_if_options_file_error = true;
    options.disable_auto_compactions = true; // do not compact on client
    // options.compaction_service will be set later
    primary_options = options;
  }
  std::shared_ptr<rocksdb::Statistics> primary_statistics;
  primary_statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();

  primary_options.statistics = primary_statistics;

  // Create External Compaction Service

  std::cout << "Creating compaction service" << std::endl;

  rocksdb::Options compactor_options;
  {
    rocksdb::Options options;
    options.env = env;
    options.create_if_missing = false; // secondary
    options.fail_if_options_file_error = true;
    compactor_options = options;
  }

  std::shared_ptr<rocksdb::ExternalCompactionService> compaction_service;
  rocksdb::ExternalCompactionService* cs;
  {
    std::string db_path = kDBPath;
    std::shared_ptr<rocksdb::Statistics> compactor_statistics =
        ROCKSDB_NAMESPACE::CreateDBStatistics();
    std::vector<std::shared_ptr<rocksdb::EventListener>> listeners;
    std::vector<std::shared_ptr<rocksdb::TablePropertiesCollectorFactory>>
        table_properties_collector_factories;

    compaction_service =
        std::make_shared<ROCKSDB_NAMESPACE::ExternalCompactionService>(
            db_path, compactor_options, compactor_statistics, listeners,
            table_properties_collector_factories);
    cs = compaction_service.get();
  }

  primary_options.compaction_service = compaction_service;

  assert(cs->GetCompactionNum() == 0);

  // Write some values

  rocksdb::Status s = rocksdb::DB::Open(primary_options, kDBPath, &db);
  assert(s.ok());

  // TEST: Write then read a key-value pair
  std::cout << "TEST: write-read one value" << std::endl;

  s = db->Put(rocksdb::WriteOptions(), "key1", "value1");
  assert(s.ok());
  {
    std::string value;
    s = db->Get(rocksdb::ReadOptions(), "key1", &value);
    assert(s.ok());

    assert(value == "value1");
  }

  // TEST: Time to write lots of values

  std::cout << "TEST: insert many values x10" << std::endl;

  const int BATCH_SIZE = 100000;
  const int BATCHES = 2;
  for (int n = 0; n < BATCHES; n++) {
    auto start = std::chrono::high_resolution_clock::now();

    for (int i = BATCH_SIZE * (n - 1); i < BATCH_SIZE * (n); i++) {
      std::string key = "key" + std::to_string(i);
      std::string value = "value" + std::to_string(i);
      s = db->Put(rocksdb::WriteOptions(), key, value);
      assert(s.ok());
    }

    s = db->Flush(rocksdb::FlushOptions());
    assert(s.ok());

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> diff = end - start;
    std::cout << "Time: " << diff.count() << " s" << std::endl;

    PrintSSTableCounts(db);
  }

  // SETUP: insert some test values to look for later
  std::cout << "Inserting some more values" << std::endl;

  for (int i = 0; i < 20; i++) {
    for (int j = 0; j < 10; j++) {
      int key_id = i * 10 + j;
      s = db->Put(rocksdb::WriteOptions(), Key(key_id),
                  "value" + std::to_string(key_id));
      assert(s.ok());
    }
    s = db->Flush(rocksdb::FlushOptions());
    assert(s.ok());
  }

  for (int i = 0; i < 10; i++) {
    for (int j = 0; j < 10; j++) {
      int key_id = i * 20 + j * 2;
      s = db->Put(rocksdb::WriteOptions(), Key(key_id),
                  "value_new" + std::to_string(key_id));
      assert(s.ok());
    }
    s = db->Flush(rocksdb::FlushOptions());
    assert(s.ok());
  }
  PrintSSTableCounts(db);

  std::cout << "Verifying values are correct" << std::endl;

  for (int i = 0; i < 200; i++) {
    std::string result;
    db->Get(rocksdb::ReadOptions(), Key(i), &result);
    if (i % 2) {
      assert(result == "value" + std::to_string(i));
    } else {
      assert(result == "value_new" + std::to_string(i));
    }
  }

  // Perform compaction

  auto num = cs->GetCompactionNum();

  std::cout << "Performing compaction " << num << std::endl;

  rocksdb::CompactRangeOptions coptions;
  db->CompactRange(coptions, nullptr,
                    nullptr);  // compact whole database, b/c nullptr is both
                              // first and last record

  num = cs->GetCompactionNum();

  std::cout << "Compaction complete, now count is " << num << std::endl;

  // Verify values are correct

  std::cout << "Verifying values are correct" << std::endl;

  for (int i = 0; i < 200; i++) {
    std::string result;
    db->Get(rocksdb::ReadOptions(), Key(i), &result);
    if (i % 2) {
      assert(result == "value" + std::to_string(i));
    } else {
      assert(result == "value_new" + std::to_string(i));
    }
  }

  // Statistics

  PrintSSTableCounts(db);


  std::cout << "Done with main" << std::endl;

  // clean up db in temp folder
  delete db;
  rocksdb::DestroyDB(kDBPath, primary_options);

  // ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  //::testing::InitGoogleTest(&argc, argv);
  // RegisterCustomObjects(argc, argv);
  // return RUN_ALL_TESTS();
  return 0;
}
