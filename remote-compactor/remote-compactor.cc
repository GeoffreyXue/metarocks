#include <iostream>

#include "db/compaction/compaction.h"
#include "db/compaction/compaction_job.h"
#include "db/compaction/compaction_service.h"
#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "table/unique_id_impl.h"

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
