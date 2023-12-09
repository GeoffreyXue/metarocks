#include <iostream>

#include "rocksdb/compaction_service.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"

using ROCKSDB_NAMESPACE::ColumnFamilyMetaData;
using ROCKSDB_NAMESPACE::CompactRangeOptions;
using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::Env;
using ROCKSDB_NAMESPACE::EventListener;
using ROCKSDB_NAMESPACE::ExternalCompactionService;
using ROCKSDB_NAMESPACE::FlushOptions;
using ROCKSDB_NAMESPACE::LiveFileMetaData;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Statistics;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::TablePropertiesCollectorFactory;
using ROCKSDB_NAMESPACE::WriteOptions;

using namespace std;

static string Key(int i) {
  char buf[100];
  snprintf(buf, sizeof(buf), "key%06d", i);
  return string(buf);
}

string kDBPath = "/tmp/rocksdb_remote_test";

static void PrintStats(DB* db) {
  string stats;
  db->GetProperty("rocksdb.stats", &stats);
  cout << "Stats: " << stats << endl;
}

static void PrintSSTableCounts(DB* db) {
  vector<LiveFileMetaData> metadata;
  db->GetLiveFilesMetaData(&metadata);
  cout << "SSTable Counts: " << metadata.size() << endl;

  ColumnFamilyMetaData meta;
  db->GetColumnFamilyMetaData(&meta);

  vector<std::vector<rocksdb::SstFileMetaData>::size_type>
      sstable_files_by_level;
  for (const auto& level : meta.levels) {
    sstable_files_by_level.push_back(level.files.size());
  }

  cout << "SSTable Files by Level: ";
  for (vector<int>::size_type i = 0; i < sstable_files_by_level.size(); i++) {
    cout << sstable_files_by_level[i] << " ";
  }
  cout << endl;
}

int main(int argc, char** argv) {
  cout << "Remote Compaction Demo." << endl;

  // Create DB
  cout << "Creating db at " << kDBPath << endl;

  DB* db;

  Env* env = Env::Default();
  Options primary_options;  // for client
  {
    Options options;
    options.env = env;
    options.create_if_missing = true;
    options.fail_if_options_file_error = true;
    options.disable_auto_compactions = true;  // do not compact on client
    // options.compaction_service will be set later
    primary_options = options;
  }
  shared_ptr<Statistics> primary_statistics;
  primary_statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();

  primary_options.statistics = primary_statistics;

  // Create External Compaction Service

  cout << "Creating compaction service" << endl;

  Options compactor_options;
  {
    Options options;
    options.env = env;
    options.create_if_missing = false;  // secondary
    options.fail_if_options_file_error = true;
    compactor_options = options;
  }

  shared_ptr<ExternalCompactionService> compaction_service;
  ExternalCompactionService* cs;
  {
    string db_path = kDBPath;
    shared_ptr<Statistics> compactor_statistics =
        ROCKSDB_NAMESPACE::CreateDBStatistics();
    vector<shared_ptr<EventListener>> listeners;
    vector<shared_ptr<TablePropertiesCollectorFactory>>
        table_properties_collector_factories;

    compaction_service =
        make_shared<ROCKSDB_NAMESPACE::ExternalCompactionService>(
            db_path, compactor_options, compactor_statistics, listeners,
            table_properties_collector_factories);
    cs = compaction_service.get();
  }

  primary_options.compaction_service = compaction_service;

  assert(cs->GetCompactionNum() == 0);

  // Write some values

  Status s = DB::Open(primary_options, kDBPath, &db);
  assert(s.ok());

  // TEST: Write then read a key-value pair
  cout << "TEST: write-read one value" << endl;

  s = db->Put(WriteOptions(), "key1", "value1");
  assert(s.ok());
  {
    string value;
    s = db->Get(ReadOptions(), "key1", &value);
    assert(s.ok());

    assert(value == "value1");
  }

  PrintSSTableCounts(db);

  // TEST: Time to write lots of values

  cout << "TEST: insert many values x10" << endl;

  const int BATCH_SIZE = 100000;
  const int BATCHES = 5;
  for (int n = 0; n < BATCHES; n++) {
    auto start = chrono::high_resolution_clock::now();

    for (int i = BATCH_SIZE * (n - 1); i < BATCH_SIZE * (n); i++) {
      string key = "key" + to_string(i);
      string value = "value" + to_string(i);
      s = db->Put(WriteOptions(), key, value);
      assert(s.ok());
    }

    s = db->Flush(FlushOptions());
    assert(s.ok());

    auto end = chrono::high_resolution_clock::now();
    chrono::duration<double> diff = end - start;
    cout << "Time: " << diff.count() << " s" << endl;

    PrintSSTableCounts(db);
  }

  // SETUP: insert some test values to look for later
  cout << "Inserting some more values" << endl;

  for (int i = 0; i < 20; i++) {
    for (int j = 0; j < 10; j++) {
      int key_id = i * 10 + j;
      s = db->Put(WriteOptions(), Key(key_id), "value" + to_string(key_id));
      assert(s.ok());
    }
    s = db->Flush(FlushOptions());
    assert(s.ok());
  }

  for (int i = 0; i < 10; i++) {
    for (int j = 0; j < 10; j++) {
      int key_id = i * 20 + j * 2;
      s = db->Put(WriteOptions(), Key(key_id), "value_new" + to_string(key_id));
      assert(s.ok());
    }
    s = db->Flush(FlushOptions());
    assert(s.ok());
  }

  cout << "Verifying values are correct" << endl;

  for (int i = 0; i < 200; i++) {
    string result;
    db->Get(ReadOptions(), Key(i), &result);
    if (i % 2) {
      assert(result == "value" + to_string(i));
    } else {
      assert(result == "value_new" + to_string(i));
    }
  }

  // Perform compaction

  PrintSSTableCounts(db);

  auto num = cs->GetCompactionNum();

  cout << "Performing compaction " << num << endl;

  CompactRangeOptions coptions;
  db->CompactRange(coptions, nullptr,
                   nullptr);  // compact whole database, b/c nullptr is both
                              // first and last record

  num = cs->GetCompactionNum();

  cout << "Compaction complete, now count is " << num << endl;

  PrintSSTableCounts(db);

  // Verify values are correct

  cout << "Verifying values are correct" << endl;

  for (int i = 0; i < 200; i++) {
    string result;
    db->Get(ReadOptions(), Key(i), &result);
    if (i % 2) {
      assert(result == "value" + to_string(i));
    } else {
      assert(result == "value_new" + to_string(i));
    }
  }

  // Statistics
  // PrintStats(db)

  cout << "Done with main" << endl;

  // clean up db in temp folder
  delete db;
  rocksdb::DestroyDB(kDBPath, primary_options);

  return 0;
}
