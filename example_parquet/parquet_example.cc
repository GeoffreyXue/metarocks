#include <string>

#include "rocksdb/db.h"
#include "rocksdb/options.h"

using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::Status;

#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\rocksdb_parquet_example";
#else
std::string kDBPath = "./rocksdb_parquet_example";
#endif

int main() {
  Options options;
  options.create_if_missing = true;

  // https://betterprogramming.pub/navigating-the-minefield-of-rocksdb-configuration-options-246af1e1d3f9

  // Don't slow down writes, but have much smaller stop trigger
  // options.level0_slowdown_writes_trigger = 3;
  options.level0_stop_writes_trigger = 3;

  // Restrict to just one thread for compaction
  options.IncreaseParallelism(1);
  

  // Memtable Settings
  // Try to minimize memory caching as much as possible for this test
  // So we get to compaction faster
  // Setting to 1MB
  options.write_buffer_size = 1024 * 1024;

  DB* db = nullptr;
  ROCKSDB_NAMESPACE::DestroyDB(kDBPath, options);
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());
  assert(db);


  for (int i = 1; i < 10000; ++i) {
    std::string value = "{\"hello\":" + std::to_string(i) + ", \"world\":\"" + std::string(500, 'a' + (i % 26)) + "\"}";
    db->Put(rocksdb::WriteOptions(), std::to_string(i), value);
  }

  // verify the values are still there
  std::string value;
  for (int i = 1; i < 10000; ++i) {
    db->Get(rocksdb::ReadOptions(), std::to_string(i), &value);
    std::string val(500, 'a' + (i % 26));
    std::string original = "{\"hello\":" + std::to_string(i) + ", \"world\": " + std::string(500, 'a' + (i % 26)) + "}";
    assert(value == original);
  }

  // close the db.
  delete db;

  return 0;
}
