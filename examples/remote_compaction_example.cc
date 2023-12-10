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


#include <iostream>
#include <string>
#include <aws/core/Aws.h>
#include <aws/core/utils/crypto/Sha256.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/core/utils/Outcome.h>
#include <aws/sqs/SQSClient.h>
#include <aws/sqs/model/ReceiveMessageRequest.h>
#include <aws/sqs/model/SendMessageRequest.h>
#include <aws/sqs/model/DeleteMessageRequest.h>


using std::string;
using std::cout;
using std::endl;

string waitForResponse(const string& queueUrl) {
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    string result = "";
    {
        Aws::Client::ClientConfiguration clientConfig;
        clientConfig.region = Aws::Region::US_EAST_2; // Set the region to Ohio

        Aws::SQS::SQSClient sqs(clientConfig);
  
        // Create a receive message request
        Aws::SQS::Model::ReceiveMessageRequest receive_request;
        receive_request.SetQueueUrl(queueUrl);
        receive_request.SetMaxNumberOfMessages(1); // Max number of messages to receive
        receive_request.SetVisibilityTimeout(30); // Visibility timeout
        receive_request.SetWaitTimeSeconds(20); // Long polling wait time

        // Receive the message
        auto receive_outcome = sqs.ReceiveMessage(receive_request);

        if (receive_outcome.IsSuccess()) {
            const auto &messages = receive_outcome.GetResult().GetMessages();
            if (!messages.empty()) {
                for (const auto &message: messages) {
                    result = message.GetBody();

                    // After processing, delete the message from the queue
                    Aws::SQS::Model::DeleteMessageRequest delete_request;
                    delete_request.SetQueueUrl(queueUrl);
                    delete_request.SetReceiptHandle(message.GetReceiptHandle());
                    auto delete_outcome = sqs.DeleteMessage(delete_request);
                    if (!delete_outcome.IsSuccess()) {
                        std::cerr << "Error deleting message: " << delete_outcome.GetError().GetMessage() << endl;
                    }
                }
            } else {
                cout << "No messages to process." << endl;
            }
        } else {
            std::cerr << "Error receiving messages: " << receive_outcome.GetError().GetMessage() << endl;
        }
    }
    Aws::ShutdownAPI(options);
    return result;
}

void sendMessage(const string& message,const string& queueUrl) {
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    {
        auto now = std::chrono::high_resolution_clock::now();

        // Convert the time point to a duration since the epoch
        auto duration_since_epoch = now.time_since_epoch();

        // Convert the duration to a specific unit (e.g., nanoseconds)
        auto nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(duration_since_epoch).count();

        std::stringstream ss;
        ss << nanoseconds;
        string nanoStr = ss.str();

        // Hash the input string
        Aws::Utils::Crypto::Sha256 sha256;
        auto hashBytes = sha256.Calculate(message + nanoStr);
        auto hash = Aws::Utils::HashingUtils::HexEncode(hashBytes.GetResult());

        Aws::Client::ClientConfiguration clientConfig;
        clientConfig.region = Aws::Region::US_EAST_2; // Set the region to Ohio

        Aws::SQS::SQSClient sqs(clientConfig);

        while (1) {
            Aws::SQS::Model::SendMessageRequest smReq;
            smReq.SetQueueUrl(queueUrl);
            smReq.SetMessageGroupId("group");
            smReq.SetMessageDeduplicationId(hash);
            smReq.SetMessageBody(message);

            auto sm_out = sqs.SendMessage(smReq);
            if (sm_out.IsSuccess()) {
                return;
            } else {
                std::cerr << "Error sending message: " << sm_out.GetError().GetMessage() << endl;
            }
        }
    }
    Aws::ShutdownAPI(options);

}

int aws_main() {
    sendMessage("the message ggg", "https://sqs.us-east-2.amazonaws.com/848490464384/request.fifo");
    string msg = waitForResponse("https://sqs.us-east-2.amazonaws.com/848490464384/request.fifo");
    cout << "got back:" << msg << endl;
    return 0;
}

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

void PrintCompactionCount(ExternalCompactionService* cs) {
  cout << "Compaction Count: " << cs->GetCompactionNum() << endl;
}

void PrintStatus(DB* db, ExternalCompactionService* cs) {
  PrintSSTableCounts(db);
  PrintCompactionCount(cs);
}

int main(int argc, char** argv) {
  aws_main();

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


  Status s = DB::Open(primary_options, kDBPath, &db);
  assert(s.ok());

  PrintStatus(db, cs);


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

    PrintStatus(db, cs);
  }

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
  
  PrintStatus(db, cs);

  cout << "Manual compaction " << cs->GetCompactionNum() << endl;

  CompactRangeOptions coptions;
  db->CompactRange(coptions, nullptr,
                   nullptr);  // compact whole database, b/c nullptr is both
                              // first and last record

  cout << "Compaction complete, now count is " << cs->GetCompactionNum() << endl;

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
