#include "rocksdb/compaction_service.h"

#include "db/compaction/compaction_job.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"

#include <iostream>

#include <aws/core/Aws.h>
#include <aws/core/utils/Array.h>
#include <aws/core/utils/base64/Base64.h>
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

namespace ROCKSDB_NAMESPACE {

string kDBPath = "/home/ubuntu/s3fuse/conn.db";
string kDBCompactionOutputPath = "/home/ubuntu/s3fuse/conn.db/output";
string kCompactionRequestQueueUrl = "https://sqs.us-east-2.amazonaws.com/848490464384/request.fifo";
string kCompactionResponseQueueUrl ="https://sqs.us-east-2.amazonaws.com/848490464384/response.fifo";

string waitForResponse(const string &queueUrl) {
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
    receive_request.SetMaxNumberOfMessages(
        1); // Max number of messages to receive
    receive_request.SetVisibilityTimeout(30); // Visibility timeout
    receive_request.SetWaitTimeSeconds(20);   // Long polling wait time

    // Receive the message
    auto receive_outcome = sqs.ReceiveMessage(receive_request);

    if (receive_outcome.IsSuccess()) {
      const auto &messages = receive_outcome.GetResult().GetMessages();
      if (!messages.empty()) {
        std::cout << "Successfully received non-empty message" << std::endl;
        for (const auto &message : messages) {
          result = message.GetBody();

          // After processing, delete the message from the queue
          Aws::SQS::Model::DeleteMessageRequest delete_request;
          delete_request.SetQueueUrl(queueUrl);
          delete_request.SetReceiptHandle(message.GetReceiptHandle());
          auto delete_outcome = sqs.DeleteMessage(delete_request);
          if (!delete_outcome.IsSuccess()) {
            std::cerr << "Error deleting message: "
                      << delete_outcome.GetError().GetMessage() << endl;
          }
        }
      } else {
        cout << "No messages to process." << endl;
      }
    } else {
      std::cerr << "Error receiving messages: "
                << receive_outcome.GetError().GetMessage() << endl;
    }
  }
  Aws::ShutdownAPI(options);
  return result;
}

void sendMessage(const string &message, const string &queueUrl) {
  Aws::SDKOptions options;
  Aws::InitAPI(options);
  {
    auto now = std::chrono::high_resolution_clock::now();

    // Convert the time point to a duration since the epoch
    auto duration_since_epoch = now.time_since_epoch();

    // Convert the duration to a specific unit (e.g., nanoseconds)
    auto nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(
                           duration_since_epoch)
                           .count();

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
        std::cout << "Successfully sent message out" << std::endl;
        return;
      } else {
        std::cerr << "Error sending message: " << sm_out.GetError().GetMessage()
                  << endl;
      }
    }
  }
  Aws::ShutdownAPI(options);
}

CompactionServiceJobStatus ExternalCompactionService::StartV2(
    const CompactionServiceJobInfo& info,
    const std::string& compaction_service_input) {
  std::scoped_lock lock(mutex_);

  // Add to queue
  start_info_ = info;
  assert(info.db_name == db_path_);

  // Send message to queue
  Aws::Utils::ByteBuffer messageBuffer = Aws::Utils::ByteBuffer(
      reinterpret_cast<const unsigned char *>(compaction_service_input.c_str()),
      compaction_service_input.length());
  Aws::String base64Message = Aws::Utils::HashingUtils::Base64Encode(messageBuffer);

  sendMessage(base64Message, kCompactionRequestQueueUrl);

  jobs_.emplace(info.job_id, compaction_service_input);

  // PRint dbug
  std::cout << "StartV2: Sending job " << info.job_id << std::endl;

  // Decide if queue add successful
  CompactionServiceJobStatus s = CompactionServiceJobStatus::kSuccess;
  if (is_override_start_status_) {
    return override_start_status_; // if testing failure, inject failure
  }

  return s;
}

void OpenAndCompactInThread(
    const OpenAndCompactOptions& options, const std::string& name,
    const std::string& output_directory, const std::string& input,
    std::string* output,
    const CompactionServiceOptionsOverride& override_options, Status* s) {
  *s = DB::OpenAndCompact(
      name, output_directory,
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

  string message = "";
  while (message.empty()) {
    message = waitForResponse(kCompactionResponseQueueUrl);
  }

  // can we parse the result? CompactionServiceResult :: Read or whatever
  CompactionServiceResult result;
  Status s = CompactionServiceResult::Read(message, &result);

  // result is good
  *compaction_service_result = message;

  std::cout << "WaitForCompleteV2: Received job " << info.job_id << std::endl;

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