// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <chrono>
#include <ctime>
#include <functional>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <thread>

#include "kudu/client/callbacks.h"
#include "kudu/client/client.h"
#include "kudu/client/row_result.h"
#include "kudu/client/stubs.h"
#include "kudu/client/value.h"
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/util/monotime.h"

using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduDelete;
using kudu::client::KuduScanBatch;
using kudu::client::KuduScanner;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduTableAlterer;
using kudu::client::KuduTableCreator;
using kudu::client::KuduUpdate;
using kudu::client::KuduUpsert;
using kudu::client::KuduValue;
using kudu::client::sp::shared_ptr;
using kudu::KuduPartialRow;
using kudu::MonoDelta;
using kudu::MonoTime;
using kudu::Status;

using std::function;
using std::map;
using std::ostringstream;
using std::string;
using std::unique_ptr;
using std::vector;

static Status CreateClient(const vector<string>& master_addrs,
                           shared_ptr<KuduClient>* client) {
  return KuduClientBuilder()
      .master_server_addrs(master_addrs)
      .default_admin_operation_timeout(MonoDelta::FromSeconds(20))
      .Build(client);
}

static KuduSchema CreateSchema() {
  KuduSchemaBuilder b;
  b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
  b.AddColumn("int_val")->Type(KuduColumnSchema::INT32)->NotNull();
  b.AddColumn("non_null_with_default")->
      Type(KuduColumnSchema::INT32)->
      NotNull()->
      Default(KuduValue::FromInt(12345));

  KuduSchema schema;
  KUDU_CHECK_OK(b.Build(&schema));
  return schema;
}

static Status DoesTableExist(const shared_ptr<KuduClient>& client,
                             const string& table_name,
                             bool *exists) {
  shared_ptr<KuduTable> table;
  Status s = client->OpenTable(table_name, &table);
  if (s.ok()) {
    *exists = true;
  } else if (s.IsNotFound()) {
    *exists = false;
    s = Status::OK();
  }
  return s;
}

static Status CreateTable(const shared_ptr<KuduClient>& client,
                          const string& table_name,
                          const KuduSchema& schema,
                          int num_tablets) {
  vector<string> column_names;
  column_names.push_back("key");

  // Set the schema and range partition columns.
  KuduTableCreator* table_creator = client->NewTableCreator();
  table_creator->table_name(table_name)
      .schema(&schema)
      .set_range_partition_columns(column_names);

  // Generate and add the range partition splits for the table.
  int32_t increment = 1000 / num_tablets;
  for (int32_t i = 1; i < num_tablets; i++) {
    KuduPartialRow* row = schema.NewRow();
    KUDU_CHECK_OK(row->SetInt32(0, i * increment));
    table_creator->add_range_partition_split(row);
  }

  Status s = table_creator->Create();
  delete table_creator;
  return s;
}

// A helper class providing custom logging callback. It also manages
// automatic callback installation and removal.
class LogCallbackHelper {
 public:
  LogCallbackHelper() : log_cb_(&LogCallbackHelper::LogCb, NULL) {
    kudu::client::InstallLoggingCallback(&log_cb_);
  }

  ~LogCallbackHelper() {
    kudu::client::UninstallLoggingCallback();
  }

  static void LogCb(void* unused,
                    kudu::client::KuduLogSeverity severity,
                    const char* filename,
                    int line_number,
                    const struct ::tm* time,
                    const char* message,
                    size_t message_len) {
    char time_buf[32];
    // Example: Tue Mar 24 11:46:43 2015.
    KUDU_CHECK(strftime(time_buf, sizeof(time_buf), "%a %b %d %T %Y", time));

    KUDU_LOG_INTERNAL(severity) << filename << ":" << line_number << " "
                                << time_buf << "\n" << string(message, message_len);
  }

 private:
  kudu::client::KuduLoggingFunctionCallback<void*> log_cb_;
};

void openDiffScan(KuduScanner& scanner, uint64_t start_ts, uint64_t end_ts) {
  KUDU_CHECK_OK(scanner.SetSelection(KuduClient::ReplicaSelection::LEADER_ONLY));
  // KUDU_CHECK_OK(scanner.SetDiffScan(start_ts, end_ts));
  KUDU_CHECK_OK(scanner.Open());
}

// Format a raw Kudu HybridClock timestamp for human-readable log output.
//
// HybridClock encoding: physical microseconds since Unix epoch are stored in
// the upper 52 bits (i.e. ts >> 12), and a 12-bit logical counter occupies
// the lower bits.  We extract the physical part, convert it to a UTC wall-
// clock string (YYYY-MM-DD HH:MM:SS.ffffff UTC), and append the logical value
// so the full raw value is reproducible from the log.
static string HybridTimestampToString(uint64_t ts) {
  uint64_t micros = ts >> 12;
  uint32_t logical = static_cast<uint32_t>(ts & 0xFFF);

  time_t secs = static_cast<time_t>(micros / 1000000);
  uint32_t frac_us = static_cast<uint32_t>(micros % 1000000);

  struct tm tm_utc;
  gmtime_r(&secs, &tm_utc);
  char buf[32];
  strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm_utc);

  ostringstream oss;
  oss << buf
      << "." << std::setfill('0') << std::setw(6) << frac_us
      << " UTC (logical=" << logical << ", raw=" << ts << ")";
  return oss.str();
}

static int CountTableRows(const shared_ptr<KuduTable>& table) {
  KuduScanner scanner(table.get());
  KUDU_CHECK_OK(scanner.SetSelection(KuduClient::ReplicaSelection::LEADER_ONLY));
  KUDU_CHECK_OK(scanner.Open());
  KuduScanBatch batch;
  int count = 0;
  while (scanner.HasMoreRows()) {
    KUDU_CHECK_OK(scanner.NextBatch(&batch));
    count += batch.NumRows();
  }
  return count;
}

int main(int argc, char* argv[]) {
  KUDU_LOG(INFO) << "Running with Kudu client version: " <<
      kudu::client::GetShortVersionString();

  LogCallbackHelper log_cb_helper;

  if (argc < 2) {
    KUDU_LOG(FATAL) << "usage: " << argv[0] << " <master host> ...";
  }
  vector<string> master_addrs;
  for (int i = 1; i < argc; i++) {
    master_addrs.push_back(argv[i]);
  }

  const string kTableName = "db.test_table";

  // Connect to the cluster.
  shared_ptr<KuduClient> client;
  KUDU_CHECK_OK(CreateClient(master_addrs, &client));
  KUDU_LOG(INFO) << "Connected to cluster";

  // Step 1: Create the table, capture the server's HybridClock timestamp via
  //         a lightweight scan of the freshly created (empty) table, and set
  //         that timestamp as the table's migration_timestamp.
  //         Any row inserted *after* this timestamp will have a higher
  //         HybridClock timestamp and will NOT be considered migrated yet —
  //         so scans will return those rows normally.

  // Open the table handle here so we can use it for the timestamp seed scan
  // and for all subsequent read/write operations.
  KuduSchema schema(CreateSchema());
  bool exists;
  KUDU_CHECK_OK(DoesTableExist(client, kTableName, &exists));
  if (exists) {
    KUDU_CHECK_OK(client->DeleteTable(kTableName));
    KUDU_LOG(INFO) << "Dropped pre-existing table";
  }
  KUDU_CHECK_OK(CreateTable(client, kTableName, schema, 10));

  shared_ptr<KuduTable> table;
  KUDU_CHECK_OK(client->OpenTable(kTableName, &table));

  // Seed the client's observed timestamp from the server.
  //
  // client->GetLatestObservedTimestamp() returns 0 (kNoTimestamp) until the
  // client receives at least one data-plane RPC response that carries a
  // propagated_timestamp.  DDL calls (CreateTable, AlterTable) go to the
  // master, whose responses never carry a HybridClock timestamp, so they do
  // not help here.
  //
  // A scan — even on an empty table — sends a ScanRpc to the tablet server.
  // The tablet server always includes propagated_timestamp in its response
  // (see scanner-internal.cc: UpdateLatestObservedTimestamp is called inside
  // OpenTablet() right after SendScanRpc returns).  After this call
  // GetLatestObservedTimestamp() will hold a real server-side timestamp.
  CountTableRows(table);  // returns 0; side-effect: seeds observed timestamp
  uint64_t creation_ts = client->GetLatestObservedTimestamp();
  KUDU_LOG(INFO) << "Step 1: Created table, server creation_ts="
                 << HybridTimestampToString(creation_ts);

  {
    // Set migration_timestamp = creation_ts on the table.
    map<string, string> extra_configs;
    extra_configs["kudu.table.migration_timestamp"] = std::to_string(creation_ts);
    unique_ptr<KuduTableAlterer> alterer(client->NewTableAlterer(kTableName));
    KUDU_CHECK_OK(alterer->AlterExtraConfig(extra_configs)->Alter());
    KUDU_LOG(INFO) << "Step 1: Set migration_timestamp="
                   << HybridTimestampToString(creation_ts);
  }

  // Step 2: Insert one row (key=1, int_val=42).
  //         This write happens *after* creation_ts, so the row's HybridClock
  //         timestamp will be > migration_timestamp — it is not yet migrated.
  {
    shared_ptr<KuduSession> session = client->NewSession();
    KuduUpsert* upsert = table->NewUpsert();
    KuduPartialRow* row = upsert->mutable_row();
    KUDU_CHECK_OK(row->SetInt32("key", 1));
    KUDU_CHECK_OK(row->SetInt32("int_val", 42));
    KUDU_CHECK_OK(row->SetInt32("non_null_with_default", 100));
    KUDU_CHECK_OK(session->Apply(upsert));
    KUDU_CHECK_OK(session->Flush());
    // GetLatestObservedTimestamp() is updated by the batcher from the write
    // RPC response timestamp, so this reflects the server-assigned insert time.
    uint64_t insert_ts = client->GetLatestObservedTimestamp();
    KUDU_CHECK_OK(session->Close());
    KUDU_LOG(INFO) << "Step 2: Inserted row (key=1, int_val=42), insert_ts="
                   << HybridTimestampToString(insert_ts);
  }

  // Step 3: Wait 10 seconds to ensure the row is flushed to disk.
  {
    const int kFlushSec = 10;
    KUDU_LOG(INFO) << "Step 3: Sleeping " << kFlushSec
                   << "s to ensure the row is flushed to disk...";
    std::this_thread::sleep_for(std::chrono::seconds(kFlushSec));
  }

  // Step 4: Scan and verify the inserted row is returned.
  //         migration_timestamp is still set to creation_ts (before the
  //         insert), so the row is not yet eligible for GC.
  {
    KUDU_LOG(INFO) << "Step 4: Scanning table (expecting 1 row with int_val=42)";
    KuduScanner scanner(table.get());
    KUDU_CHECK_OK(scanner.SetSelection(KuduClient::ReplicaSelection::LEADER_ONLY));
    KUDU_CHECK_OK(scanner.Open());
    KuduScanBatch batch;
    int row_count = 0;
    while (scanner.HasMoreRows()) {
      KUDU_CHECK_OK(scanner.NextBatch(&batch));
      row_count += batch.NumRows();
      for (KuduScanBatch::const_iterator it = batch.begin(); it != batch.end(); ++it) {
        KuduScanBatch::RowPtr row(*it);
        KUDU_LOG(INFO) << "  Row: " << row->ToString();
      }
    }
    KUDU_LOG(INFO) << "Step 4: Row count = " << row_count;
  }

  // Step 5: Advance migration_timestamp to "now" — a timestamp captured
  //         *after* the row was written and flushed.  From this point on,
  //         the row's timestamp is < migration_timestamp, so it is fully
  //         migrated and eligible for deletion by the GC.
  {
    uint64_t now_ts = client->GetLatestObservedTimestamp();
    KUDU_LOG(INFO) << "Step 5: Setting migration_timestamp="
                   << HybridTimestampToString(now_ts)
                   << " (row timestamp is now below the migration mark)";
    map<string, string> extra_configs;
    extra_configs["kudu.table.migration_timestamp"] = std::to_string(now_ts);
    unique_ptr<KuduTableAlterer> alterer(client->NewTableAlterer(kTableName));
    KUDU_CHECK_OK(alterer->AlterExtraConfig(extra_configs)->Alter());
  }

  // Step 6: Wait 1 second to allow the GC to process the migrated rowset.
  {
    KUDU_LOG(INFO) << "Step 6: Sleeping 1s to allow GC to run...";
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  // Step 7: Scan and verify that no row is returned — the GC has deleted the
  //         rowset because its latest mutation is older than migration_timestamp.
  {
    KUDU_LOG(INFO) << "Step 7: Scanning table after GC (expecting 0 rows)";
    KuduScanner scanner(table.get());
    KUDU_CHECK_OK(scanner.SetSelection(KuduClient::ReplicaSelection::LEADER_ONLY));
    KUDU_CHECK_OK(scanner.Open());
    KuduScanBatch batch;
    int row_count = 0;
    while (scanner.HasMoreRows()) {
      KUDU_CHECK_OK(scanner.NextBatch(&batch));
      row_count += batch.NumRows();
    }
    KUDU_LOG(INFO) << "Step 7: Row count = " << row_count;
  }

  KUDU_LOG(INFO) << "Done";
  return 0;
}
