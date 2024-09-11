// Copyright 2023 Ant Group Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "psi/apsi/pir.h"

#include <filesystem>
#include <fstream>
#include <string>
#include <utility>
#include <vector>
#include <chrono>

#include "gtest/gtest.h"
#include "spdlog/spdlog.h"
#include "yacl/crypto/rand/rand.h"
#include "yacl/crypto/tools/prg.h"
#include "yacl/link/test_util.h"
#include "yacl/utils/scope_guard.h"

#include "psi/apsi/psi_params.h"
#include "psi/utils/arrow_csv_batch_provider.h"
#include "psi/utils/io.h"

namespace {

constexpr size_t kPsiStartPos = 2;

struct TestParams {
  size_t nr;
  size_t ns;
  size_t label_bytes = 16;
  bool use_filedb = true;
  bool compressed = false;
  size_t bucket_size = 10000;
};

//读csv文件
bool ReadCsvFile(const std::string &csv_file) {
  try {
    std::cout << "csv_file:" << csv_file << std::endl;
    std::ifstream f;
    f.open(csv_file, std::fstream::in);
    if (!f.is_open()) {
      std::cout << "ReadCsvFile is false! csv_file = " << csv_file << std::endl;
      return false;
    }
    std::string line;
    while (getline(f, line)) {
      std::cout << line << std::endl;
      // std::vector<std::string> data;
      // boost::split(data, line, boost::is_any_of(","), boost::token_compress_on);
      // output.push_back(data);
    }
    f.close();
  } catch (std::exception &e) {
    std::cout << "ReadCsvFile is false! e = " << e.what() << std::endl;
    return false;
  }
  std::cout << "####ReadCsvFile is success!####" << std::endl;
  return true;
}

void WriteSecretKey(const std::string &ecdh_secret_key_path) {
  std::ofstream wf(ecdh_secret_key_path, std::ios::out | std::ios::binary);

  std::vector<uint8_t> oprf_key = yacl::crypto::RandBytes(32);

  wf.write((const char *)oprf_key.data(), oprf_key.size());
  wf.close();
}

}  // namespace

namespace psi::apsi {

class PirCsvTest : public testing::TestWithParam<TestParams> {};

TEST_P(PirCsvTest, Works) {
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("******************* pir csv test begin ********************");
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("**********************************************************");
  auto params = GetParam();
  auto ctxs = yacl::link::test::SetupWorld(2);
  ::apsi::PSIParams psi_params = GetPsiParams(params.nr, params.ns);

  std::string tmp_store_path =
      fmt::format("data_{}_{}", yacl::crypto::FastRandU64(), params.ns);

  std::filesystem::create_directory(tmp_store_path);

  // register remove of temp dir.
  ON_SCOPE_EXIT([&] {
    if (!tmp_store_path.empty()) {
      std::error_code ec;
      std::filesystem::remove_all(tmp_store_path, ec);
      if (ec.value() != 0) {
        SPDLOG_WARN("can not remove tmp dir: {}, msg: {}", tmp_store_path,
                    ec.message());
      }
    }
  });

  std::string client_csv_path ="examples/pir/data/client_csv.csv";
  const std::string& server_csv_path = "{\n"
                             "  \"datasource_kind\": 5,\n"
                             "  \"server_file_path\": \"examples/pir/data/server_csv.csv\"\n"
                             "}";

  std::string pir_result_path = "examples/pir/data/pir_csv_result.csv";
  
  std::vector<size_t> intersection_idx;
  std::vector<std::string> intersection_label;

  {
    SPDLOG_WARN("####client_csv_data####");
    ReadCsvFile(client_csv_path);
  }

  // generate 32 bytes oprf_key
  std::string oprf_key_path = fmt::format("{}/oprf_key.bin", tmp_store_path);
  WriteSecretKey(oprf_key_path);

  std::string setup_path = fmt::format("{}/setup_path", tmp_store_path);

  std::vector<std::string> ids = {"id", "name"};
  std::vector<std::string> labels = {"age", "label"};

  SPDLOG_WARN("####data is ready, now to compute####");

  std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now(); // 开始计时

  ::psi::PirServerConfig config;

  config.set_input_path(server_csv_path);
  config.mutable_key_columns()->Add(ids.begin(), ids.end());
  config.mutable_label_columns()->Add(labels.begin(), labels.end());

  config.mutable_apsi_server_config()->set_num_per_query(params.nr);
  config.set_label_max_len(params.label_bytes);
  config.mutable_apsi_server_config()->set_oprf_key_path(oprf_key_path);
  config.set_setup_path(setup_path);
  config.mutable_apsi_server_config()->set_compressed(params.compressed);
  config.set_bucket_size(params.bucket_size);

  ::psi::PirResultReport setup_report = PirServerSetup(config);

  EXPECT_EQ(setup_report.data_count(), params.ns);

  std::future<::psi::PirResultReport> f_server = std::async([&] {
    ::psi::PirServerConfig config;
    config.set_setup_path(setup_path);

    ::psi::PirResultReport report = PirServerOnline(ctxs[0], config);
    return report;
  });

  std::future<::psi::PirResultReport> f_client = std::async([&] {
    ::psi::PirClientConfig config;

    config.set_input_path(client_csv_path);
    config.mutable_key_columns()->Add(ids.begin(), ids.end());
    config.set_output_path(pir_result_path);

    ::psi::PirResultReport report = PirClient(ctxs[1], config);
    return report;
  });

  ::psi::PirResultReport server_report = f_server.get();
  ::psi::PirResultReport client_report = f_client.get();
   
  std::chrono::high_resolution_clock::time_point finish = std::chrono::high_resolution_clock::now(); // 结束计时
  auto during_time = std::chrono::duration_cast<std::chrono::milliseconds>(finish - start).count();

  EXPECT_EQ(server_report.data_count(), params.nr);
  EXPECT_EQ(client_report.data_count(), params.nr);

  std::shared_ptr<::psi::ILabeledBatchProvider> pir_result_provider =
      std::make_shared<::psi::ArrowCsvBatchProvider>(
          pir_result_path, ids, intersection_idx.size(), labels);

  // read pir_result from csv
  std::vector<std::string> pir_result_ids;
  std::vector<std::string> pir_result_labels;
  std::tie(pir_result_ids, pir_result_labels) =
      pir_result_provider->ReadNextLabeledBatch();
  
  SPDLOG_WARN("###########pir_csv_result_csv_data###########");
  ReadCsvFile(pir_result_path);

  // check pir result correct
  EXPECT_EQ(pir_result_ids.size(), intersection_idx.size());

  EXPECT_EQ(pir_result_labels, intersection_label);
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("******************* pir csv test end *********************");
  SPDLOG_INFO("****************data_count: {}, during_time: {}ms*********", params.ns, during_time);
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("**********************************************************");
}

INSTANTIATE_TEST_SUITE_P(Works_Instances, PirCsvTest,testing::Values(TestParams{1, 10, 64}));

class PirMysqlTest : public testing::TestWithParam<TestParams> {};

TEST_P(PirMysqlTest, Works) {
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("******************* pir mysql test begin *****************");
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("**********************************************************");
  auto params = GetParam();
  auto ctxs = yacl::link::test::SetupWorld(2);
  ::apsi::PSIParams psi_params = GetPsiParams(params.nr, params.ns);

  std::string tmp_store_path =
      fmt::format("data_{}_{}", yacl::crypto::FastRandU64(), params.ns);

  std::filesystem::create_directory(tmp_store_path);

  // register remove of temp dir.
  ON_SCOPE_EXIT([&] {
    if (!tmp_store_path.empty()) {
      std::error_code ec;
      std::filesystem::remove_all(tmp_store_path, ec);
      if (ec.value() != 0) {
        SPDLOG_WARN("can not remove tmp dir: {}, msg: {}", tmp_store_path,
                    ec.message());
      }
    }
  });

  std::string client_csv_path ="examples/pir/data/client_mysql.csv";

  const std::string& server_csv_path = "{\n"
                             "  \"connection_str\": \"host=172.16.0.217; port=12001; user=root; password=isafelinkepw@2077;db=linkempc;compress=true;auto-reconnect=true\",\n"
                             "  \"datasource_kind\": 1,\n"
                             "  \"table_name\": \"auto_asset\"\n"
                             "}";

  std::string pir_result_path = "examples/pir/data/pir_mysql_result.csv";
  
  std::vector<size_t> intersection_idx;
  std::vector<std::string> intersection_label;

  {
    SPDLOG_WARN("####client_csv_data####");
    ReadCsvFile(client_csv_path);
  }

  // generate 32 bytes oprf_key
  std::string oprf_key_path = fmt::format("{}/oprf_key.bin", tmp_store_path);
  WriteSecretKey(oprf_key_path);

  std::string setup_path = fmt::format("{}/setup_path", tmp_store_path);

  std::vector<std::string> ids = {"id"};
  std::vector<std::string> labels = {"house","age"};

  SPDLOG_WARN("####data is ready, now to compute####");

  std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now(); // 开始计时

  ::psi::PirServerConfig config;

  config.set_input_path(server_csv_path);
  config.mutable_key_columns()->Add(ids.begin(), ids.end());
  config.mutable_label_columns()->Add(labels.begin(), labels.end());

  config.mutable_apsi_server_config()->set_num_per_query(params.nr);
  config.set_label_max_len(params.label_bytes);
  config.mutable_apsi_server_config()->set_oprf_key_path(oprf_key_path);
  config.set_setup_path(setup_path);
  config.mutable_apsi_server_config()->set_compressed(params.compressed);
  config.set_bucket_size(params.bucket_size);

  ::psi::PirResultReport setup_report = PirServerSetup(config);

  EXPECT_EQ(setup_report.data_count(), params.ns);

  std::future<::psi::PirResultReport> f_server = std::async([&] {
    ::psi::PirServerConfig config;
    config.set_setup_path(setup_path);

    ::psi::PirResultReport report = PirServerOnline(ctxs[0], config);
    return report;
  });

  std::future<::psi::PirResultReport> f_client = std::async([&] {
    ::psi::PirClientConfig config;

    config.set_input_path(client_csv_path);
    config.mutable_key_columns()->Add(ids.begin(), ids.end());
    config.set_output_path(pir_result_path);

    ::psi::PirResultReport report = PirClient(ctxs[1], config);
    return report;
  });

  ::psi::PirResultReport server_report = f_server.get();
  ::psi::PirResultReport client_report = f_client.get();

  std::chrono::high_resolution_clock::time_point finish = std::chrono::high_resolution_clock::now(); // 结束计时
  auto during_time = std::chrono::duration_cast<std::chrono::milliseconds>(finish - start).count();

  EXPECT_EQ(server_report.data_count(), params.nr);
  EXPECT_EQ(client_report.data_count(), params.nr);

  std::shared_ptr<::psi::ILabeledBatchProvider> pir_result_provider =
      std::make_shared<::psi::ArrowCsvBatchProvider>(
          pir_result_path, ids, intersection_idx.size(), labels);

  // read pir_result from csv
  std::vector<std::string> pir_result_ids;
  std::vector<std::string> pir_result_labels;
  std::tie(pir_result_ids, pir_result_labels) =
      pir_result_provider->ReadNextLabeledBatch();
  
  SPDLOG_WARN("###########pir_mysql_result_csv_data###########");
  ReadCsvFile(pir_result_path);

  // check pir result correct
  EXPECT_EQ(pir_result_ids.size(), intersection_idx.size());

  EXPECT_EQ(pir_result_labels, intersection_label);
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("******************* pir mysql test end *******************");
  SPDLOG_INFO("****************data_count: {}, during_time: {}ms*********", params.ns, during_time);
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("**********************************************************");
}

INSTANTIATE_TEST_SUITE_P(Works_Instances, PirMysqlTest,testing::Values(TestParams{1, 1000, 64}));

class PirPgTest : public testing::TestWithParam<TestParams> {};

TEST_P(PirPgTest, Works) {
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("******************* pir pg test begin ********************");
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("**********************************************************");
  auto params = GetParam();
  auto ctxs = yacl::link::test::SetupWorld(2);
  ::apsi::PSIParams psi_params = GetPsiParams(params.nr, params.ns);

  std::string tmp_store_path =
      fmt::format("data_{}_{}", yacl::crypto::FastRandU64(), params.ns);

  std::filesystem::create_directory(tmp_store_path);

  // register remove of temp dir.
  ON_SCOPE_EXIT([&] {
    if (!tmp_store_path.empty()) {
      std::error_code ec;
      std::filesystem::remove_all(tmp_store_path, ec);
      if (ec.value() != 0) {
        SPDLOG_WARN("can not remove tmp dir: {}, msg: {}", tmp_store_path,
                    ec.message());
      }
    }
  });

  std::string client_csv_path ="examples/pir/data/client_pg.csv";

  const std::string& server_csv_path = "{\n"
                             "  \"connection_str\": \"host=172.16.16.116 port=9876 user=postgres password=123456 dbname=postgres\",\n"
                             "  \"datasource_kind\": 2,\n"
                             "  \"table_name\": \"test_person\"\n"
                             "}";

  std::string pir_result_path = "examples/pir/data/pir_pg_result.csv";
  
  std::vector<size_t> intersection_idx;
  std::vector<std::string> intersection_label;

  {
    SPDLOG_WARN("####client_csv_data####");
    ReadCsvFile(client_csv_path);
  }

  // generate 32 bytes oprf_key
  std::string oprf_key_path = fmt::format("{}/oprf_key.bin", tmp_store_path);
  WriteSecretKey(oprf_key_path);

  std::string setup_path = fmt::format("{}/setup_path", tmp_store_path);

  std::vector<std::string> ids = {"id", "name", "age"};
  std::vector<std::string> labels = {"label"};

  SPDLOG_WARN("####data is ready, now to compute####");

  std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now(); // 开始计时

  ::psi::PirServerConfig config;

  config.set_input_path(server_csv_path);
  config.mutable_key_columns()->Add(ids.begin(), ids.end());
  config.mutable_label_columns()->Add(labels.begin(), labels.end());

  config.mutable_apsi_server_config()->set_num_per_query(params.nr);
  config.set_label_max_len(params.label_bytes);
  config.mutable_apsi_server_config()->set_oprf_key_path(oprf_key_path);
  config.set_setup_path(setup_path);
  config.mutable_apsi_server_config()->set_compressed(params.compressed);
  config.set_bucket_size(params.bucket_size);

  ::psi::PirResultReport setup_report = PirServerSetup(config);

  EXPECT_EQ(setup_report.data_count(), params.ns);

  std::future<::psi::PirResultReport> f_server = std::async([&] {
    ::psi::PirServerConfig config;
    config.set_setup_path(setup_path);

    ::psi::PirResultReport report = PirServerOnline(ctxs[0], config);
    return report;
  });

  std::future<::psi::PirResultReport> f_client = std::async([&] {
    ::psi::PirClientConfig config;

    config.set_input_path(client_csv_path);
    config.mutable_key_columns()->Add(ids.begin(), ids.end());
    config.set_output_path(pir_result_path);

    ::psi::PirResultReport report = PirClient(ctxs[1], config);
    return report;
  });

  ::psi::PirResultReport server_report = f_server.get();
  ::psi::PirResultReport client_report = f_client.get();

  std::chrono::high_resolution_clock::time_point finish = std::chrono::high_resolution_clock::now(); // 结束计时
  auto during_time = std::chrono::duration_cast<std::chrono::milliseconds>(finish - start).count();

  EXPECT_EQ(server_report.data_count(), params.nr);
  EXPECT_EQ(client_report.data_count(), params.nr);

  std::shared_ptr<::psi::ILabeledBatchProvider> pir_result_provider =
      std::make_shared<::psi::ArrowCsvBatchProvider>(
          pir_result_path, ids, intersection_idx.size(), labels);

  // read pir_result from csv
  std::vector<std::string> pir_result_ids;
  std::vector<std::string> pir_result_labels;
  std::tie(pir_result_ids, pir_result_labels) =
      pir_result_provider->ReadNextLabeledBatch();
  
  SPDLOG_WARN("###########pir_pg_result_csv_data###########");
  ReadCsvFile(pir_result_path);

  // check pir result correct
  EXPECT_EQ(pir_result_ids.size(), intersection_idx.size());

  EXPECT_EQ(pir_result_labels, intersection_label);
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("******************* pir pg test end **********************");
  SPDLOG_INFO("****************data_count: {}, during_time: {}ms*********", params.ns, during_time);
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("**********************************************************");
}

INSTANTIATE_TEST_SUITE_P(Works_Instances, PirPgTest,testing::Values(TestParams{1, 15, 64}));


class PirPgOdbcTest : public testing::TestWithParam<TestParams> {};

TEST_P(PirPgOdbcTest, Works) {
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("******************* pir dmodbc test begin ****************");
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("**********************************************************");
  auto params = GetParam();
  auto ctxs = yacl::link::test::SetupWorld(2);
  ::apsi::PSIParams psi_params = GetPsiParams(params.nr, params.ns);

  std::string tmp_store_path =
      fmt::format("data_{}_{}", yacl::crypto::FastRandU64(), params.ns);

  std::filesystem::create_directory(tmp_store_path);

  // register remove of temp dir.
  ON_SCOPE_EXIT([&] {
    if (!tmp_store_path.empty()) {
      std::error_code ec;
      std::filesystem::remove_all(tmp_store_path, ec);
      if (ec.value() != 0) {
        SPDLOG_WARN("can not remove tmp dir: {}, msg: {}", tmp_store_path,
                    ec.message());
      }
    }
  });

  std::string client_csv_path ="examples/pir/data/client_pg.csv";

  const std::string& server_csv_path = "{\n"
                             "  \"connection_str\": \"DRIVER=PostgreSQL Unicode;DATABASE=postgres;SERVER=172.16.16.116;PORT=9876;UID=postgres;PWD=123456;\",\n"
                             "  \"datasource_kind\": 3,\n"
                             "  \"datasource_kind_sub\": 0,\n"
                             "  \"table_name\": \"test_person\"\n"
                             "}";

  std::string pir_result_path = "examples/pir/data/pir_pgodbc_result.csv";
  
  std::vector<size_t> intersection_idx;
  std::vector<std::string> intersection_label;

  {
    SPDLOG_WARN("####client_csv_data####");
    ReadCsvFile(client_csv_path);
  }

  // generate 32 bytes oprf_key
  std::string oprf_key_path = fmt::format("{}/oprf_key.bin", tmp_store_path);
  WriteSecretKey(oprf_key_path);

  std::string setup_path = fmt::format("{}/setup_path", tmp_store_path);

  std::vector<std::string> ids = {"id", "name", "age"};
  std::vector<std::string> labels = {"label"};

  SPDLOG_WARN("####data is ready, now to compute####");

  std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now(); // 开始计时

  ::psi::PirServerConfig config;

  config.set_input_path(server_csv_path);
  config.mutable_key_columns()->Add(ids.begin(), ids.end());
  config.mutable_label_columns()->Add(labels.begin(), labels.end());

  config.mutable_apsi_server_config()->set_num_per_query(params.nr);
  config.set_label_max_len(params.label_bytes);
  config.mutable_apsi_server_config()->set_oprf_key_path(oprf_key_path);
  config.set_setup_path(setup_path);
  config.mutable_apsi_server_config()->set_compressed(params.compressed);
  config.set_bucket_size(params.bucket_size);

  ::psi::PirResultReport setup_report = PirServerSetup(config);

  EXPECT_EQ(setup_report.data_count(), params.ns);

  std::future<::psi::PirResultReport> f_server = std::async([&] {
    ::psi::PirServerConfig config;
    config.set_setup_path(setup_path);

    ::psi::PirResultReport report = PirServerOnline(ctxs[0], config);
    return report;
  });

  std::future<::psi::PirResultReport> f_client = std::async([&] {
    ::psi::PirClientConfig config;

    config.set_input_path(client_csv_path);
    config.mutable_key_columns()->Add(ids.begin(), ids.end());
    config.set_output_path(pir_result_path);

    ::psi::PirResultReport report = PirClient(ctxs[1], config);
    return report;
  });

  ::psi::PirResultReport server_report = f_server.get();
  ::psi::PirResultReport client_report = f_client.get();

  std::chrono::high_resolution_clock::time_point finish = std::chrono::high_resolution_clock::now(); // 结束计时
  auto during_time = std::chrono::duration_cast<std::chrono::milliseconds>(finish - start).count();

  EXPECT_EQ(server_report.data_count(), params.nr);
  EXPECT_EQ(client_report.data_count(), params.nr);

  std::shared_ptr<::psi::ILabeledBatchProvider> pir_result_provider =
      std::make_shared<::psi::ArrowCsvBatchProvider>(
          pir_result_path, ids, intersection_idx.size(), labels);

  // read pir_result from csv
  std::vector<std::string> pir_result_ids;
  std::vector<std::string> pir_result_labels;
  std::tie(pir_result_ids, pir_result_labels) =
      pir_result_provider->ReadNextLabeledBatch();
  
  SPDLOG_WARN("###########pir_pgodbc_result_csv_data###########");
  ReadCsvFile(pir_result_path);

  // check pir result correct
  EXPECT_EQ(pir_result_ids.size(), intersection_idx.size());

  EXPECT_EQ(pir_result_labels, intersection_label);

  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("******************* pir pgodbc test end ******************");
  SPDLOG_INFO("****************data_count: {}, during_time: {}ms*********", params.ns, during_time);
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("**********************************************************");
}

INSTANTIATE_TEST_SUITE_P(Works_Instances, PirPgOdbcTest,testing::Values(TestParams{1, 15, 64}));


class PirDmOdbcTest : public testing::TestWithParam<TestParams> {};

TEST_P(PirDmOdbcTest, Works) {
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("******************* pir pgodbc test begin ****************");
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("**********************************************************");
  auto params = GetParam();
  auto ctxs = yacl::link::test::SetupWorld(2);
  ::apsi::PSIParams psi_params = GetPsiParams(params.nr, params.ns);

  std::string tmp_store_path =
      fmt::format("data_{}_{}", yacl::crypto::FastRandU64(), params.ns);

  std::filesystem::create_directory(tmp_store_path);

  // register remove of temp dir.
  ON_SCOPE_EXIT([&] {
    if (!tmp_store_path.empty()) {
      std::error_code ec;
      std::filesystem::remove_all(tmp_store_path, ec);
      if (ec.value() != 0) {
        SPDLOG_WARN("can not remove tmp dir: {}, msg: {}", tmp_store_path,
                    ec.message());
      }
    }
  });

  std::string client_csv_path ="examples/pir/data/client_dm.csv";

  const std::string& server_csv_path = "{\n"
                             "  \"connection_str\": \"DSN=dm;SERVER=172.16.0.217;UID=SYSDBA;PWD=SYSDBA001;TCP_PORT=52360;\",\n"
                             "  \"datasource_kind\": 3,\n"
                             "  \"datasource_kind_sub\": 1,\n"
                             "  \"table_name\": \"CREDIT_ACTIVE000\"\n"
                             "}";

  std::string pir_result_path = "examples/pir/data/pir_dmodbc_result.csv";
  
  std::vector<size_t> intersection_idx;
  std::vector<std::string> intersection_label;

  {
    SPDLOG_WARN("####client_csv_data####");
    ReadCsvFile(client_csv_path);
  }

  // generate 32 bytes oprf_key
  std::string oprf_key_path = fmt::format("{}/oprf_key.bin", tmp_store_path);
  WriteSecretKey(oprf_key_path);

  std::string setup_path = fmt::format("{}/setup_path", tmp_store_path);

  std::vector<std::string> ids = {"ID", "A"};
  std::vector<std::string> labels = {"B", "F"};

  SPDLOG_WARN("####data is ready, now to compute####");

  std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now(); // 开始计时

  ::psi::PirServerConfig config;

  config.set_input_path(server_csv_path);
  config.mutable_key_columns()->Add(ids.begin(), ids.end());
  config.mutable_label_columns()->Add(labels.begin(), labels.end());

  config.mutable_apsi_server_config()->set_num_per_query(params.nr);
  config.set_label_max_len(params.label_bytes);
  config.mutable_apsi_server_config()->set_oprf_key_path(oprf_key_path);
  config.set_setup_path(setup_path);
  config.mutable_apsi_server_config()->set_compressed(params.compressed);
  config.set_bucket_size(params.bucket_size);

  ::psi::PirResultReport setup_report = PirServerSetup(config);

  EXPECT_EQ(setup_report.data_count(), params.ns);

  std::future<::psi::PirResultReport> f_server = std::async([&] {
    ::psi::PirServerConfig config;
    config.set_setup_path(setup_path);

    ::psi::PirResultReport report = PirServerOnline(ctxs[0], config);
    return report;
  });

  std::future<::psi::PirResultReport> f_client = std::async([&] {
    ::psi::PirClientConfig config;

    config.set_input_path(client_csv_path);
    config.mutable_key_columns()->Add(ids.begin(), ids.end());
    config.set_output_path(pir_result_path);

    ::psi::PirResultReport report = PirClient(ctxs[1], config);
    return report;
  });

  ::psi::PirResultReport server_report = f_server.get();
  ::psi::PirResultReport client_report = f_client.get();

  std::chrono::high_resolution_clock::time_point finish = std::chrono::high_resolution_clock::now(); // 结束计时
  auto during_time = std::chrono::duration_cast<std::chrono::milliseconds>(finish - start).count();

  EXPECT_EQ(server_report.data_count(), params.nr);
  EXPECT_EQ(client_report.data_count(), params.nr);

  std::shared_ptr<::psi::ILabeledBatchProvider> pir_result_provider =
      std::make_shared<::psi::ArrowCsvBatchProvider>(
          pir_result_path, ids, intersection_idx.size(), labels);

  // read pir_result from csv
  std::vector<std::string> pir_result_ids;
  std::vector<std::string> pir_result_labels;
  std::tie(pir_result_ids, pir_result_labels) =
      pir_result_provider->ReadNextLabeledBatch();
  
  SPDLOG_WARN("###########pir_dmodbc_result_csv_data###########");
  ReadCsvFile(pir_result_path);

  // check pir result correct
  EXPECT_EQ(pir_result_ids.size(), intersection_idx.size());

  EXPECT_EQ(pir_result_labels, intersection_label);

  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("******************* pir dmodbc test end ******************");
  SPDLOG_INFO("****************data_count: {}, during_time: {}ms*********", params.ns, during_time);
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("**********************************************************");
}

INSTANTIATE_TEST_SUITE_P(Works_Instances, PirDmOdbcTest,testing::Values(TestParams{1, 150000, 64}));

}  // namespace psi::apsi
