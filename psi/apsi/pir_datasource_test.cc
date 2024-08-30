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
  size_t bucket_size = 1000000;
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
  std::cout << "ReadCsvFile is success!" << std::endl;
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

class PirTest : public testing::TestWithParam<TestParams> {};

TEST_P(PirTest, Works) {
  SPDLOG_INFO("*******************pir test begin *********************");
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

  std::string client_csv_path ="examples/pir/data/client.csv";
  //std::string server_csv_path = "examples/pir/data/server.csv";


  const std::string& server_csv_path = "{\n"
                             "  \"connection_str\": \"DRIVER=PostgreSQL Unicode;DATABASE=postgres;SERVER=172.16.16.116;PORT=9876;UID=postgres;PWD=123456;\",\n"
                             "  \"datasource_kind\": 3,\n"
                             "  \"table_name\": \"test_person\"\n"
                             "}";
  // const std::string& server_csv_path = "{\n"
  //                            "  \"datasource_kind\": 5,\n"
  //                            "  \"server_file_path\": \"examples/pir/data/server.csv\"\n"
  //                            "}";
  std::string pir_result_path = "examples/pir/data/pir_result.csv";
  
  std::string id_cloumn_name = "id";
  std::string label_cloumn_name = "label";

  std::vector<size_t> intersection_idx;
  std::vector<std::string> intersection_label;

  // generate test csv data
  {
    SPDLOG_WARN("**************************client_csv_data****************************");
    ReadCsvFile(client_csv_path);
    // SPDLOG_WARN("**************************server_csv_data****************************");
    // ReadCsvFile(server_csv_path);
  }

  // generate 32 bytes oprf_key
  std::string oprf_key_path = fmt::format("{}/oprf_key.bin", tmp_store_path);
  WriteSecretKey(oprf_key_path);

  std::string setup_path = fmt::format("{}/setup_path", tmp_store_path);
  // std::string pir_result_path =
  //     fmt::format("{}/pir_result.csv", tmp_store_path);

  std::vector<std::string> ids = {id_cloumn_name};
  std::vector<std::string> labels = {"label"};

  if (params.use_filedb) {
    SPDLOG_WARN("**********************data is ready, now to compute************************");
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

    EXPECT_EQ(server_report.data_count(), params.nr);
    EXPECT_EQ(client_report.data_count(), params.nr);

  } else {
    std::future<::psi::PirResultReport> f_server = std::async([&] {
      ::psi::PirServerConfig config;

      config.set_input_path(server_csv_path);
      config.mutable_key_columns()->Add(ids.begin(), ids.end());
      config.mutable_label_columns()->Add(labels.begin(), labels.end());

      config.mutable_apsi_server_config()->set_num_per_query(params.nr);
      config.set_label_max_len(params.label_bytes);
      config.mutable_apsi_server_config()->set_compressed(params.compressed);
      config.set_bucket_size(params.bucket_size);

      ::psi::PirResultReport report = PirServerFull(ctxs[0], config);
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

    EXPECT_EQ(server_report.data_count(), params.nr);
    EXPECT_EQ(client_report.data_count(), params.nr);
  }

  std::shared_ptr<::psi::ILabeledBatchProvider> pir_result_provider =
      std::make_shared<::psi::ArrowCsvBatchProvider>(
          pir_result_path, ids, intersection_idx.size(), labels);

  // read pir_result from csv
  std::vector<std::string> pir_result_ids;
  std::vector<std::string> pir_result_labels;
  std::tie(pir_result_ids, pir_result_labels) =
      pir_result_provider->ReadNextLabeledBatch();
  
  SPDLOG_WARN("**************************server_csv_data****************************");
  ReadCsvFile(pir_result_path);

  for (auto id : pir_result_ids) {
    SPDLOG_WARN("pir result: id: {}", id);
  }
  for (auto label : pir_result_labels) {
    SPDLOG_WARN("pir result: label: {}", label);
  }

  // check pir result correct
  EXPECT_EQ(pir_result_ids.size(), intersection_idx.size());

  EXPECT_EQ(pir_result_labels, intersection_label);
}

INSTANTIATE_TEST_SUITE_P(Works_Instances, PirTest,
                         testing::Values(                         //
                             TestParams{1, 15, 64}         // 10-10K-32
                             )  // 100-100K-32

);

}  // namespace psi::apsi
