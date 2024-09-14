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

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <cstddef>
#include <filesystem>
#include <iostream>
#include <iterator>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/csv/api.h"
#include "arrow/io/api.h"
#include "arrow/ipc/api.h"
#include "boost/uuid/uuid.hpp"
#include "boost/uuid/uuid_generators.hpp"
#include "boost/uuid/uuid_io.hpp"
#include "gtest/gtest.h"
#include "spdlog/spdlog.h"
#include "yacl/link/test_util.h"

#include "psi/factory.h"
#include "psi/prelude.h"
#include "psi/utils/io.h"

#include "psi/proto/psi_v2.pb.h"

namespace psi {
namespace {

struct TestTable {
  std::vector<std::string> headers;

  std::vector<std::vector<std::string>> rows;
};

struct TestParams {
  std::string title;
  std::vector<TestTable> inputs;
  std::vector<TestTable> outputs;
  std::vector<std::vector<std::string>> keys;
  bool disable_alignment = true;
  bool broadcast_result = false;
  v2::PsiConfig::AdvancedJoinType advanced_join_type =
      v2::PsiConfig::ADVANCED_JOIN_TYPE_UNSPECIFIED;
};

// void SaveTableAsFile(const TestTable& data, const std::string& path) {
//   io::FileIoOptions io_opt(path);

//   yacl::io::Schema schema;
//   schema.feature_names = data.headers;
//   schema.feature_types = std::vector<yacl::io::Schema::Type>(
//       data.headers.size(), yacl::io::Schema::STRING);

//   io::CsvOptions csv_opt;
//   csv_opt.writer_options.file_schema = schema;

//   std::unique_ptr<io::Writer> writer = io::BuildWriter(io_opt, csv_opt);
//   yacl::io::ColumnVectorBatch batch;
//   for (size_t i = 0; i < data.headers.size(); i++) {
//     std::vector<std::string> col;
//     col.reserve(data.rows.size());
//     for (const auto& row : data.rows) {
//       col.emplace_back(row[i]);
//     }
//     batch.AppendCol(col);
//   }

//   writer->Add(batch);
//   writer->Flush();
//   writer->Close();
// }

TestTable LoadTableFromFile(const std::string& path,
                            const std::vector<std::string>& headers) {
  EXPECT_TRUE(std::filesystem::exists(path));

  arrow::io::IOContext io_context = arrow::io::default_io_context();
  std::shared_ptr<arrow::io::ReadableFile> infile;
  infile = arrow::io::ReadableFile::Open(path, arrow::default_memory_pool())
               .ValueOrDie();
  auto read_options = arrow::csv::ReadOptions::Defaults();
  auto parse_options = arrow::csv::ParseOptions::Defaults();

  auto convert_options = arrow::csv::ConvertOptions::Defaults();
  for (const auto& header : headers) {
    convert_options.column_types[header] = arrow::utf8();
  }
  auto reader =
      arrow::csv::StreamingReader::Make(io_context, infile, read_options,
                                        parse_options, convert_options)
          .ValueOrDie();

  const std::shared_ptr<arrow::Schema>& input_schema = reader->schema();
  EXPECT_EQ(headers.size(), input_schema->num_fields());

  TestTable res;
  res.headers = headers;
  std::shared_ptr<arrow::RecordBatch> batch;
  while (true) {
    arrow::Status status = reader->ReadNext(&batch);

    if (!status.ok()) {
      YACL_THROW("Read csv error.");
    }

    if (batch == NULL) {
      break;
    }

    for (int i = 0; i < batch->num_rows(); i++) {
      std::vector<std::string> row;
      for (const auto& header : headers) {
        auto a = std::static_pointer_cast<arrow::StringArray>(
            batch->GetColumnByName(header));
        row.emplace_back(a->Value(i));
        // for (auto r : row) {
        //   SPDLOG_INFO("row: {}", r);
        // }
      }
      res.rows.emplace_back(row);
    }
  }

  return res;
}

class PsiCsvTest
    : public testing::TestWithParam<std::tuple<v2::Protocol, TestParams>> {
 protected:
  void SetUp() override { tmp_dir_ = std::filesystem::temp_directory_path(); }
  // void TearDown() override {
  //   std::error_code ec;

  //   for (const auto& p : tmp_paths_) {
  //     if (std::filesystem::exists(p)) {
  //       if (std::filesystem::is_directory(p)) {
  //         std::filesystem::remove_all(p, ec);
  //         if (ec.value() != 0) {
  //           SPDLOG_WARN("can not remove temp dir: {}, msg: {}", p.string(),
  //                       ec.message());
  //         }
  //       } else {
  //         std::filesystem::remove(p, ec);
  //         if (ec.value() != 0) {
  //           SPDLOG_WARN("can not remove temp file: {}, msg: {}", p.string(),
  //                       ec.message());
  //         }
  //       }
  //     }
  //   }
  // }

  std::vector<std::filesystem::path> GenTempPaths(
      const std::string& name_prefix, int cnt) {
    std::vector<std::filesystem::path> res;
    res.reserve(cnt);
    for (int i = 0; i < cnt; ++i) {
      res.emplace_back(tmp_dir_ / fmt::format("{}-{}", name_prefix, i));
    }
    tmp_paths_.insert(tmp_paths_.end(), res.begin(), res.end());

    return res;
  }

  std::filesystem::path tmp_dir_;
  std::vector<std::filesystem::path> tmp_paths_;
};

TEST_P(PsiCsvTest, Works) {
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("******************* psi csv test begin *******************");
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("**********************************************************");
  auto protocol = std::get<0>(GetParam());
  auto params = std::get<1>(GetParam());

  std::cout << "Test title: " << params.title << std::endl;
  std::cout << "Protocol: " << protocol << std::endl;

  const ::testing::TestInfo* const test_info =
      ::testing::UnitTest::GetInstance()->current_test_info();

  std::string test_suite_name = test_info->test_suite_name();
  std::string test_case_name = test_info->test_case_name();
  std::replace(test_suite_name.begin(), test_suite_name.end(), '/', '_');
  std::replace(test_case_name.begin(), test_case_name.end(), '/', '_');

  boost::uuids::random_generator uuid_generator;
  auto uuid_str = boost::uuids::to_string(uuid_generator());

  std::string test_name =
      test_suite_name + "-" + test_case_name + "-" + uuid_str;

  // std::vector<std::filesystem::path> input_paths =
  //     GenTempPaths(test_name + "-input", 2);
  
  const std::string& input_json_1 = "{\n"
                             "  \"datasource_kind\": 5,\n"
                             "  \"server_file_path\": \"examples/psi/data/input_mulkey_1.csv\"\n"
                             "}";
  const std::string& input_json_2 = "{\n"
                             "  \"datasource_kind\": 5,\n"
                             "  \"server_file_path\": \"examples/psi/data/input_mulkey_2.csv\"\n"
                             "}";
  std::vector<std::string> input_json{input_json_1, input_json_2};
  std::vector<std::filesystem::path> output_paths =
      GenTempPaths(test_name + "-output", 2);

  auto lctxs = yacl::link::test::SetupWorld(2);

  auto proc = [&](int idx) -> PsiResultReport {
    std::cout << "###idx: " << idx << std::endl;
    std::cout << "###input_paths[idx]: " << input_json[idx] << std::endl;
    v2::PsiConfig config;
    config.mutable_input_config()->set_path(input_json[idx]);
    config.mutable_input_config()->set_type(v2::IO_TYPE_FILE_CSV);
    config.mutable_keys()->Add(params.keys[idx].begin(),
                               params.keys[idx].end());
    config.mutable_output_config()->set_path(output_paths[idx]);
    config.mutable_output_config()->set_type(v2::IO_TYPE_FILE_CSV);
    config.set_disable_alignment(params.disable_alignment);
    config.mutable_protocol_config()->set_protocol(protocol);
    if (protocol == v2::PROTOCOL_ECDH) {
      config.mutable_protocol_config()->mutable_ecdh_config()->set_curve(
          CurveType::CURVE_25519);
    }
    config.mutable_protocol_config()->set_broadcast_result(
        params.broadcast_result);
    config.set_advanced_join_type(params.advanced_join_type);
    config.set_left_side(v2::Role::ROLE_RECEIVER);

    std::unique_ptr<AbstractPsiParty> party;
    if (idx == 0) {
      config.mutable_protocol_config()->set_role(v2::Role::ROLE_RECEIVER);
      party = createPsiParty(config, lctxs[idx]);
    } else {
      config.mutable_protocol_config()->set_role(v2::Role::ROLE_SENDER);
      party = createPsiParty(config, lctxs[idx]);
    }

    return party->Run();
  };

  size_t world_size = lctxs.size();
  std::vector<std::future<PsiResultReport>> f_links(world_size);
  for (size_t i = 0; i < world_size; i++) {
    // SaveTableAsFile(params.inputs[i], input_paths[i].string());
    f_links[i] = std::async(proc, i);
  }

  for (size_t i = 0; i < world_size; i++) {
    std::exception_ptr exptr = nullptr;

    PsiResultReport report;
    try {
      report = f_links[i].get();
    } catch (const std::exception& e) {
      exptr = std::current_exception();
      SPDLOG_ERROR("Error from party {}: {}", i, e.what());
    }
    if (exptr) {
      std::rethrow_exception(exptr);
    }
    if (i == 0 || params.broadcast_result || params.advanced_join_type) {
      TestTable output_hat = LoadTableFromFile(output_paths[i].string(),
                                               params.outputs[i].headers);
      EXPECT_EQ(params.outputs[i].rows, output_hat.rows);
    }
  }
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("******************* psi csv test end *********************");
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("**********************************************************");
}

INSTANTIATE_TEST_SUITE_P(
    Works_Instances, PsiCsvTest,
    testing::Combine(
        testing::Values(v2::PROTOCOL_ECDH, v2::PROTOCOL_KKRT,v2::PROTOCOL_RR22),
        testing::Values(
                        TestParams{"testcase 4: w/ payload",
                       // inputs
                       {TestTable{{}, {}},
                        TestTable{{}, {}}},
                       // outputs
                       {TestTable{// header
                                  {"测试id1", "测试id2", "测试payload1"},
                                  {// row
                                   {"测试1", "测试1", "测试first1"},
                                   // row
                                   {"测试3", "测试1", "测试first1"},
                                   // row
                                   {"测试3", "测试3", "测试third1"}}}},
                       // keys
                       {{"测试id1", "测试id2"}, {"测试id3", "测试id4"}},
                       /*disable_alignment = */ false,
                       /*broadcast_result = */ false})));

class PsiMysqlTest
    : public testing::TestWithParam<std::tuple<v2::Protocol, TestParams>> {
 protected:
  void SetUp() override { tmp_dir_ = std::filesystem::temp_directory_path(); }
  
  std::vector<std::filesystem::path> GenTempPaths(
      const std::string& name_prefix, int cnt) {
    std::vector<std::filesystem::path> res;
    res.reserve(cnt);
    for (int i = 0; i < cnt; ++i) {
      res.emplace_back(tmp_dir_ / fmt::format("{}-{}", name_prefix, i));
    }
    tmp_paths_.insert(tmp_paths_.end(), res.begin(), res.end());

    return res;
  }

  std::filesystem::path tmp_dir_;
  std::vector<std::filesystem::path> tmp_paths_;
};

TEST_P(PsiMysqlTest, Works) {
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("******************* psi mysql test begin *****************");
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("**********************************************************");
  auto protocol = std::get<0>(GetParam());
  auto params = std::get<1>(GetParam());

  std::cout << "Test title: " << params.title << std::endl;
  std::cout << "Protocol: " << protocol << std::endl;

  const ::testing::TestInfo* const test_info =
      ::testing::UnitTest::GetInstance()->current_test_info();

  std::string test_suite_name = test_info->test_suite_name();
  std::string test_case_name = test_info->test_case_name();
  std::replace(test_suite_name.begin(), test_suite_name.end(), '/', '_');
  std::replace(test_case_name.begin(), test_case_name.end(), '/', '_');

  boost::uuids::random_generator uuid_generator;
  auto uuid_str = boost::uuids::to_string(uuid_generator());

  std::string test_name =
      test_suite_name + "-" + test_case_name + "-" + uuid_str;

 const std::string& datasource_json = "{\n"
                             "  \"connection_str\": \"host=172.16.0.217; port=12001; user=root; password=isafelinkepw@2077;db=linkempc;compress=true;auto-reconnect=true;\",\n"
                             "  \"datasource_kind\": 1,\n"
                             "  \"table_name\": \"auto_asset\"\n"
                             "}";

  const std::string& datasource_json1 = "{\n"
                             "  \"connection_str\": \"host=172.16.0.217; port=12001; user=root; password=isafelinkepw@2077;db=linkempc;compress=true;auto-reconnect=true;\",\n"
                             "  \"datasource_kind\": 1,\n"
                             "  \"table_name\": \"auto_asset_bak\"\n"
                             "}";
  std::vector<std::string> input_paths{datasource_json, datasource_json1};
  
  std::vector<std::filesystem::path> output_paths =
      GenTempPaths(test_name + "-output", 2);

  auto lctxs = yacl::link::test::SetupWorld(2);

  auto proc = [&](int idx) -> PsiResultReport {
    std::cout << "###idx: " << idx << std::endl;
    v2::PsiConfig config;
    config.mutable_input_config()->set_path(input_paths[idx]);
    config.mutable_input_config()->set_type(v2::IO_TYPE_SQL);
    config.mutable_keys()->Add(params.keys[idx].begin(),
                               params.keys[idx].end());
    config.mutable_output_config()->set_path(output_paths[idx]);
    config.mutable_output_config()->set_type(v2::IO_TYPE_FILE_CSV);
    config.set_disable_alignment(params.disable_alignment);
    config.mutable_protocol_config()->set_protocol(protocol);
    if (protocol == v2::PROTOCOL_ECDH) {
      config.mutable_protocol_config()->mutable_ecdh_config()->set_curve(
          CurveType::CURVE_25519);
    }
    config.mutable_protocol_config()->set_broadcast_result(
        params.broadcast_result);
    config.set_advanced_join_type(params.advanced_join_type);
    config.set_left_side(v2::Role::ROLE_RECEIVER);

    std::unique_ptr<AbstractPsiParty> party;
    if (idx == 0) {
      config.mutable_protocol_config()->set_role(v2::Role::ROLE_RECEIVER);
      party = createPsiParty(config, lctxs[idx]);
    } else {
      config.mutable_protocol_config()->set_role(v2::Role::ROLE_SENDER);
      party = createPsiParty(config, lctxs[idx]);
    }

    return party->Run();
  };

  size_t world_size = lctxs.size();
  std::vector<std::future<PsiResultReport>> f_links(world_size);
  for (size_t i = 0; i < world_size; i++) {
    f_links[i] = std::async(proc, i);
  }

  for (size_t i = 0; i < world_size; i++) {
    std::exception_ptr exptr = nullptr;

    PsiResultReport report;
    try {
      report = f_links[i].get();
    } catch (const std::exception& e) {
      exptr = std::current_exception();
      SPDLOG_ERROR("Error from party {}: {}", i, e.what());
    }

    if (exptr) {
      std::rethrow_exception(exptr);
    }

    if (i == 0 || params.broadcast_result || params.advanced_join_type) {
      TestTable output_hat = LoadTableFromFile(output_paths[i].string(),
                                               params.outputs[i].headers);

      EXPECT_EQ(params.outputs[i].rows, output_hat.rows);
    }
  }
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("******************* psi mysql test end *******************");
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("**********************************************************");
}

INSTANTIATE_TEST_SUITE_P(
    Works_Instances, PsiMysqlTest,
    testing::Combine(
        testing::Values(v2::PROTOCOL_ECDH, v2::PROTOCOL_KKRT,v2::PROTOCOL_RR22),
        testing::Values(
                        TestParams{"testcase 4: w/ payload",
                       // inputs
                       {TestTable{{}, {}},
                        TestTable{// header
                                  {}, {}}},
                       // outputs
                       {TestTable{// header
                                  {"id", "house", "age"},
                                  {// row
                                   { "1", "1", "23" }, 
                                   { "10", "5", "39" }, 
                                   { "1000", "2", "31" }, 
                                   { "2", "2", "22" }, 
                                   { "3", "3", "29" }, 
                                   { "4", "0", "29" }, 
                                   { "5", "0", "25" }, 
                                   { "6", "1", "31" }, 
                                   { "7", "2", "21" }, 
                                   { "8", "3", "26" }, 
                                   { "9", "0", "28" }}}},
                       // keys
                       {{"id"}, {"id1"}},
                       /*disable_alignment = */ false,
                       /*broadcast_result = */ false})));


class PsiPgTest
    : public testing::TestWithParam<std::tuple<v2::Protocol, TestParams>> {
 protected:
  void SetUp() override { tmp_dir_ = std::filesystem::temp_directory_path(); }
  
  std::vector<std::filesystem::path> GenTempPaths(
      const std::string& name_prefix, int cnt) {
    std::vector<std::filesystem::path> res;
    res.reserve(cnt);
    for (int i = 0; i < cnt; ++i) {
      res.emplace_back(tmp_dir_ / fmt::format("{}-{}", name_prefix, i));
    }
    tmp_paths_.insert(tmp_paths_.end(), res.begin(), res.end());

    return res;
  }

  std::filesystem::path tmp_dir_;
  std::vector<std::filesystem::path> tmp_paths_;
};

TEST_P(PsiPgTest, Works) {
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("******************* psi pg test begin ********************");
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("**********************************************************");
  auto protocol = std::get<0>(GetParam());
  auto params = std::get<1>(GetParam());

  std::cout << "Test title: " << params.title << std::endl;
  std::cout << "Protocol: " << protocol << std::endl;

  const ::testing::TestInfo* const test_info =
      ::testing::UnitTest::GetInstance()->current_test_info();

  std::string test_suite_name = test_info->test_suite_name();
  std::string test_case_name = test_info->test_case_name();
  std::replace(test_suite_name.begin(), test_suite_name.end(), '/', '_');
  std::replace(test_case_name.begin(), test_case_name.end(), '/', '_');

  boost::uuids::random_generator uuid_generator;
  auto uuid_str = boost::uuids::to_string(uuid_generator());

  std::string test_name =
      test_suite_name + "-" + test_case_name + "-" + uuid_str;

 const std::string& datasource_json = "{\n"
                             "  \"connection_str\": \"host=172.16.16.116 port=9876 user=postgres password=123456 dbname=postgres\",\n"
                             "  \"datasource_kind\": 2,\n"
                             "  \"table_name\": \"test_person\"\n"
                             "}";

  const std::string& datasource_json1 = "{\n"
                             "  \"connection_str\": \"host=172.16.16.116 port=9876 user=postgres password=123456 dbname=postgres\",\n"
                             "  \"datasource_kind\": 2,\n"
                             "  \"table_name\": \"test_person_bak\"\n"
                             "}";
  std::vector<std::string> input_paths{datasource_json, datasource_json1};
  
  std::vector<std::filesystem::path> output_paths =
      GenTempPaths(test_name + "-output", 2);

  auto lctxs = yacl::link::test::SetupWorld(2);

  auto proc = [&](int idx) -> PsiResultReport {
    std::cout << "###idx: " << idx << std::endl;
    v2::PsiConfig config;
    config.mutable_input_config()->set_path(input_paths[idx]);
    config.mutable_input_config()->set_type(v2::IO_TYPE_SQL);
    config.mutable_keys()->Add(params.keys[idx].begin(),
                               params.keys[idx].end());
    config.mutable_output_config()->set_path(output_paths[idx]);
    config.mutable_output_config()->set_type(v2::IO_TYPE_FILE_CSV);
    config.set_disable_alignment(params.disable_alignment);
    config.mutable_protocol_config()->set_protocol(protocol);
    if (protocol == v2::PROTOCOL_ECDH) {
      config.mutable_protocol_config()->mutable_ecdh_config()->set_curve(
          CurveType::CURVE_25519);
    }
    config.mutable_protocol_config()->set_broadcast_result(
        params.broadcast_result);
    config.set_advanced_join_type(params.advanced_join_type);
    config.set_left_side(v2::Role::ROLE_RECEIVER);

    std::unique_ptr<AbstractPsiParty> party;
    if (idx == 0) {
      config.mutable_protocol_config()->set_role(v2::Role::ROLE_RECEIVER);
      party = createPsiParty(config, lctxs[idx]);
    } else {
      config.mutable_protocol_config()->set_role(v2::Role::ROLE_SENDER);
      party = createPsiParty(config, lctxs[idx]);
    }

    return party->Run();
  };

  size_t world_size = lctxs.size();
  std::vector<std::future<PsiResultReport>> f_links(world_size);
  for (size_t i = 0; i < world_size; i++) {
    f_links[i] = std::async(proc, i);
  }

  for (size_t i = 0; i < world_size; i++) {
    std::exception_ptr exptr = nullptr;

    PsiResultReport report;
    try {
      report = f_links[i].get();
    } catch (const std::exception& e) {
      exptr = std::current_exception();
      SPDLOG_ERROR("Error from party {}: {}", i, e.what());
    }

    if (exptr) {
      std::rethrow_exception(exptr);
    }

    if (i == 0 || params.broadcast_result || params.advanced_join_type) {
      TestTable output_hat = LoadTableFromFile(output_paths[i].string(),
                                               params.outputs[i].headers);

      EXPECT_EQ(params.outputs[i].rows, output_hat.rows);
    }
  }
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("******************* psi pg test end **********************");
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("**********************************************************");
}

INSTANTIATE_TEST_SUITE_P(
    Works_Instances, PsiPgTest,
    testing::Combine(
        testing::Values(v2::PROTOCOL_ECDH, v2::PROTOCOL_KKRT,v2::PROTOCOL_RR22),
        testing::Values(
                        TestParams{"testcase 4: w/ payload",
                       // inputs
                       {TestTable{{}, {}},
                        TestTable{// header
                                  {"id2", "payload2"},
                                  {// row
                                   {"3", "third2"},
                                   // row
                                   {"6", "sixth6"},
                                   // row
                                   {"1", "first2"}}}},
                       // outputs
                       {TestTable{// header
                                  {"id", "name", "age", "label"},
                                  {// row
                                   {"3","wangwu","22.987","0"},
                                   // row
                                   {"4","zhaoliu","24.384","0"},
                                   {"5","qianqi","26.254","0"}}}},
                       // keys
                       {{"id"}, {"id1"}},
                       /*disable_alignment = */ false,
                       /*broadcast_result = */ false})));

class PsiPgOdbcTest
    : public testing::TestWithParam<std::tuple<v2::Protocol, TestParams>> {
 protected:
  void SetUp() override { tmp_dir_ = std::filesystem::temp_directory_path(); }
  
  std::vector<std::filesystem::path> GenTempPaths(
      const std::string& name_prefix, int cnt) {
    std::vector<std::filesystem::path> res;
    res.reserve(cnt);
    for (int i = 0; i < cnt; ++i) {
      res.emplace_back(tmp_dir_ / fmt::format("{}-{}", name_prefix, i));
    }
    tmp_paths_.insert(tmp_paths_.end(), res.begin(), res.end());

    return res;
  }

  std::filesystem::path tmp_dir_;
  std::vector<std::filesystem::path> tmp_paths_;
};

TEST_P(PsiPgOdbcTest, Works) {
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("******************* psi pgodbc test begin ****************");
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("**********************************************************");
  auto protocol = std::get<0>(GetParam());
  auto params = std::get<1>(GetParam());

  std::cout << "Test title: " << params.title << std::endl;
  std::cout << "Protocol: " << protocol << std::endl;

  const ::testing::TestInfo* const test_info =
      ::testing::UnitTest::GetInstance()->current_test_info();

  std::string test_suite_name = test_info->test_suite_name();
  std::string test_case_name = test_info->test_case_name();
  std::replace(test_suite_name.begin(), test_suite_name.end(), '/', '_');
  std::replace(test_case_name.begin(), test_case_name.end(), '/', '_');

  boost::uuids::random_generator uuid_generator;
  auto uuid_str = boost::uuids::to_string(uuid_generator());

  std::string test_name =
      test_suite_name + "-" + test_case_name + "-" + uuid_str;

 const std::string& datasource_json = "{\n"
                             "  \"connection_str\": \"DRIVER=PostgreSQL Unicode;DATABASE=postgres;SERVER=172.16.16.116;PORT=9876;UID=postgres;PWD=123456;\",\n"
                             "  \"datasource_kind\": 3,\n"
                             "  \"datasource_kind_sub\": 0,\n"
                             "  \"table_name\": \"test_person\"\n"
                             "}";

  const std::string& datasource_json1 = "{\n"
                             "  \"connection_str\": \"DRIVER=PostgreSQL Unicode;DATABASE=postgres;SERVER=172.16.16.116;PORT=9876;UID=postgres;PWD=123456;\",\n"
                             "  \"datasource_kind\": 3,\n"
                             "  \"datasource_kind_sub\": 0,\n"
                             "  \"table_name\": \"test_person_bak\"\n"
                             "}";
  std::vector<std::string> input_paths{datasource_json, datasource_json1};
  
  std::vector<std::filesystem::path> output_paths =
      GenTempPaths(test_name + "-output", 2);

  auto lctxs = yacl::link::test::SetupWorld(2);

  auto proc = [&](int idx) -> PsiResultReport {
    std::cout << "###idx: " << idx << std::endl;
    v2::PsiConfig config;
    config.mutable_input_config()->set_path(input_paths[idx]);
    config.mutable_input_config()->set_type(v2::IO_TYPE_SQL);
    config.mutable_keys()->Add(params.keys[idx].begin(),
                               params.keys[idx].end());
    config.mutable_output_config()->set_path(output_paths[idx]);
    config.mutable_output_config()->set_type(v2::IO_TYPE_FILE_CSV);
    config.set_disable_alignment(params.disable_alignment);
    config.mutable_protocol_config()->set_protocol(protocol);
    if (protocol == v2::PROTOCOL_ECDH) {
      config.mutable_protocol_config()->mutable_ecdh_config()->set_curve(
          CurveType::CURVE_25519);
    }
    config.mutable_protocol_config()->set_broadcast_result(
        params.broadcast_result);
    config.set_advanced_join_type(params.advanced_join_type);
    config.set_left_side(v2::Role::ROLE_RECEIVER);

    std::unique_ptr<AbstractPsiParty> party;
    if (idx == 0) {
      config.mutable_protocol_config()->set_role(v2::Role::ROLE_RECEIVER);
      party = createPsiParty(config, lctxs[idx]);
    } else {
      config.mutable_protocol_config()->set_role(v2::Role::ROLE_SENDER);
      party = createPsiParty(config, lctxs[idx]);
    }

    return party->Run();
  };

  size_t world_size = lctxs.size();
  std::vector<std::future<PsiResultReport>> f_links(world_size);
  for (size_t i = 0; i < world_size; i++) {
    f_links[i] = std::async(proc, i);
  }

  for (size_t i = 0; i < world_size; i++) {
    std::exception_ptr exptr = nullptr;

    PsiResultReport report;
    try {
      report = f_links[i].get();
    } catch (const std::exception& e) {
      exptr = std::current_exception();
      SPDLOG_ERROR("Error from party {}: {}", i, e.what());
    }

    if (exptr) {
      std::rethrow_exception(exptr);
    }

    if (i == 0 || params.broadcast_result || params.advanced_join_type) {
      TestTable output_hat = LoadTableFromFile(output_paths[i].string(),
                                               params.outputs[i].headers);

      EXPECT_EQ(params.outputs[i].rows, output_hat.rows);
    }
  }
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("******************* psi pgodbc test end ******************");
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("**********************************************************");
}

INSTANTIATE_TEST_SUITE_P(
    Works_Instances, PsiPgOdbcTest,
    testing::Combine(
        testing::Values(v2::PROTOCOL_ECDH, v2::PROTOCOL_KKRT,v2::PROTOCOL_RR22),
        testing::Values(
                        TestParams{"testcase 4: w/ payload",
                       // inputs
                       {TestTable{{}, {}},
                        TestTable{// header
                                  {}, {}}},
                       // outputs
                       {TestTable{// header
                                  {"id", "name", "age", "label"},
                                  {// row
                                   {"3","wangwu","22.987","0"},
                                   // row
                                   {"4","zhaoliu","24.384","0"},
                                   {"5","qianqi","26.254","0"}}}},
                       // keys
                       {{"id"}, {"id1"}},
                       /*disable_alignment = */ false,
                       /*broadcast_result = */ false})));


class PsiDmOdbcTest
    : public testing::TestWithParam<std::tuple<v2::Protocol, TestParams>> {
 protected:
  void SetUp() override { tmp_dir_ = std::filesystem::temp_directory_path(); }
  
  std::vector<std::filesystem::path> GenTempPaths(
      const std::string& name_prefix, int cnt) {
    std::vector<std::filesystem::path> res;
    res.reserve(cnt);
    for (int i = 0; i < cnt; ++i) {
      res.emplace_back(tmp_dir_ / fmt::format("{}-{}", name_prefix, i));
    }
    tmp_paths_.insert(tmp_paths_.end(), res.begin(), res.end());

    return res;
  }

  std::filesystem::path tmp_dir_;
  std::vector<std::filesystem::path> tmp_paths_;
};

TEST_P(PsiDmOdbcTest, Works) {
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("******************* psi dmodbc test begin ****************");
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("**********************************************************");
  auto protocol = std::get<0>(GetParam());
  auto params = std::get<1>(GetParam());

  std::cout << "Test title: " << params.title << std::endl;
  std::cout << "Protocol: " << protocol << std::endl;

  const ::testing::TestInfo* const test_info =
      ::testing::UnitTest::GetInstance()->current_test_info();

  std::string test_suite_name = test_info->test_suite_name();
  std::string test_case_name = test_info->test_case_name();
  std::replace(test_suite_name.begin(), test_suite_name.end(), '/', '_');
  std::replace(test_case_name.begin(), test_case_name.end(), '/', '_');

  boost::uuids::random_generator uuid_generator;
  auto uuid_str = boost::uuids::to_string(uuid_generator());

  std::string test_name =
      test_suite_name + "-" + test_case_name + "-" + uuid_str;

 const std::string& datasource_json = "{\n"
                             "  \"connection_str\": \"DSN=dm;SERVER=172.16.0.217;UID=SYSDBA;PWD=SYSDBA001;TCP_PORT=52360;\",\n"
                             "  \"datasource_kind\": 3,\n"
                             "  \"datasource_kind_sub\": 1,\n"
                             "  \"table_name\": \"CREDIT_ACTIVE000\"\n"
                             "}";

  const std::string& datasource_json1 = "{\n"
                             "  \"connection_str\": \"DSN=dm;SERVER=172.16.0.217;UID=SYSDBA;PWD=SYSDBA001;TCP_PORT=52360;\",\n"
                             "  \"datasource_kind\": 3,\n"
                             "  \"datasource_kind_sub\": 1,\n"
                             "  \"table_name\": \"CREDIT_ACTIVE000\"\n"
                             "}";
  std::vector<std::string> input_paths{datasource_json, datasource_json1};
  
  std::vector<std::filesystem::path> output_paths =
      GenTempPaths(test_name + "-output", 2);

  auto lctxs = yacl::link::test::SetupWorld(2);

  auto proc = [&](int idx) -> PsiResultReport {
    std::cout << "###idx: " << idx << std::endl;
    v2::PsiConfig config;
    config.mutable_input_config()->set_path(input_paths[idx]);
    config.mutable_input_config()->set_type(v2::IO_TYPE_SQL);
    config.mutable_keys()->Add(params.keys[idx].begin(),
                               params.keys[idx].end());
    config.mutable_output_config()->set_path(output_paths[idx]);
    config.mutable_output_config()->set_type(v2::IO_TYPE_FILE_CSV);
    config.set_disable_alignment(params.disable_alignment);
    config.mutable_protocol_config()->set_protocol(protocol);
    if (protocol == v2::PROTOCOL_ECDH) {
      config.mutable_protocol_config()->mutable_ecdh_config()->set_curve(
          CurveType::CURVE_25519);
    }
    config.mutable_protocol_config()->set_broadcast_result(
        params.broadcast_result);
    config.set_advanced_join_type(params.advanced_join_type);
    config.set_left_side(v2::Role::ROLE_RECEIVER);

    std::unique_ptr<AbstractPsiParty> party;
    if (idx == 0) {
      config.mutable_protocol_config()->set_role(v2::Role::ROLE_RECEIVER);
      party = createPsiParty(config, lctxs[idx]);
    } else {
      config.mutable_protocol_config()->set_role(v2::Role::ROLE_SENDER);
      party = createPsiParty(config, lctxs[idx]);
    }

    return party->Run();
  };

  size_t world_size = lctxs.size();
  std::vector<std::future<PsiResultReport>> f_links(world_size);
  for (size_t i = 0; i < world_size; i++) {
    f_links[i] = std::async(proc, i);
  }

  for (size_t i = 0; i < world_size; i++) {
    std::exception_ptr exptr = nullptr;

    PsiResultReport report;
    try {
      report = f_links[i].get();
    } catch (const std::exception& e) {
      exptr = std::current_exception();
      SPDLOG_ERROR("Error from party {}: {}", i, e.what());
    }

    if (exptr) {
      std::rethrow_exception(exptr);
    }

    // if (i == 0 || params.broadcast_result || params.advanced_join_type) {
    //   TestTable output_hat = LoadTableFromFile(output_paths[i].string(),
    //                                            params.outputs[i].headers);

    //   EXPECT_EQ(params.outputs[i].rows, output_hat.rows);
    // }

  }
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("******************* psi dmodbc test end ******************");
  SPDLOG_INFO("**********************************************************");
  SPDLOG_INFO("**********************************************************");
}

INSTANTIATE_TEST_SUITE_P(
    Works_Instances, PsiDmOdbcTest,
    testing::Combine(
        testing::Values(v2::PROTOCOL_ECDH, v2::PROTOCOL_KKRT,v2::PROTOCOL_RR22),
        testing::Values(
                        TestParams{"testcase 4: w/ payload",
                       // inputs
                       {TestTable{{}, {}},
                        TestTable{// header
                                  {}, {}}},
                       // outputs
                       {TestTable{// header
                                  {"ID", "A", "B", "C", "D", "E", "F"},
                                  {// row
                                   {"3","wangwu","22.987","0"},
                                   // row
                                   {"4","zhaoliu","24.384","0"},
                                   {"5","qianqi","26.254","0"}}}},
                       // keys
                       {{"ID"}, {"ID"}},
                       /*disable_alignment = */ false,
                       /*broadcast_result = */ false})));

}  // namespace
}  // namespace psi
