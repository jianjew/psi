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

#include "psi_datasource_operate.h"

#include <iostream>
#include <string>
#include <sstream>
#include <filesystem>
#include <boost/algorithm/string/join.hpp>
#include "boost/uuid/uuid.hpp"
#include "boost/uuid/uuid_generators.hpp"
#include "boost/uuid/uuid_io.hpp"

#include "arrow/api.h"
#include "arrow/array.h"
#include "arrow/compute/api.h"
#include "arrow/datum.h"
#include "arrow/io/api.h"
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "spdlog/spdlog.h"


namespace psi {

PsiDatasourceOperate::PsiDatasourceOperate(const v2::PsiConfig& config) {
  const std::string input_str = config.input_config().path();
  key_columns_.insert(key_columns_.end(), config.keys().begin(), config.keys().end());
  bool check_duplicates_ = config.skip_duplicates_check();
  bool check_hash_digest_ = config.check_hash_digest();
  bool disable_alignment_ = config.disable_alignment();
  // bucket_size_ = config.bucket_size();                    
  rapidjson::Document d;
  d.Parse(input_str.c_str());
  assert(d.IsObject());
  assert(d.HasMember("datasource_kind"));
  datasource_kind_ = (psi::DataSourceKind)d["datasource_kind"].GetInt();
  // 这里的逻辑判断主要是为了兼容框架，框架传了type，自定义的json中有datasource_kind，需要保持一致。
  if (config.input_config().type() == v2::IO_TYPE_FILE_CSV) {
    assert(datasource_kind_ == psi::DataSourceKind::CSVDB);
  }
  if (config.input_config().type() == v2::IO_TYPE_SQL) {
    assert(datasource_kind_ == psi::DataSourceKind::MYSQL || datasource_kind_ == psi::DataSourceKind::POSTGRESQL || datasource_kind_ == psi::DataSourceKind::ODBC);
  }
  
  if (datasource_kind_ == psi::DataSourceKind::ODBC) {
    assert(d.HasMember("datasource_kind_sub"));
    datasource_kind_sub_ = (psi::DataSourceKindSub)d["datasource_kind_sub"].GetInt();
  }
  // 文件类型 
  if (datasource_kind_ == psi::DataSourceKind::CSVDB) {
      assert(d.HasMember("server_file_path"));
      server_file_path_ = d["server_file_path"].GetString();
      // csv_batch_provider_ = std::make_shared<::psi::ArrowCsvBatchProvider>(
      //     server_file_path_, key_columns_, bucket_size_, label_columns_);
  }
  // 数据库类型 
  if (datasource_kind_ == psi::DataSourceKind::MYSQL || datasource_kind_ == psi::DataSourceKind::POSTGRESQL || datasource_kind_ == psi::DataSourceKind::ODBC) {
      assert(d.HasMember("connection_str"));
      assert(d.HasMember("table_name"));
      connection_str_ = d["connection_str"].GetString();
      table_name_ = d["table_name"].GetString();
      // init adaptor_
      DataSource options;
      options.kind = datasource_kind_;
      options.connection_str = connection_str_;
      std::unique_ptr<psi::DatasourceAdaptorMgr> datasourceAdaptorMgr = std::make_unique<psi::DatasourceAdaptorMgr>();
      adaptor_ = datasourceAdaptorMgr->GetAdaptor(options);
  }
  // 其他类型待添加
}

CheckCsvReport PsiDatasourceOperate::CheckDatasource() {
  CheckCsvReport report;
  size_t count = 0;
  switch(datasource_kind_) {
    case DataSourceKind::CSVDB:
      report = CheckCsv(server_file_path_, key_columns_, check_duplicates_, check_hash_digest_);
      break;
    case DataSourceKind::MYSQL:
    case DataSourceKind::POSTGRESQL:
    case DataSourceKind::ODBC:
      std::string query = "SELECT COUNT(*) FROM " + table_name_ + ";";
      SPDLOG_INFO("select count string:{}", query);
      try {
            auto query_result = adaptor_->ExecQuery(query);
            assert(query_result.size() == 1);
            assert(query_result[0]->Length() == 1);
            auto num = query_result[0].get()->ToArrowChunkedArray()->GetScalar(0).ValueOrDie();
            std::stringstream sstream(num->ToString());
            sstream >> count;
      } catch (const std::exception& e) {
          YACL_THROW("CountDataContentNums Error: {}", e.what());
      }
      report.num_rows = count;
      break;
  }
  return report;
}

std::unique_ptr<HashBucketCache> PsiDatasourceOperate::GetDatasouceBatchContent(std::string input_bucket_store_path, size_t bucket_count) {
  SPDLOG_INFO("GetDatasouceBatchContent enter, bucket_count: {}", bucket_count);
  if (input_bucket_store_path.empty()) {
    input_bucket_store_path = std::filesystem::path(server_file_path_).parent_path();
  }
  std::unique_ptr<HashBucketCache> hash_bucket_cache;
  switch(datasource_kind_) {
    case DataSourceKind::CSVDB:
      hash_bucket_cache = CreateCacheFromCsv(server_file_path_, key_columns_, input_bucket_store_path, bucket_count);
      break;
    case DataSourceKind::MYSQL:
    case DataSourceKind::POSTGRESQL:
    case DataSourceKind::ODBC:
      hash_bucket_cache = GetTableContent(input_bucket_store_path, bucket_count);
      break;
  }
  return hash_bucket_cache;
}

// todo 这里如何分页，是否需要分页， bigdata咋搞
std::unique_ptr<HashBucketCache> PsiDatasourceOperate::GetTableContent(const std::string& cache_dir, uint32_t bucket_num, uint32_t read_batch_size,
    bool use_scoped_tmp_dir) {
  SPDLOG_INFO("###CreateCacheFromCsv, cache_dir: {}, bucket_num: {}, read_batch_size: {}, use_scoped_tmp_dir: {}",
    cache_dir, bucket_num, read_batch_size, use_scoped_tmp_dir);
  for (auto& schema: key_columns_) {
    SPDLOG_INFO("###CreateCacheFromCsv, schema: {}", schema);
  }
  auto bucket_cache = std::make_unique<HashBucketCache>(cache_dir, bucket_num, use_scoped_tmp_dir);
  std::vector<std::string> select_query(key_columns_.begin(), key_columns_.end());
  auto query_join = boost::algorithm::join(select_query, ",");
  std::string query;
  switch(datasource_kind_) {
    case DataSourceKind::MYSQL:
      query = "SELECT " + query_join + " FROM " + table_name_ + ";";
      break;
    case DataSourceKind::POSTGRESQL:
      query = "SELECT " + query_join + " FROM " + table_name_ + ";";
      break;
    case DataSourceKind::ODBC:
      switch (datasource_kind_sub_) {
        case DataSourceKindSub::POSTGRESQL_ODBC:
          query = "SELECT " + query_join + " FROM " + table_name_ + ";";
          break;
        case DataSourceKindSub::DAMENG_ODBC:
          query = "SELECT " + query_join + " FROM " + table_name_ + ";";
          break;
        default:
          YACL_THROW("unsupported datasource kind sub.");
      }
      break;
    default:
          YACL_THROW("unsupported datasource kind.");
  }
  SPDLOG_INFO("select items string:{}", query);
  try {
    auto query_result = adaptor_->ExecQuery(query);
    size_t num_rows = query_result[0]->Length();
    SPDLOG_INFO("batch num_rows: {}, key_columns_ size: {}", num_rows, key_columns_.size());
    std::vector<std::shared_ptr<arrow::StringArray>> arrays;
    arrays.clear();
    for (int i = 0; i < query_result.size(); i++) {
        std::shared_ptr<arrow::ChunkedArray> chunked_array = query_result[i]->ToArrowChunkedArray();
        arrays.emplace_back(std::static_pointer_cast<arrow::StringArray>(chunked_array->chunk(0)));
    }
    for (int64_t idx_in_batch = 0; idx_in_batch < num_rows; idx_in_batch++) {
      {
        std::vector<std::string> values;
        for (size_t i = 0; i < key_columns_.size(); i++) {
          if (arrays[i]->type()->id() == arrow::Type::STRING || arrays[i]->type()->id() == arrow::Type::LARGE_STRING) {
            std::string tmp_str = arrays[i]->GetScalar(idx_in_batch).ValueOrDie()->ToString();
            int start_idx = tmp_str.find_first_of('"') + 1;
            int end_idx = tmp_str.find_last_of('"');
            tmp_str = tmp_str.substr(start_idx, end_idx - start_idx);
            values.emplace_back(tmp_str);
          } else {
            values.emplace_back(arrays[i]->GetScalar(idx_in_batch).ValueOrDie()->ToString());
          }
        }
        auto str_join = boost::algorithm::join(values, ",");
        bucket_cache->WriteItem(str_join);
        // SPDLOG_INFO("###CreateCacheFromCsv, it: {}", str_join);
      }
    }
    bucket_cache->Flush();
  } catch (const std::exception& e) {
      YACL_THROW("GetTableBatchContent Error: {}", e.what());
  }

  return bucket_cache;
}

size_t PsiDatasourceOperate::PsiGenerateResult(const std::string& output_path, std::filesystem::path indices, bool sort_output, bool digest_equal, bool output_difference) {
  SPDLOG_INFO("###GenerateResult enter, sort_output: {}, indices: {}, digest_equal: {}", sort_output, indices.string(), digest_equal);
  size_t count = 0;
  switch(datasource_kind_) {
    case DataSourceKind::CSVDB:
      count = GenerateResult(server_file_path_, output_path, key_columns_, indices, sort_output, digest_equal, output_difference);
      break;
    case DataSourceKind::MYSQL:
    case DataSourceKind::POSTGRESQL:
    case DataSourceKind::ODBC:
      count = GenerateResultInner(output_path, indices, sort_output, digest_equal, output_difference);
      break;
  }
  SPDLOG_INFO("###GenerateResult result count: {}", count);
  return count;
}

template <typename T>
size_t PsiDatasourceOperate::GenerateResultInner(const std::string& output_path, const T& indices, bool sort_output, bool digest_equal, bool output_difference) {
  // use tmp file to avoid `shell Injection`
  boost::uuids::random_generator uuid_generator;
  auto uuid_str = boost::uuids::to_string(uuid_generator());
  auto tmp_sort_in_file = std::filesystem::path(output_path)
                              .parent_path()
                              .append(fmt::format("tmp-sort-in-{}", uuid_str));
  auto tmp_sort_out_file =
      std::filesystem::path(output_path)
          .parent_path()
          .append(fmt::format("tmp-sort-out-{}", uuid_str));
  // register remove of temp file.
  ON_SCOPE_EXIT([&] {
    std::error_code ec;
    std::filesystem::remove(tmp_sort_out_file, ec);
    if (ec.value() != 0) {
      SPDLOG_WARN("can not remove tmp file: {}, msg: {}",
                  tmp_sort_out_file.c_str(), ec.message());
    }
    std::filesystem::remove(tmp_sort_in_file, ec);
    if (ec.value() != 0) {
      SPDLOG_WARN("can not remove tmp file: {}, msg: {}",
                  tmp_sort_in_file.c_str(), ec.message());
    }
  });

  size_t cnt = FilterFileByIndicesInner(tmp_sort_in_file, indices, output_difference);
  SPDLOG_INFO("###GenerateResultInner:cnt: {}", cnt);

  if (sort_output && !digest_equal) {
    SPDLOG_INFO("###GenerateResultInner:cnt 00000");
    MultiKeySort(tmp_sort_in_file, tmp_sort_out_file, key_columns_);
    SPDLOG_INFO("###GenerateResultInner:cnt 11111");
    std::filesystem::rename(tmp_sort_out_file, output_path);
  } else {
    SPDLOG_INFO("###GenerateResultInner:cnt 22222");
    std::filesystem::rename(tmp_sort_in_file, output_path);
  }

  return cnt;
}

size_t PsiDatasourceOperate::FilterFileByIndicesInner(const std::string& output, const std::filesystem::path& indices, bool output_difference) {
  SPDLOG_INFO("###FilterFileByIndicesInner:output: {}, indices: {}, output_difference: {}", output,indices.string(), output_difference);
  auto out = io::BuildOutputStream(io::FileIoOptions(output));

  std::string line;
  size_t idx = 0;
  size_t actual_count = 0;
  IndexReader reader(indices);

  std::optional<uint64_t> intersection_index = reader.GetNext();
  std::string queryColumnNames;
  switch(datasource_kind_) {
    case DataSourceKind::MYSQL:
    case DataSourceKind::POSTGRESQL:
      queryColumnNames = "SELECT column_name FROM information_schema.columns WHERE table_name = '"  + table_name_ + "' ORDER BY ordinal_position;";
      break;
    case DataSourceKind::ODBC:
      switch (datasource_kind_sub_) {
        case DataSourceKindSub::POSTGRESQL_ODBC:
          queryColumnNames = "SELECT column_name FROM information_schema.columns WHERE table_name = '"  + table_name_ + "' ORDER BY ordinal_position;";
          break;
        case DataSourceKindSub::DAMENG_ODBC:
          queryColumnNames = "select COLUMN_NAME from all_tab_columns where Table_Name = '"  + table_name_ + "';";
          break;
        default:
          YACL_THROW("unsupported datasource kind sub.");
      }
      break;
    default:
          YACL_THROW("unsupported datasource kind.");
  }
  
  SPDLOG_INFO("queryColumnNames: {}", queryColumnNames);

  std::string query = "SELECT * FROM " + table_name_ + ";";
  try {
    // step0: get all column names
    auto query_culumn_name_result = adaptor_->ExecQuery(queryColumnNames);
    SPDLOG_INFO("query_culumn_name_result num_rows: {}, num_columns: {}", query_culumn_name_result[0]->Length(), query_culumn_name_result.size());
    auto culumn_name_result = std::static_pointer_cast<arrow::StringArray>(query_culumn_name_result[0]->ToArrowChunkedArray()->chunk(0));
    std::vector<std::string> culumn_name_values;
    for (size_t i = 0; i < query_culumn_name_result[0]->Length(); i++) {
      if (culumn_name_result->type()->id() == arrow::Type::STRING || culumn_name_result->type()->id() == arrow::Type::LARGE_STRING) {
        std::string tmp_str = culumn_name_result->GetScalar(i).ValueOrDie()->ToString();
        int start_idx = tmp_str.find_first_of('"') + 1;
        int end_idx = tmp_str.find_last_of('"');
        tmp_str = tmp_str.substr(start_idx, end_idx - start_idx);
        culumn_name_values.emplace_back(tmp_str);
      } else {
        culumn_name_values.emplace_back(culumn_name_result->GetScalar(i).ValueOrDie()->ToString());
      }
    }
    auto culumn_name_values_join = boost::algorithm::join(culumn_name_values, ",");
    SPDLOG_INFO("###culumn_name_values_join: {}", culumn_name_values_join);
    out->Write(culumn_name_values_join);
    out->Write("\n");

    // step1: get all data
    auto query_result = adaptor_->ExecQuery(query);
    size_t num_rows = query_result[0]->Length();
    SPDLOG_INFO("batch num_rows: {}, num_columns: {}", num_rows, query_result.size());
    std::vector<std::shared_ptr<arrow::StringArray>> arrays;
    arrays.clear();
    for (int i = 0; i < query_result.size(); i++) {
        std::shared_ptr<arrow::ChunkedArray> chunked_array = query_result[i]->ToArrowChunkedArray();
        arrays.emplace_back(std::static_pointer_cast<arrow::StringArray>(chunked_array->chunk(0)));
    }
    
    for (int64_t idx_in_batch = 0; idx_in_batch < num_rows; idx_in_batch++) {
      {
        std::vector<std::string> values;
        for (size_t i = 0; i < query_result.size(); i++) {
          if (arrays[i]->type()->id() == arrow::Type::STRING || arrays[i]->type()->id() == arrow::Type::LARGE_STRING) {
            std::string tmp_str = arrays[i]->GetScalar(idx_in_batch).ValueOrDie()->ToString();
            int start_idx = tmp_str.find_first_of('"') + 1;
            int end_idx = tmp_str.find_last_of('"');
            tmp_str = tmp_str.substr(start_idx, end_idx - start_idx);
            values.emplace_back(tmp_str);
          } else {
            values.emplace_back(arrays[i]->GetScalar(idx_in_batch).ValueOrDie()->ToString());
          }
        }
        auto str_join = boost::algorithm::join(values, ",");
        
        // step2: select by index
        SPDLOG_INFO("###FilterFileByIndicesInner:str_join: {}, indx: {}", str_join, idx);
        if (!output_difference) {
          if (!intersection_index.has_value()) {
            break;
          }
        }

        if ((intersection_index.has_value() &&
              intersection_index.value() == idx) !=
            output_difference) {
          out->Write(str_join);
          out->Write("\n");
          actual_count++;
        }

        if (intersection_index.has_value() &&
            intersection_index.value() == idx) {
          intersection_index = reader.GetNext();
        }
        idx++;
      }
    }
  } catch (const std::exception& e) {
      YACL_THROW("GetTableBatchContent Error: {}", e.what());
  }


  size_t target_count =
      (output_difference ? (idx - reader.read_cnt())
                         : reader.read_cnt());

  out->Close();

  return reader.read_cnt();
}

}  // namespace psi