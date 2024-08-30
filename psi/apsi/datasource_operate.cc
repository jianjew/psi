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

#include "datasource_operate.h"

#include <iostream>
#include <string>
#include <sstream>
#include <boost/algorithm/string/join.hpp>

#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "arrow/api.h"
#include "arrow/compute/api.h"
#include "arrow/array.h"
#include "arrow/datum.h"
#include "arrow/io/api.h"


namespace psi {

size_t CsvFileDataCount(const std::string &file_path,
                        const std::vector<std::string> &ids) {
  size_t data_count = 0;

  std::shared_ptr<::psi::IBasicBatchProvider> batch_provider =
      std::make_shared<::psi::ArrowCsvBatchProvider>(file_path, ids, 4096);

  while (true) {
    auto batch = batch_provider->ReadNextBatch();
    if (batch.empty()) {
      break;
    }
    data_count += batch.size();
  }

  return data_count;
}

DatasourceOperate::DatasourceOperate(const PirServerConfig &config) {
  const std::string input_str = config.input_path();
  key_columns_.insert(key_columns_.end(), config.key_columns().begin(),
                     config.key_columns().end());
  label_columns_.insert(label_columns_.end(), config.label_columns().begin(),
                       config.label_columns().end());
  bucket_size_ = config.bucket_size();                    
  rapidjson::Document d;
  d.Parse(input_str.c_str());
  assert(d.IsObject());
  assert(d.HasMember("datasource_kind"));
  datasource_kind_ = (psi::DataSourceKind)d["datasource_kind"].GetInt();
  // 文件类型 
  if (datasource_kind_ == psi::DataSourceKind::CSVDB) {
      assert(d.HasMember("server_file_path"));
      server_file_path_ = d["server_file_path"].GetString();
      csv_batch_provider_ = std::make_shared<::psi::ArrowCsvBatchProvider>(
          server_file_path_, key_columns_, bucket_size_, label_columns_);
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


size_t DatasourceOperate::CountDataContentNums() {
  size_t count = 0;
  switch(datasource_kind_) {
    case DataSourceKind::CSVDB:
      count = CsvFileDataCount(server_file_path_, key_columns_);
      break;
    case DataSourceKind::MYSQL:
    case DataSourceKind::POSTGRESQL:
    case DataSourceKind::ODBC:
      std::string query = "SELECT COUNT(*) FROM " + table_name_ + ";";
      std::cout << query << std::endl;
      try {
            auto query_result = adaptor_->ExecQuery(query);
            assert(query_result.size() == 1);
            assert(query_result[0]->Length() == 1);
            auto num = query_result[0].get()->ToArrowChunkedArray()->GetScalar(0).ValueOrDie();
            std::stringstream sstream(num->ToString());
            sstream >> count;
      } catch (const std::exception& e) {
            std::cout << "捕获到异常: " << e.what() << std::endl;
      }
      break;
  
  }
  return count;
}

std::pair<std::vector<std::string>, std::vector<std::string>> DatasourceOperate::GetDatasouceBatchContent() {
  std::cout << "GetDatasouceBatchContent: " << std::endl;
  std::pair<std::vector<std::string>, std::vector<std::string>> data_batch_content;
  switch(datasource_kind_) {
    case DataSourceKind::CSVDB:
      data_batch_content = csv_batch_provider_->ReadNextLabeledBatch();
      break;
    case DataSourceKind::MYSQL:
    case DataSourceKind::POSTGRESQL:
    case DataSourceKind::ODBC:
      data_batch_content = GetTableBatchContent();
      break;

  }
  return data_batch_content;
}

std::pair<std::vector<std::string>, std::vector<std::string>> DatasourceOperate::GetTableBatchContent() {
  std::vector<std::string> read_keys;
  std::vector<std::string> read_labels;
  std::vector<std::string> select_query(key_columns_.begin(), key_columns_.end());
  select_query.insert(select_query.end(), label_columns_.begin(), label_columns_.end());
  auto query_join = boost::algorithm::join(select_query, ",");
  std::string query = "SELECT " + query_join + " FROM " + table_name_ + ";";
  std::cout << query << std::endl;
  try {
        auto query_result = adaptor_->ExecQuery(query);
        size_t num_rows = query_result[0]->Length();
        std::cout << "batch num_rows: " << num_rows << std::endl;
        std::vector<std::shared_ptr<arrow::StringArray>> arrays;
        arrays.clear();
        for (int i = 0; i < query_result.size(); i++) {
            std::shared_ptr<arrow::ChunkedArray> chunked_array = query_result[i]->ToArrowChunkedArray();
            arrays.emplace_back(std::static_pointer_cast<arrow::StringArray>(chunked_array->chunk(0)));
        }

        // for (auto arry : arrays) {
        //   std::cout << "arry: " << arry->ToString() << std::endl;
        // }
        // std::cout << "arrays[i]->Value(idx_in_batch): " << arrays[0]->GetScalar(2).ValueOrDie()->ToString() << std::endl;
        // std::cout << "arrays[i]->Value(idx_in_batch): " << arrays[2]->GetScalar(2).ValueOrDie()->ToString() << std::endl;
        // std::cout << "arrays[i]->Value(idx_in_batch): " << arrays[1]->GetScalar(2).MoveValueUnsafe()->ToString() << std::endl;
        // //std::cout << "arrays[i]->Value(idx_in_batch): " << arrays[1]->View(2) << std::endl;
        
        std::cout << "key_columns_ size: " << key_columns_.size() << std::endl;
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
            read_keys.emplace_back(str_join);
          }
  
          {
            std::vector<std::string> values;
            for (size_t i = key_columns_.size(); i < query_result.size(); i++) {
              if (arrays[i]->type()->id() == arrow::Type::STRING || arrays[i]->type()->id() == arrow::Type::LARGE_STRING) {
                std::string tmp_str = arrays[i]->GetScalar(idx_in_batch).ValueOrDie()->ToString();
                int start_idx = tmp_str.find_first_of('"') + 1;
                int end_idx = tmp_str.find_last_of('"');
                absl::string_view view = tmp_str.substr(start_idx, end_idx - start_idx);
                values.emplace_back(view);
              } else {
                values.emplace_back(arrays[i]->GetScalar(idx_in_batch).ValueOrDie()->ToString());
              }
            }
            auto str_join = boost::algorithm::join(values, ",");
            read_labels.emplace_back(str_join);
          }
        }
        
        return std::make_pair(read_keys, read_labels);
      
  } catch (const std::exception& e) {
        std::cout << "捕获到异常: " << e.what() << std::endl;
  }
  
}

}  // namespace psi