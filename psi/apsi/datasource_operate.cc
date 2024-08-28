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

#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"

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
      std::stringstream ss;
      std::copy(key_columns_.begin(), key_columns_.end(), std::ostream_iterator<std::string>(ss, " AND "));
      std::string concat_str = std::move(ss.str());
      std::string query = "SELECT COUNT(*) FROM table_name_ WHERE " + concat_str + ";";
      std::cout << query << std::endl;
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
      // DataSource options;
      // options.kind = datasource_kind_;
      // options.connection_str = connection_str_;
      // std::stringstream ss;
      // std::copy(key_columns_.begin(), key_columns_.end(), std::ostream_iterator<std::string>(ss, " AND "));
      // std::string concat_str = std::move(ss.str());
      // str.pop_back();
      
      try {
            // auto results = adaptor->ExecQuery(query);
            // std::cout << "tensor size: " << results.size() << std::endl;
            // count = result;
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
  }
  return data_batch_content;
}

}  // namespace psi