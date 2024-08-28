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

#pragma once

#include <iostream>
#include <string>

#include "psi/datasource/datasource_base.h"
#include "psi/datasource/datasource_adaptor_mgr.h"
#include "psi/utils/arrow_csv_batch_provider.h"
#include "psi/proto/pir.pb.h"

namespace psi {

/**
 * Provide information on obtaining data sources in practical scenarios.
 * 
 * author: jianjew
 */
class DatasourceOperate {
 public:
  DatasourceOperate(const PirServerConfig &config);

  size_t CountDataContentNums();

  std::pair<std::vector<std::string>, std::vector<std::string>> GetDatasouceBatchContent();

 private:
  std::string connection_str_;
  DataSourceKind datasource_kind_;
  DataSourceKindSub datasource_kind_sub_;
  std::string table_name_;
  std::string server_file_path_;
  std::vector<std::string> key_columns_;
  std::vector<std::string> label_columns_;
  size_t bucket_size_;
  std::shared_ptr<DatasourceAdaptor> adaptor_;
  std::shared_ptr<::psi::ILabeledBatchProvider> csv_batch_provider_;
};

}  // namespace psi