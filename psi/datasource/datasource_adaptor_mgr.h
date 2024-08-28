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

#include <memory>
#include <map>
#include "datasource_adaptor.h"
#include "datasource_base.h"
#include "datasource_adaptor_factory.h"

namespace psi {
/// @brief Datasource Adaptor Manager
class DatasourceAdaptorMgr {
 public:
  DatasourceAdaptorMgr();

  /// @brief Returns adaptor if it already existed,
  /// otherwise, create adaptor according DataSource spec.
  std::shared_ptr<DatasourceAdaptor> GetAdaptor(
      const DataSource& datasource_spec);

 private:
  void RegisterBuiltinAdaptorFactories();

  std::shared_ptr<DatasourceAdaptor> CreateAdaptor(
      const DataSource& datasource_spec);

    std::map<DataSourceKind, std::shared_ptr<DatasourceAdaptorFactory>>
      factory_maps_;

//  absl::Mutex mu_;
  // following member variables are protected by `mu_`

  // datasource.connection_str + datasource.kind --> datasource adaptor
  std::map<std::pair<std::string, int>,
                      std::shared_ptr<DatasourceAdaptor>>
      adaptors_;
};

}  // namespace psi