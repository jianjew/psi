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
#include "psi/utils/csv_checker.h"
#include "psi/utils/hash_bucket_cache.h"
#include "psi/legacy/bucket_psi.h"
#include "psi/utils/ec_point_store.h"
#include "psi/ecdh/ecdh_psi.h"

#include "psi/proto/psi_v2.pb.h"


namespace psi {
/**
 * Provide information on obtaining data sources in practical scenarios.
 * 
 * author: jianjew
 */
class PsiDatasourceOperate {
 public:
  PsiDatasourceOperate(const v2::PsiConfig& config);

  CheckCsvReport CheckDatasource();

  std::unique_ptr<HashBucketCache> GetDatasouceBatchContent(std::string input_bucket_store_path, size_t bucket_count);

  void RunEcdhPsiDatasource(struct psi::ecdh::EcdhPsiOptions& options, std::shared_ptr<HashBucketEcPointStore> self_ec_point_store,
   std::shared_ptr<HashBucketEcPointStore> peer_ec_point_store);
  
  size_t GetEcdhPsiDataSize();

  size_t PsiGenerateResult(const std::string& output_path, std::filesystem::path indices, bool sort_output, bool digest_equal, bool output_difference = false);

 private:
  std::unique_ptr<HashBucketCache> GetTableContent(const std::string& cache_dir, uint32_t bucket_num, uint32_t read_batch_size = 4096, bool use_scoped_tmp_dir  = true);

  template <typename T>
  size_t GenerateResultInner(const std::string& output_path, const T& indices, bool sort_output, bool digest_equal, bool output_difference = false);

  size_t FilterFileByIndicesInner(const std::string& output, const std::filesystem::path& indices, bool output_difference);
  
  void RunEcdhPsiInner(struct psi::ecdh::EcdhPsiOptions& options, const std::shared_ptr<IEcPointStore>& self_ec_point_store,
    const std::shared_ptr<IEcPointStore>& peer_ec_point_store);

 private:
  std::string connection_str_;
  DataSourceKind datasource_kind_;
  DataSourceKindSub datasource_kind_sub_;
  std::string table_name_;
  std::string server_file_path_;
  std::vector<std::string> key_columns_;
  size_t bucket_size_;
  std::shared_ptr<DatasourceAdaptor> adaptor_;
  std::shared_ptr<::psi::ILabeledBatchProvider> csv_batch_provider_;
  bool check_duplicates_;
  bool check_hash_digest_;
  bool disable_alignment_;
};

}  // namespace psi