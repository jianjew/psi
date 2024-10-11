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

#include <cstddef>
#include <string>
#include <vector>
#include <curl/curl.h>

#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"

namespace psi {

class ApiReader {
 public:
  explicit ApiReader(const std::string& url, const std::vector<std::string>& keys, const std::vector<std::string>& labels = {});

  size_t GetApiDataCount();

  [[nodiscard]] size_t row_cnt() const { return row_cnt_; }

  std::pair<std::vector<std::string>, std::vector<std::string>> GetApiBatchContent(size_t current_patch, size_t batch_size, bool batch_read = true);

  std::vector<std::string> GetApiAllContent() const { return std::move(api_data_all_); }

 private:
  bool DoGetRequset(std::string& response);

  void ParseJsonDataFromGetRequest();

  std::string GetPieceData(const rapidjson::Value& res_data, std::vector<std::string>& keys, int index);

 private:
  std::string url_;

  std::vector<std::string> key_columns_;

  std::vector<std::string> label_columns_;

  size_t row_cnt_ = 0;

  CURL* curl_ = nullptr;

  std::pair<std::vector<std::string>, std::vector<std::string>> api_data_;  // 仅保存匹配的ids和labels数据

  std::vector<std::string> api_data_all_;  // 保存整个文件内容（包括列名）
};

}  // namespace psi
