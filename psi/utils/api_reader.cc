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

#include "psi/utils/api_reader.h"

#include <iostream>
#include <string>
#include <vector>
#include <boost/algorithm/string/join.hpp>

#include "spdlog/spdlog.h"
#include "yacl/base/exception.h"

namespace psi {

ApiReader::ApiReader(const std::string& url, const std::vector<std::string>& keys, const std::vector<std::string>& labels) {
  url_ = url;
  key_columns_.insert(key_columns_.end(), keys.begin(), keys.end());
  label_columns_.insert(label_columns_.end(), labels.begin(), labels.end());
  curl_global_init(CURL_GLOBAL_ALL);
  curl_ = curl_easy_init();
}

std::pair<std::vector<std::string>, std::vector<std::string>> ApiReader::GetApiBatchContent(size_t current_patch, size_t batch_size, bool batch_read) {
  std::vector<std::string> read_keys;
  std::vector<std::string> read_labels;
  if (api_data_.first.empty() || api_data_.second.empty()) {
    ParseJsonDataFromGetRequest();
    if (api_data_.first.empty() || api_data_.second.empty()) {
      YACL_THROW("api_data_ is empty.");
    }
  }
  if (batch_read) {
    int start_point = current_patch * batch_size;
    int end_point = start_point + batch_size > row_cnt_ ? row_cnt_ : start_point + batch_size;
    read_keys.assign(api_data_.first.begin() + start_point, api_data_.first.begin() + end_point);
    read_labels.assign(api_data_.second.begin() + start_point, api_data_.second.begin() + end_point);
  } else {
    read_keys = std::move(api_data_.first);
    read_labels = std::move(api_data_.second);
  }

  return std::make_pair(read_keys, read_labels);
}

size_t ApiReader::GetApiDataCount() {
  ParseJsonDataFromGetRequest();
  return row_cnt();
}

/**
 * Json is like this:
 * {
 *  "code": 200,
 *  "data": {
 *      "apiData": [
 *          {
 *            "id": 100,
 *            "f0": 0,
 *            "f1": 1,
 *            "f2": 2,
 *            "f3": 3,
 *            "f4": 4
 *          }
 *      ],
 *      "params": "id>1"
 *    },
 *  "message": "成功",
 *  "success": true
 *  }
 */
void ApiReader::ParseJsonDataFromGetRequest() {
  // Step0: get request to obtain data
  std::string response;
  if (!DoGetRequset(response)) {
    YACL_THROW("DoGetRequset failed");
  }

  // Step1: parse json string
  rapidjson::Document d;
  d.Parse(response.c_str());
  if (d.HasParseError()) {
    YACL_THROW("Parse json failed.");
  }
  if (!d.HasMember("code") || !d.HasMember("data")) {
    YACL_THROW("Json error: do not contain necessary member.");
  }
  
  if (d["code"].GetInt() != 200) {
    YACL_THROW("Json error: code is not 200.");
  }
  const rapidjson::Value& res_data = d["data"];
  if (!res_data.HasMember("apiData") || !res_data["apiData"].IsArray()) {
    YACL_THROW("Json error: do not contain apiData array member.");
  }
  auto api_data = res_data["apiData"].GetArray();
  if (api_data.Size() < 1) {
    YACL_THROW("dataset length is 0, please check dataset.");
  }

  // Step2: Divide the data into two groups: read_keys and read_labels
  std::vector<std::string> read_keys;
  std::vector<std::string> read_labels;
  std::cout << "size: " << api_data.Size() << std::endl;
  std::vector<std::string> all_column_name;
  for (size_t  i = 0; i < api_data.Size(); i++) {
    auto str_join = GetPieceData(res_data, key_columns_, i);
    std::cout << "key str_join: " << str_join << std::endl;
    read_keys.emplace_back(str_join);

    str_join = GetPieceData(res_data, label_columns_, i);
    std::cout << "label str_join: " << str_join << std::endl;
    read_labels.emplace_back(str_join);

    // 这里保存整个文件内容（包括列名
    if (i == 0) {
      for (rapidjson::Value::ConstMemberIterator iter = api_data[0].MemberBegin(); iter != api_data[0].MemberEnd(); iter++) {
        rapidjson::Value jKey;
        rapidjson::Document::AllocatorType allocator;
        jKey.CopyFrom(iter->name, allocator);
        if (jKey.IsString()) {
          std::string column_name = jKey.GetString();
          all_column_name.emplace_back(column_name);
        } 
      }
      std::string concat_column_name = boost::algorithm::join(all_column_name, ",");
      std::cout << "concat_column_name: " << concat_column_name << std::endl;
      api_data_all_.emplace_back(concat_column_name);
    }
    str_join = GetPieceData(res_data, all_column_name, i);
    std::cout << "all str_join: " << str_join << std::endl;
    api_data_all_.emplace_back(str_join);
  }
  row_cnt_ = read_keys.size();
  if (row_cnt_ == 0) {
    YACL_THROW("dataset length is 0, please check dataset.");
  }
  api_data_ = std::move(std::make_pair(read_keys, read_labels));
}

std::string ApiReader::GetPieceData(const rapidjson::Value& res_data, std::vector<std::string>& keys, int index) {
  auto api_data = res_data["apiData"].GetArray();  // 放在函数外面传值有点问题，就放到函数里面了，有点丑！！！
  std::vector<std::string> vec;
  std::string str;
  for (const std::string& key : keys) {
    if (api_data[index].HasMember(key.c_str())) {
      if(api_data[index][key.c_str()].IsString()) {
        str = api_data[index][key.c_str()].GetString();
      } else if (api_data[index][key.c_str()].IsInt()) {
        str = std::to_string(api_data[index][key.c_str()].GetInt());
      } else if (api_data[index][key.c_str()].IsUint()) {
        str = std::to_string(api_data[index][key.c_str()].GetUint());
      } else if (api_data[index][key.c_str()].IsInt64()) {
        str = std::to_string(api_data[index][key.c_str()].GetInt64());
      } else if (api_data[index][key.c_str()].IsUint64()) {
        str = std::to_string(api_data[index][key.c_str()].GetUint64());
      } else if (api_data[index][key.c_str()].IsDouble()) {
        str = std::to_string(api_data[index][key.c_str()].GetDouble());
      } else if (api_data[index][key.c_str()].IsTrue()) {
        str = std::to_string(1);
      } else if (api_data[index][key.c_str()].IsFalse()) {
        str = std::to_string(0);
      }
      vec.emplace_back(str);
    }
  }
  return boost::algorithm::join(vec, ",");
}

size_t WriteCallback(void *buffer, size_t size, size_t nmemb, void *userp){
    char *d = (char*)buffer;
    std::string *b = (std::string*)(userp);
    int result = 0;
    if (b != NULL){
        b->append(d, size * nmemb);
        result = size * nmemb;
    }
    return result;
}

bool ApiReader::DoGetRequset(std::string& response) {
  if (!curl_) {
    std::cout << "curl_ is nullptr" << std::endl;
    return false;
  }
  CURLcode ret_code;
  struct curl_slist *headers = NULL;
  headers = curl_slist_append(headers, "Content-Type: application/json");

  curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, headers); 
  curl_easy_setopt(curl_, CURLOPT_URL, url_.c_str());  
  curl_easy_setopt(curl_, CURLOPT_CUSTOMREQUEST, "GET");
  // 写入回调函数
	curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, WriteCallback);
  curl_easy_setopt(curl_, CURLOPT_WRITEDATA, &response);
  curl_easy_setopt(curl_, CURLOPT_TIMEOUT, 5);

  ret_code = curl_easy_perform(curl_);
  curl_slist_free_all(headers);
  curl_easy_cleanup(curl_);
  if (CURLE_OK != ret_code) {
		return false;
	}
  return true;
}

}  // namespace psi