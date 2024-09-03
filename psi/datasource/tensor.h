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
#include "arrow/chunked_array.h"

namespace psi {

/// @brief A Tensor reprensents a column of a relation
class Tensor {
 public:
  explicit Tensor(std::shared_ptr<arrow::ChunkedArray> chunked_arr);

  Tensor(const Tensor&) = delete;
  Tensor& operator=(const Tensor&) = delete;

  [[nodiscard]] int64_t Length() const { return chunked_arr_->length(); }

  // return the total number of nulls
  [[nodiscard]] int64_t GetNullCount() const { return chunked_arr_->null_count(); }

  /// @returns as arrow chunked array
  [[nodiscard]] std::shared_ptr<arrow::ChunkedArray> ToArrowChunkedArray() const {
    return chunked_arr_;
  }

 protected:
  std::shared_ptr<arrow::ChunkedArray> chunked_arr_;
  int dtype_;
};

using TensorPtr = std::shared_ptr<Tensor>;

}  // namespace psi