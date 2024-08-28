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

#include "arrow/type.h"
#include "datasource_base.h"

namespace psi {

enum class Visibility {
  Unknown = 0,
  Private,
  Public,
  Secret,
};

/// @returns PrimitiveDataType::PrimitiveDataType_UNDEFINED
/// if @param[in] dtype is not supported
PrimitiveDataType FromArrowDataType(
    const std::shared_ptr<arrow::DataType>& dtype);

///// @brief convert scql primitive data type to arrow data type
///// @returns nullptr if @param[in] dtype is not supported
std::shared_ptr<arrow::DataType> ToArrowDataType(PrimitiveDataType dtype);

}  // namespace psi