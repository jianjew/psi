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

#include <memory>
#include "type.h"

namespace psi {

// TODO(shunde.csd): try to simplify the following data type conversion via
// macro instructions.

PrimitiveDataType FromArrowDataType(
    const std::shared_ptr<arrow::DataType>& dtype) {
  PrimitiveDataType ty;
  switch (dtype->id()) {
    case arrow::Type::BOOL:
      ty = PrimitiveDataType::BOOL;
      break;
    case arrow::Type::UINT8:
    case arrow::Type::INT8:
    case arrow::Type::UINT16:
    case arrow::Type::INT16:
    case arrow::Type::INT32:
      ty = PrimitiveDataType::INT32;
      break;
    case arrow::Type::UINT32:
    case arrow::Type::INT64:
    case arrow::Type::UINT64:
      ty = PrimitiveDataType::INT64;
      break;
    case arrow::Type::FLOAT:
      ty = PrimitiveDataType::FLOAT32;
      break;
    case arrow::Type::DOUBLE:
      ty = PrimitiveDataType::FLOAT64;
      break;
    case arrow::Type::DECIMAL128: {
      auto decimal_type =
          std::dynamic_pointer_cast<arrow::Decimal128Type>(dtype);
      if (decimal_type->scale() == 0) {
        ty = PrimitiveDataType::INT64;
      } else {
        ty = PrimitiveDataType::PrimitiveDataType_UNDEFINED;
      }
      break;
    }
    case arrow::Type::STRING:
    case arrow::Type::LARGE_STRING:
      ty = PrimitiveDataType::STRING;
      break;
    default:
      ty = PrimitiveDataType::PrimitiveDataType_UNDEFINED;
  }
  return ty;
}

std::shared_ptr<arrow::DataType> ToArrowDataType(PrimitiveDataType dtype) {
  std::shared_ptr<arrow::DataType> dt;
  switch (dtype) {
    case PrimitiveDataType::INT8:
      dt = arrow::int8();
      break;
    case PrimitiveDataType::INT16:
      dt = arrow::int16();
      break;
    case PrimitiveDataType::INT32:
      dt = arrow::int32();
      break;
    case PrimitiveDataType::INT64:
      dt = arrow::int64();
      break;
    case PrimitiveDataType::BOOL:
      dt = arrow::boolean();
      break;
    case PrimitiveDataType::FLOAT32:
      dt = arrow::float32();
      break;
    case PrimitiveDataType::FLOAT64:
      dt = arrow::float64();
      break;
    case PrimitiveDataType::STRING:
      dt = arrow::large_utf8();
      break;
    case PrimitiveDataType::DATETIME:
    case PrimitiveDataType::TIMESTAMP:
      dt = arrow::int64();
      break;
    default:
      dt = nullptr;
  }
  return dt;
}

}  // namespace spi