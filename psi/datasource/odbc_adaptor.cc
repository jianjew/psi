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

#include "odbc_adaptor.h"

#include <mutex>
#include <vector>
#include "primitive_builder.h"
#include "string_tensor_builder.h"
#include "Poco/Data/MySQL/Connector.h"
#include "Poco/Data/PostgreSQL/Connector.h"
#include "Poco/Data/RecordSet.h"
#include "Poco/Data/SQLite/Connector.h"
#include "Poco/Data/ODBC/Connector.h"


namespace psi {

namespace {
std::once_flag mysql_register_flag;
}

OdbcAdaptor::OdbcAdaptor(OdbcAdaptorOptions options)
    : options_(std::move(options)), pool_(nullptr) {
  try {
    Init();
  } catch (const Poco::Data::DataException& e) {
    // NOTE: Poco Exception's what() method only return the exception category
    // without detail information, so we rethrow it with displayText() method.
    // https://docs.pocoproject.org/current/Poco.Exception.html#13533
    printf("catch unexpected Poco::Data::DataException: %s",
               e.name());
  }
}

std::vector<TensorPtr> OdbcAdaptor::GetQueryResult(const std::string& query) {
  try {
    return GetQueryResultImpl(query);
  } catch (const Poco::Data::DataException& e) {
      printf("catch unexpected Poco::Data::DataException:  %s",
               e.name());
    throw Poco::RuntimeException("get query result failed...");
  }
}

std::vector<TensorPtr> OdbcAdaptor::GetQueryResultImpl(
    const std::string& query) {
  try {
    auto session = CreateSession();
    Poco::Data::Statement select(session);
    select << query;
    select.execute();

    Poco::Data::RecordSet rs(select);

    // check amount of columns
    std::size_t column_cnt = rs.columnCount();

    using Poco::Data::MetaColumn;

    std::vector<std::unique_ptr<TensorBuilder>> builders;
    for (std::size_t i = 0; i < column_cnt; ++i) {
      std::unique_ptr<TensorBuilder> builder;
      switch (rs.columnType(i)) {
        case MetaColumn::ColumnDataType::FDT_BOOL:
          builder = std::make_unique<BooleanTensorBuilder>();
          break;
        case MetaColumn::ColumnDataType::FDT_INT8:
        case MetaColumn::ColumnDataType::FDT_UINT8:
        case MetaColumn::ColumnDataType::FDT_INT16:
        case MetaColumn::ColumnDataType::FDT_UINT16:
        case MetaColumn::ColumnDataType::FDT_INT32:
        case MetaColumn::ColumnDataType::FDT_UINT32:
        case MetaColumn::ColumnDataType::FDT_INT64:
        // FIXME: convert uint64 to int64 may overflow
        case MetaColumn::ColumnDataType::FDT_UINT64:
        case MetaColumn::ColumnDataType::FDT_DATE:
        case MetaColumn::ColumnDataType::FDT_TIME:
        case MetaColumn::ColumnDataType::FDT_TIMESTAMP:
          builder = std::make_unique<Int64TensorBuilder>();
          break;
        case MetaColumn::ColumnDataType::FDT_FLOAT:
          builder = std::make_unique<FloatTensorBuilder>();
          break;
        case MetaColumn::ColumnDataType::FDT_DOUBLE:
          builder = std::make_unique<DoubleTensorBuilder>();
          break;
        case MetaColumn::ColumnDataType::FDT_STRING:
        case MetaColumn::ColumnDataType::FDT_WSTRING:
        case MetaColumn::ColumnDataType::FDT_BLOB:
        case MetaColumn::ColumnDataType::FDT_CLOB:
          builder = std::make_unique<StringTensorBuilder>();
          break;
        default:
          std::cout << "Type is invalid..." << std::endl;
      }
      builders.push_back(std::move(builder));
    }

    bool more = rs.moveFirst();
    while (more) {
      for (std::size_t col_index = 0; col_index < column_cnt; col_index++) {
        auto var = rs[col_index];
        if (var.isEmpty()) {
          builders[col_index]->AppendNull();
          continue;
        }

        switch (rs.columnType(col_index)) {
          case MetaColumn::ColumnDataType::FDT_BOOL: {
            auto* builder =
                static_cast<BooleanTensorBuilder*>(builders[col_index].get());
            builder->Append(var.convert<bool>());
            break;
          }
          case MetaColumn::ColumnDataType::FDT_INT8:
          case MetaColumn::ColumnDataType::FDT_UINT8:
          case MetaColumn::ColumnDataType::FDT_INT16:
          case MetaColumn::ColumnDataType::FDT_UINT16:
          case MetaColumn::ColumnDataType::FDT_INT32:
          case MetaColumn::ColumnDataType::FDT_UINT32:
          case MetaColumn::ColumnDataType::FDT_INT64:
          case MetaColumn::ColumnDataType::FDT_UINT64: {
            auto* builder =
                static_cast<Int64TensorBuilder*>(builders[col_index].get());
            builder->Append(var.convert<int64_t>());
            break;
          }
          case MetaColumn::ColumnDataType::FDT_DATE:
          case MetaColumn::ColumnDataType::FDT_TIME:
          case MetaColumn::ColumnDataType::FDT_TIMESTAMP: {
            auto* builder =
                static_cast<Int64TensorBuilder*>(builders[col_index].get());
            auto epoch = var.convert<Poco::DateTime>().timestamp().epochTime();
            builder->Append(static_cast<int64_t>(epoch));
            break;
          }
          case MetaColumn::ColumnDataType::FDT_STRING:
          case MetaColumn::ColumnDataType::FDT_WSTRING:
          case MetaColumn::ColumnDataType::FDT_BLOB:
          case MetaColumn::ColumnDataType::FDT_CLOB: {
            auto* builder =
                static_cast<StringTensorBuilder*>(builders[col_index].get());
            builder->Append(var.convert<std::string>());
            break;
          }
          case MetaColumn::ColumnDataType::FDT_FLOAT: {
            auto* builder =
                static_cast<FloatTensorBuilder*>(builders[col_index].get());
            builder->Append(var.convert<float>());
            break;
          }
          case MetaColumn::ColumnDataType::FDT_DOUBLE: {
            auto* builder =
                static_cast<DoubleTensorBuilder*>(builders[col_index].get());
            builder->Append(var.convert<double>());
            break;
          }
          default:
           std::cout << "data type is invalid..." << std::endl;
        }
      }
      more = rs.moveNext();
    }
    std::vector<TensorPtr> results(column_cnt);
    for (std::size_t i = 0; i < column_cnt; ++i) {
      builders[i]->Finish(&results[i]);
    }
    return results;
  } catch (const std::exception& e) {
    std::cout << "捕获到异常: " << e.what() << std::endl;
    throw Poco::RuntimeException("get query result impl failed...");
  }
}

Poco::Data::Session OdbcAdaptor::CreateSession() {
  if (pool_) {
    return pool_->get();
  }
  return Poco::Data::Session(connector_, options_.connection_str);
}

void OdbcAdaptor::Init() {
  switch (options_.kind) {
    case DataSourceKind::MYSQL:
      connector_ = Poco::Data::MySQL::Connector::KEY;
      // `mysql_library_init` is not thread-safe in multithread environment,
      // see: https://dev.mysql.com/doc/c-api/5.7/en/mysql-library-init.html
      // MySQL::Connector::registerConnector() invokes `mysql_library_init`
      // refer to
      // https://github.com/pocoproject/poco/blob/poco-1.12.2-release/Data/MySQL/src/Connector.cpp#L55
      std::call_once(mysql_register_flag,
                     Poco::Data::MySQL::Connector::registerConnector);
      break;
    case DataSourceKind::SQLITE:
      connector_ = Poco::Data::SQLite::Connector::KEY;
      Poco::Data::SQLite::Connector::registerConnector();
      break;
    case DataSourceKind::POSTGRESQL:
      connector_ = Poco::Data::PostgreSQL::Connector::KEY;
      Poco::Data::PostgreSQL::Connector::registerConnector();
      break;
    case DataSourceKind::ODBC:
      connector_ = Poco::Data::ODBC::Connector::KEY;
      Poco::Data::ODBC::Connector::registerConnector();
      break;
    default:
      printf("unsupported DataSourceKind: %d", options_.kind);
  }

  if (options_.connection_type == ConnectionType::Pooled) {
    pool_ = std::make_unique<Poco::Data::SessionPool>(
        connector_, options_.connection_str, 1, options_.pool_size);
  }
}

}  // namespace psi