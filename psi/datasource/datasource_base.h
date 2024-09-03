//
// Created by jianjun on 24-8-20.
//

#pragma once

#include <memory>
#include <string>
#include <vector>

namespace psi {

  enum ConnectionType {
    Unknown = 0,
    Short = 1,
    Pooled = 2,
  };

  enum DataSourceKind {
    UNKNOWN = 0,
    MYSQL = 1,
    POSTGRESQL = 2,
    ODBC= 3,
    SQLITE=4,
    CSVDB=5,
  };

 enum DataSourceKindSub {
    POSTGRESQL_ODBC = 0,
    DAMENG_ODBC = 1,
  };

  struct DataSource {
    // datasource uuid
    std::string id;
    // human-friendly datasource name
    std::string name;
    DataSourceKind kind;
    // concrete data source connection string
    // It is comprehend to related data source adaptor.
    std::string connection_str;
  };

  enum PrimitiveDataType {
    PrimitiveDataType_UNDEFINED = 0,
    // Numeric types
    INT8 = 1,     // the 8-bit signed integer type
    INT16 = 2,    // the 16-bit signed integer type
    INT32 = 3,    // the 32-bit signed integer type
    INT64 = 4,    // the 64-bit signed integer type
    FLOAT32 = 5,  // the 32-bit binary floating point type
    FLOAT64 = 6,  // the 64-bit binary floating point type

    // Other types
    BOOL = 7,
    STRING = 8,
    // DATETIME and TIMESTAMP
    DATETIME = 9,    // https://dev.mysql.com/doc/refman/8.0/en/datetime.html
    TIMESTAMP = 10,  // seconds since '1970-01-01 00:00:00' UTC
  };

  /// @brief ColumnDesc contains column metadata information.
  struct ColumnDesc {
    std::string name;             // column name
    PrimitiveDataType dtype;  // column data type

    ColumnDesc(std::string name, PrimitiveDataType dtype)
        : name(std::move(name)), dtype(dtype) {}
  };
}
