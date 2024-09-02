# PIR Datasource unittest Examples

## datasource_test.cpp

1. build/run.

```bash
bazel build //psi/datasource:datasource_test
bazel-bin/psi/datasource/datasource_test
```

## self testing

1. Add or edit getConnectionStr. This is the connection information for defining the data source;

2. Add new DataSourceKind to vector<psi::DataSourceKind> kinds; you can add new datasource type;

3. Add or edit new SQL query info.