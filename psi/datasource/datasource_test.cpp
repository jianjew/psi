#include <iostream>
#include "datasource_base.h"
#include "datasource_adaptor_mgr.h"


using namespace std;

#define POSTGRESQL_PORT    "9876"
#define POSTGRESQL_DB      "postgres"
#define POSTGRESQL_UID     "postgres"
#define POSTGRESQL_PWD     "123456"
#define POSTGRESQL_SERVER  "172.16.16.116"
#define POSTGRESQL_ODBC_DRIVER "PostgreSQL Unicode"

// int main()
// {
//     cout << "Hello world!" << endl;
//     return 0;
// }

std::unique_ptr<Poco::Data::Session> session_;

// void SetUp() {
//     std::cout << "start setup..." << std::endl;
//     Poco::Data::MySQL::Connector::registerConnector();
//     std::cout << "get connectionstr..." << std::endl;
//     std::string connection_str = getConnectionStr(psi::DataSourceKind::MYSQL);
//     try {
//         std::cout << "new session before..., str is: " << connection_str << std::endl;
//         session_ = std::make_unique<Poco::Data::Session>(Poco::Data::MySQL::Connector::KEY, connection_str);
//         std::cout << "new session after..." << std::endl;
//         // create table
//         using Poco::Data::Keywords::now;
//         // create table
//         *session_ << "DROP TABLE IF EXISTS person", now;
//         *session_ << "CREATE TABLE person(name VARCHAR(30), age INTEGER(3))", now;
//         // insert some rows
//         *session_ << "INSERT INTO person VALUES(\"alice\", 18)", now;
//         *session_ << "INSERT INTO person VALUES(\"bob\", 20)", now;
//     } catch (const Poco::Exception ex) {
//         std::cout << ex.message() << std::endl;
//     }
// }

std::string getConnectionStr(const psi::DataSourceKind kind) {
    switch (kind) {
        case psi::DataSourceKind::POSTGRESQL:
            return "host=172.16.16.116 port=9876 user=postgres password=123456 dbname=postgres";
        case psi::DataSourceKind::MYSQL:
            return "host=172.16.0.217; port=12001; user=root; password=isafelinkepw@2077;db=linkempc;compress=true;auto-reconnect=true";
        case psi::DataSourceKind::ODBC:
            std::cout << "**********************ODBC connect string*****************************" << std::endl;
            const std::string dm = "DSN=dm;SERVER=172.16.0.217;UID=SYSDBA;PWD=SYSDBA001;TCP_PORT=52360";  // no need space
            std::cout << dm << std::endl;

            const std::string pg = "DRIVER=PostgreSQL Unicode;DATABASE=postgres;SERVER=172.16.16.116;PORT=9876;UID=postgres;PWD=123456;";  // pg odbc
            std::cout << pg << std::endl;
            return dm;
            //return pg;
    }
}

int main() {
    std::vector<psi::DataSourceKind> kinds = {psi::DataSourceKind::MYSQL, psi::DataSourceKind::POSTGRESQL, psi::DataSourceKind::ODBC};
    std::unique_ptr<psi::DatasourceAdaptorMgr> datasourceAdaptorMgr = std::make_unique<psi::DatasourceAdaptorMgr>();
    psi::DataSource options;
    std::string query = "";
    for (const auto& kind : kinds) {
        options.kind = kind;
        options.connection_str = getConnectionStr(kind);
        std::cout << "**********************" << kind << "*****************************" << std::endl;
        switch (kind) {
            case psi::DataSourceKind::POSTGRESQL:
                query = "SELECT * FROM dataset_analysis";
                break;
            case psi::DataSourceKind::MYSQL:
                query = "SELECT * FROM auto_asset";
                break;
            case psi::DataSourceKind::ODBC:
                query = "SELECT * FROM CREDIT_ACTIVE000";  // dm8 odbc
                //query = "SELECT * FROM dataset_analysis";  // pg odbc
                break;
            default: ;
        }
        try {
            auto adaptor = datasourceAdaptorMgr->GetAdaptor(options);
            std::chrono::high_resolution_clock::time_point start;
            std::chrono::high_resolution_clock::time_point finish;

            start = std::chrono::high_resolution_clock::now(); // 开始计时
            auto results = adaptor->ExecQuery(query);
            finish = std::chrono::high_resolution_clock::now(); // 结束计时

            std::cout << "tensor size: " << results.size() << std::endl;

            // column name
            std::cout << "results[0]->Length(): " << results[0]->Length() << std::endl;

            // for (int i = 0; i < results.size(); i++) {
            //     auto chunked_array = results[i].get()->ToArrowChunkedArray();
            //     auto shared_ptr = chunked_array->ToString();
            //     std::cout << shared_ptr << std::endl;
            // }
            std::cout << "耗时为:" << std::chrono::duration_cast<std::chrono::milliseconds>(finish - start).count() << "ms.\n";
        } catch (const std::exception& e) {
            std::cout << "捕获到异常: " << e.what() << std::endl;
        }
    }
    return 1;
}
