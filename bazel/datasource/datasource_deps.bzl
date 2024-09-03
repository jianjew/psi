# Copyright 2023 Ant Group Co., Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def datasource_deps():
    _org_apache_arrow()
    _org_pocoproject_poco()
    _org_postgres()
    _org_sqlite()
    _com_mysql()
    _ncurses()
    _org_unixodbc()

def _org_apache_arrow():
    maybe(
        http_archive,
        name = "org_apache_arrow",
        urls = [
            "https://github.com/apache/arrow/archive/apache-arrow-10.0.0.tar.gz",
        ],
        patch_args = ["-p1"],
        patches = ["@//bazel/datasource:patches/arrow.patch"],
        sha256 = "2852b21f93ee84185a9d838809c9a9c41bf6deca741bed1744e0fdba6cc19e3f",
        strip_prefix = "arrow-apache-arrow-10.0.0",
        build_file = "@psi//bazel/datasource:arrow.BUILD",
    )

def _org_pocoproject_poco():
    maybe(
        http_archive,
        name = "org_pocoproject_poco",
        urls = [
            "https://github.com/pocoproject/poco/archive/refs/tags/poco-1.12.2-release.tar.gz",
        ],
        strip_prefix = "poco-poco-1.12.2-release",
        sha256 = "30442ccb097a0074133f699213a59d6f8c77db5b2c98a7c1ad9c5eeb3a2b06f3",
        build_file = "@psi//bazel/datasource:poco.BUILD",
    )

def _com_mysql():
    maybe(
        http_archive,
        name = "com_mysql",
        urls = [
            "https://github.com/mysql/mysql-server/archive/refs/tags/mysql-8.0.30.tar.gz",
        ],
        patch_args = ["-p1"],
        patches = ["@//bazel/datasource:patches/mysql.patch"],
        sha256 = "e76636197f9cb764940ad8d800644841771def046ce6ae75c346181d5cdd879a",
        strip_prefix = "mysql-server-mysql-8.0.30",
        build_file = "@psi//bazel/datasource:mysql.BUILD",
    )

def _org_postgres():
    maybe(
        http_archive,
        name = "org_postgres",
        urls = [
            "https://ftp.postgresql.org/pub/source/v15.2/postgresql-15.2.tar.gz",
        ],
        sha256 = "eccd208f3e7412ad7bc4c648ecc87e0aa514e02c24a48f71bf9e46910bf284ca",
        strip_prefix = "postgresql-15.2",
        build_file = "@psi//bazel/datasource:postgres.BUILD",
    )

def _org_unixodbc():
    maybe(
        http_archive,
        name = "org_unixodbc",
        urls = [
            "http://www.unixodbc.org/unixODBC-2.3.12.tar.gz",
        ],
        sha256 = "f210501445ce21bf607ba51ef8c125e10e22dffdffec377646462df5f01915ec",
        strip_prefix = "unixODBC-2.3.12",
        build_file = "@psi//bazel/datasource:unixodbc.BUILD",
    )

def _org_sqlite():
    maybe(
        http_archive,
        name = "org_sqlite",
        urls = [
            "https://www.sqlite.org/2020/sqlite-amalgamation-3320200.zip",
        ],
        sha256 = "7e1ebd182a61682f94b67df24c3e6563ace182126139315b659f25511e2d0b5d",
        strip_prefix = "sqlite-amalgamation-3320200",
        build_file = "@psi//bazel/datasource:sqlite3.BUILD",
    )

def _com_github_duckdb():
    maybe(
        http_archive,
        name = "com_github_duckdb",
        urls = [
            "https://github.com/duckdb/duckdb/archive/refs/tags/v0.9.2.tar.gz",
        ],
        patch_args = ["-p1"],
        patches = ["@//bazel/datasource:patches/duckdb.patch"],
        sha256 = "afff7bd925a98dc2af4039b8ab2159b0705cbf5e0ee05d97f7bb8dce5f880dc2",
        strip_prefix = "duckdb-0.9.2",
        build_file = "@psi//bazel/datasource:duckdb.BUILD",
    )

def _ncurses():
    maybe(
        http_archive,
        name = "ncurses",
        urls = [
            "https://ftp.gnu.org/pub/gnu/ncurses/ncurses-6.3.tar.gz",
        ],
        sha256 = "97fc51ac2b085d4cde31ef4d2c3122c21abc217e9090a43a30fc5ec21684e059",
        strip_prefix = "ncurses-6.3",
        build_file = "@psi//bazel/datasource:ncurses.BUILD",
    )
