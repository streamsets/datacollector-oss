/*
 * Copyright 2019 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.lib.jdbc.multithread;

/**
 * Database vendor (Oracle, MySQL, ...)
 */
public enum DatabaseVendor {
  DB2("com.ibm.db2.jcc.DB2Driver"),
  MYSQL("com.mysql.jdbc.Driver"),
  HIVE("org.apache.hive.jdbc.HiveDriver", "com.cloudera.impala.jdbc41.Driver", "com.cloudera.impala.jdbc4.Driver"),
  TERADATA("com.teradata.jdbc.TeraDriver"),
  ORACLE("oracle.jdbc.driver.OracleDriver"),
  POSTGRESQL("org.postgresql.Driver"),
  SQL_SERVER("com.microsoft.sqlserver.jdbc.SQLServerDriver"),
  UNKNOWN(),
  ;

  private final String[] drivers;

  DatabaseVendor(String ...drivers) {
    this.drivers = drivers;
  }

  public String[] getDrivers() {
    return drivers;
  }
}
