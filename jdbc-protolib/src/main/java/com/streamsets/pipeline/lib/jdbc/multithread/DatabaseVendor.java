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

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.stage.origin.jdbc.table.QuoteChar;

import java.util.Collections;
import java.util.List;

/**
 * Database vendor (Oracle, MySQL, ...)
 */
public enum DatabaseVendor {
  // https://www.ibm.com/support/knowledgecenter/en/SSEPGG_11.1.0/com.ibm.db2.luw.apdv.java.doc/src/tpc/imjcc_r0052342.html
  DB2(
      ImmutableList.of("jdbc:db2:", "jdbc:ibmdb:", "jdbc:ids:"),
      ImmutableList.of("com.ibm.db2.jcc.DB2Driver"),
      Collections.emptyList()
  ),
  //
  MYSQL(
      ImmutableList.of("jdbc:mysql:"),
      ImmutableList.of("com.mysql.cj.jdbc.Driver", "com.mysql.jdbc.Driver"),
      ImmutableList.of(QuoteChar.NONE.getLabel(), QuoteChar.BACKTICK.getLabel())
  ),
  MARIADB(
      ImmutableList.of("jdbc:mariadb:"),
      ImmutableList.of("org.mariadb.jdbc.Driver"),
      Collections.emptyList()
  ),
  HIVE(
      ImmutableList.of("jdbc:hive2:"),
      ImmutableList.of("org.apache.hive.jdbc.HiveDriver", "com.cloudera.impala.jdbc41.Driver", "com.cloudera.impala.jdbc4.Driver"),
      Collections.emptyList()
  ),
  TERADATA(
      ImmutableList.of("jdbc:teradata"),
      ImmutableList.of("com.teradata.jdbc.TeraDriver"),
      Collections.emptyList()
  ),
  ORACLE(
      ImmutableList.of("jdbc:oracle:"),
      ImmutableList.of("oracle.jdbc.driver.OracleDriver", "oracle.jdbc.OracleDriver"),
      ImmutableList.of(QuoteChar.DOUBLE_QUOTES.getLabel(), QuoteChar.NONE.getLabel())
  ),
  POSTGRESQL(
      ImmutableList.of("jdbc:postgresql:"),
      ImmutableList.of("org.postgresql.Driver"),
      ImmutableList.of(QuoteChar.DOUBLE_QUOTES.getLabel(), QuoteChar.NONE.getLabel())
  ),
  SQL_SERVER(
      ImmutableList.of("jdbc:sqlserver:"),
      ImmutableList.of("com.microsoft.sqlserver.jdbc.SQLServerDriver"),
      ImmutableList.of(QuoteChar.NONE.getLabel(), QuoteChar.DOUBLE_QUOTES.getLabel(), QuoteChar.SQUARE_BRACKETS.getLabel())
  ),
  UNKNOWN(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
  ;

  private final List<String> jdbcPrefixes;
  private final List<String> drivers;
  private final List<String> quoteCharacters;

  DatabaseVendor(List<String> jdbcPrefixes, List<String> drivers, List<String> quoteCharacters) {
    this.jdbcPrefixes = jdbcPrefixes;
    this.drivers = drivers;
    this.quoteCharacters = quoteCharacters;
  }

  public List<String> getDrivers() {
    return drivers;
  }

  public List<String> getJdbcPrefixes() {
    return jdbcPrefixes;
  }

  public boolean isOneOf(DatabaseVendor... vendors) {
    if(vendors == null) {
      return false;
    }

    for(DatabaseVendor v : vendors) {
      if(this == v) {
        return true;
      }
    }

    return false;
  }

  /**
   * Return database vendor for given URL.
   */
  public static DatabaseVendor forUrl(String url) {
    for(DatabaseVendor vendor : DatabaseVendor.values()) {
      for(String prefix : vendor.jdbcPrefixes) {
        if(url.startsWith(prefix)) {
          return vendor;
        }
      }
    }

    return UNKNOWN;
  }

  /**
   * Check if the given quote character is in the list of valid characters for this vendor. If the list is empty,
   * return it as valid.
   */
  public boolean validQuoteCharacter(String quoteChar) {
    if (this.quoteCharacters.isEmpty()){
      return true;
    }

    for (String qC : this.quoteCharacters) {
      if (qC.equals(quoteChar)){
        return true;
      }
    }

    return false;
  }
}
