/*
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.mysql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;

public class Util {
  private Util() {
  }

  /**
   * Get global variable value.
   * @param dataSource
   * @param variable
   * @return global variable value of empty string.
   * @throws SQLException
   */
  public static String getGlobalVariable(DataSource dataSource, String variable) throws SQLException {
    try (Connection conn = dataSource.getConnection()) {
      try (
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(String.format("show global variables like '%s'", variable));
      ) {
        if (rs.next()) {
          return rs.getString(2);
        } else {
          return "";
        }
      }
    }
  }

  /**
   * Get gtidset executed by server.
   * @param dataSource
   * @return
   * @throws SQLException
   */
  public static String getServerGtidExecuted(DataSource dataSource) throws SQLException {
    return getGlobalVariable(dataSource, "gtid_executed");
  }

  /**
   * Get gtidset purged from binlog.
   * @param dataSource
   * @return
   * @throws SQLException
   */
  public static String getServerGtidPurged(DataSource dataSource) throws SQLException {
    return getGlobalVariable(dataSource, "gtid_purged");
  }
}
