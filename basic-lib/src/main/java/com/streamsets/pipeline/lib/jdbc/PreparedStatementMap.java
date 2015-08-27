/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.lib.jdbc;

import com.google.common.base.Joiner;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;

class PreparedStatementMap {
  private final Connection connection;
  private final String tableName;
  private final Map<SortedSet<String>, PreparedStatement> cache = new HashMap<>();

  public PreparedStatementMap(Connection connection, String tableName) {
    this.connection = connection;
    this.tableName = tableName;
  }

  public PreparedStatement getInsertFor(SortedSet<String> columns) throws SQLException {
    // The INSERT query we're going to perform (parameterized).
    if (cache.containsKey(columns)) {
      return cache.get(columns);
    } else {
      String query = String.format(
          "INSERT INTO %s (%s) VALUES (%s);",
          tableName,
          Joiner.on(", ").join(columns),
          Joiner.on(", ").join(Collections.nCopies(columns.size(), "?"))
      );
      PreparedStatement statement = connection.prepareStatement(query);
      cache.put(columns, statement);
      return statement;
    }
  }

  public final Collection<PreparedStatement> getStatements() {
    return cache.values();
  }

  public void executeStatements() throws SQLException {
    for (PreparedStatement statement : cache.values()) {
      statement.executeBatch();
      statement.close();
    }
  }
}