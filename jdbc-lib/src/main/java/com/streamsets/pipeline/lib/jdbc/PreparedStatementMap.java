/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

class PreparedStatementMap {
  private final Connection connection;
  private final String tableName;
  private final Map<SortedMap<String, String>, PreparedStatement> cache = new HashMap<>();

  public PreparedStatementMap(Connection connection, String tableName) {
    this.connection = connection;
    this.tableName = tableName;
  }

  public PreparedStatement getInsertFor(SortedMap<String, String> columns) throws SQLException {
    // The INSERT query we're going to perform (parameterized).
    if (cache.containsKey(columns)) {
      return cache.get(columns);
    } else {
      String query = String.format(
          "INSERT INTO %s (%s) VALUES (%s)",
          tableName,
          // keySet and values will both return the same ordering, due to using a SortedMap
          Joiner.on(", ").join(columns.keySet()),
          Joiner.on(", ").join(columns.values())
      );
      PreparedStatement statement = connection.prepareStatement(query);
      cache.put(columns, statement);
      return statement;
    }
  }

  public final Collection<PreparedStatement> getStatements() {
    return cache.values();
  }
}