/**
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.jdbc.table;

import com.streamsets.pipeline.lib.jdbc.JdbcUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public final class TableReadContext {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableReadContext.class);
  private final Statement st;
  private final String query;
  private ResultSet rs;

  public TableReadContext(Statement st, String query) throws SQLException {
    this.st = st;
    this.query = query;
    LOGGER.info("Executing Query :{}", query);
    rs = st.executeQuery(query);
  }

  public ResultSet getResultSet() {
    return rs;
  }

  public String getQuery() {
    return query;
  }

  public void destroy() {
    JdbcUtil.closeQuietly(st);
    JdbcUtil.closeQuietly(rs);
  }
}
