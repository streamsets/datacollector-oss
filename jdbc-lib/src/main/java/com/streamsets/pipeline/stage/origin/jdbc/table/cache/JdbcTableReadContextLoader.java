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
package com.streamsets.pipeline.stage.origin.jdbc.table.cache;

import com.google.common.cache.CacheLoader;
import com.streamsets.pipeline.stage.origin.jdbc.table.ConnectionManager;
import com.streamsets.pipeline.stage.origin.jdbc.table.TableContext;
import com.streamsets.pipeline.stage.origin.jdbc.table.TableJdbcELEvalContext;
import com.streamsets.pipeline.stage.origin.jdbc.table.TableReadContext;
import com.streamsets.pipeline.stage.origin.jdbc.table.util.OffsetQueryUtil;

import java.sql.Statement;
import java.util.Map;

/**
 * Loads a {@link TableReadContext} for corresponding {@link TableContext}
 * Users can then use {@link TableReadContext#getResultSet()} to read rows
 * from the corresponding table
 */
public class JdbcTableReadContextLoader extends CacheLoader<TableContext, TableReadContext>{
  private final ConnectionManager connectionManager;
  private final TableJdbcELEvalContext tableJdbcELEvalContext;
  private final Map<String, String> offsets;

  private final boolean configureFetchSize;
  private final int fetchSize;

  public JdbcTableReadContextLoader(
      ConnectionManager connectionManager,
      Map<String, String> offsets,
      boolean configureFetchSize,
      int fetchSize,
      TableJdbcELEvalContext tableJdbcELEvalContext
  ) {
    this.connectionManager = connectionManager;
    this.offsets = offsets;
    this.configureFetchSize = configureFetchSize;
    this.fetchSize = fetchSize;
    this.tableJdbcELEvalContext = tableJdbcELEvalContext;
  }

  @Override
  public TableReadContext load(TableContext tableContext) throws Exception {
    Statement st = connectionManager.getConnection().createStatement();
    if (configureFetchSize) {
      st.setFetchSize(fetchSize);
    }

    String query = OffsetQueryUtil.buildQuery(
        tableContext,
        offsets.get(tableContext.getTableName()),
        tableJdbcELEvalContext
    );

    TableReadContext tableReadContext = new TableReadContext(st, query);

    //Clear the initial offset after the  query is build so we will not use the initial offset from the next
    //time the table is used.
    tableContext.clearStartOffset();

    return tableReadContext;
  }
}
