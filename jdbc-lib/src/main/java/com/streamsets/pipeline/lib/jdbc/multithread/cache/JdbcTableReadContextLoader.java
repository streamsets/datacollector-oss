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
package com.streamsets.pipeline.lib.jdbc.multithread.cache;

import com.google.common.cache.CacheLoader;
import com.streamsets.pipeline.lib.jdbc.multithread.ConnectionManager;
import com.streamsets.pipeline.lib.jdbc.multithread.TableContext;
import com.streamsets.pipeline.stage.origin.jdbc.table.TableJdbcELEvalContext;
import com.streamsets.pipeline.lib.jdbc.multithread.TableReadContext;
import com.streamsets.pipeline.lib.jdbc.multithread.TableRuntimeContext;
import com.streamsets.pipeline.lib.jdbc.multithread.util.OffsetQueryUtil;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Loads a {@link TableReadContext} for corresponding {@link TableContext}
 * Users can then use {@link TableReadContext#getResultSet()} to read rows
 * from the corresponding table
 */
public class JdbcTableReadContextLoader extends CacheLoader<TableRuntimeContext, TableReadContext>{
  private final ConnectionManager connectionManager;
  private final TableJdbcELEvalContext tableJdbcELEvalContext;
  private final Map<String, String> offsets;
  private final int fetchSize;
  private final String quoteChar;

  public JdbcTableReadContextLoader(
      ConnectionManager connectionManager,
      Map<String, String> offsets,
      int fetchSize,
      String quoteChar,
      TableJdbcELEvalContext tableJdbcELEvalContext
  ) {
    this.connectionManager = connectionManager;
    this.offsets = offsets;
    this.fetchSize = fetchSize;
    this.quoteChar = quoteChar;
    this.tableJdbcELEvalContext = tableJdbcELEvalContext;
  }

  @Override
  public TableReadContext load(TableRuntimeContext tableRuntimeContext) throws Exception {
    Pair<String, List<Pair<Integer, String>>> queryAndParamValToSet;
    final boolean nonIncremental = tableRuntimeContext.isUsingNonIncrementalLoad();
    if (nonIncremental) {
      final String baseTableQuery = OffsetQueryUtil.buildBaseTableQuery(tableRuntimeContext, quoteChar);
      queryAndParamValToSet = Pair.of(baseTableQuery, Collections.emptyList());
    } else {
      queryAndParamValToSet = OffsetQueryUtil.buildAndReturnQueryAndParamValToSet(
          tableRuntimeContext,
          offsets.get(tableRuntimeContext.getOffsetKey()),
          quoteChar,
          tableJdbcELEvalContext
      );
    }

    TableReadContext tableReadContext =
        new TableReadContext(
            connectionManager.getConnection(),
            queryAndParamValToSet.getLeft(),
            queryAndParamValToSet.getRight(),
            fetchSize,
            nonIncremental
        );

    //Clear the initial offset after the  query is build so we will not use the initial offset from the next
    //time the table is used.
    tableRuntimeContext.getSourceTableContext().clearStartOffset();

    return tableReadContext;
  }
}
