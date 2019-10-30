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
import com.streamsets.pipeline.lib.jdbc.multithread.DatabaseVendor;
import com.streamsets.pipeline.lib.jdbc.multithread.TableContext;
import com.streamsets.pipeline.lib.jdbc.multithread.TableReadContext;
import com.streamsets.pipeline.lib.jdbc.multithread.TableRuntimeContext;
import com.streamsets.pipeline.lib.jdbc.multithread.util.OffsetQueryUtil;
import com.streamsets.pipeline.lib.jdbc.multithread.util.MSQueryUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SQLServerCTContextLoader extends CacheLoader<TableRuntimeContext, TableReadContext>{
  private final ConnectionManager connectionManager;
  private final Map<String, String> offsets;
  private final int fetchSize;
  private final boolean includeJoin;

  public SQLServerCTContextLoader(
      ConnectionManager connectionManager,
      Map<String, String> offsets,
      int fetchSize,
      boolean includeJoin
  ) {
    this.connectionManager = connectionManager;
    this.offsets = offsets;
    this.fetchSize = fetchSize;
    this.includeJoin = includeJoin;
  }

  /**
   * Returns qualified table name (schema.table name) for the MSSQL Server. It uses the [] chars
   * to allow reserved words as table names.
   * @param schema schema name, can be null
   * @param tableName table name
   * @return qualified table name if schema is not null and tableName alone if schema is null.
   */
  public static String getMSSQLQualifiedTableName(String schema, String tableName) {
    tableName = "["+tableName+"]";
    return StringUtils.isEmpty(schema) ? tableName : "[" + schema + "]" + "." + tableName ;
  }

  @Override
  public TableReadContext load(TableRuntimeContext tableRuntimeContext) throws Exception {
    TableContext tableContext = tableRuntimeContext.getSourceTableContext();

    final Map<String, String> offset = OffsetQueryUtil.getColumnsToOffsetMapFromOffsetFormat(offsets.get(tableRuntimeContext.getOffsetKey()));

    String query = MSQueryUtil.buildQuery(
        offset,
        fetchSize,
        getMSSQLQualifiedTableName(tableContext.getSchema(),tableContext.getTableName()),
        tableContext.getOffsetColumns(),
        includeJoin,
        tableContext.getOffset()
    );

    Pair<String, List<Pair<Integer, String>>> queryAndParamValToSet = Pair.of(query, new ArrayList<>());

    Connection connection = connectionManager.getConnection();

    TableReadContext tableReadContext =
        new TableReadContext(
            DatabaseVendor.SQL_SERVER,
            connection,
            queryAndParamValToSet.getLeft(),
            queryAndParamValToSet.getRight(),
            fetchSize
        );

    return tableReadContext;
  }
}
