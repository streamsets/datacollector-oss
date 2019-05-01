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
import com.streamsets.pipeline.lib.jdbc.multithread.util.MSQueryUtil;
import com.streamsets.pipeline.lib.jdbc.multithread.util.OffsetQueryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;

public class SQLServerCDCContextLoader extends CacheLoader<TableRuntimeContext, TableReadContext> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SQLServerCDCContextLoader.class);
  private final ConnectionManager connectionManager;
  private final Map<String, String> offsets;
  private final int fetchSize;
  private final boolean allowLateTable;
  private final boolean enableSchemaChanges;
  private final boolean useTable;
  private final boolean isReconnect;
  private int txnWindow;

  public SQLServerCDCContextLoader(
      ConnectionManager connectionManager,
      Map<String, String> offsets,
      int fetchSize,
      boolean allowLateTable,
      boolean enableSchemaChanges,
      boolean useTable,
      int txnWindow,
      boolean isReconnect
  ) {
    this.connectionManager = connectionManager;
    this.offsets = offsets;
    this.fetchSize = fetchSize;
    this.allowLateTable = allowLateTable;
    this.enableSchemaChanges = enableSchemaChanges;
    this.useTable = useTable;
    this.txnWindow = txnWindow;
    this.isReconnect = isReconnect;
  }

  @Override
  public TableReadContext load(TableRuntimeContext tableRuntimeContext) throws Exception {
    TableContext tableContext = tableRuntimeContext.getSourceTableContext();

    final Map<String, String> offset = OffsetQueryUtil.getColumnsToOffsetMapFromOffsetFormat(offsets.get(tableRuntimeContext.getOffsetKey()));

    if (txnWindow > 0 && offset.get(MSQueryUtil.CDC_TXN_WINDOW) != null) {
      txnWindow = Integer.parseInt(offset.get(MSQueryUtil.CDC_TXN_WINDOW));
    }

    String query = MSQueryUtil.buildCDCQuery(
        offset,
        tableContext.getQualifiedName(),
        tableContext.getOffsetColumnToStartOffset(),
        allowLateTable,
        enableSchemaChanges,
        fetchSize,
        useTable,
        txnWindow
    );

    if (isReconnect) {
      LOGGER.debug("close the connection");
      connectionManager.closeConnection();
    }

    TableReadContext tableReadContext =
        new TableReadContext(
            DatabaseVendor.SQL_SERVER,
            connectionManager.getConnection(),
            query,
            new ArrayList<>(),
            fetchSize
        );

    return tableReadContext;
  }
}
