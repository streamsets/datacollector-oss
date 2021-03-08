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
package com.streamsets.pipeline.stage.origin.jdbc.CT.sqlserver;

import com.google.common.cache.CacheLoader;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.JdbcUtil;
import com.streamsets.pipeline.lib.jdbc.UtilsProvider;
import com.streamsets.pipeline.lib.jdbc.multithread.ConnectionManager;
import com.streamsets.pipeline.lib.jdbc.multithread.DatabaseVendor;
import com.streamsets.pipeline.lib.jdbc.multithread.TableContext;
import com.streamsets.pipeline.lib.jdbc.multithread.TableContextUtil;
import com.streamsets.pipeline.lib.jdbc.multithread.TableReadContext;
import com.streamsets.pipeline.lib.jdbc.multithread.TableRuntimeContext;
import com.streamsets.pipeline.lib.jdbc.multithread.cache.SQLServerCTContextLoader;
import com.streamsets.pipeline.lib.jdbc.multithread.util.MSQueryUtil;
import com.streamsets.pipeline.lib.jdbc.multithread.util.OffsetQueryUtil;
import com.streamsets.pipeline.stage.origin.jdbc.AbstractTableJdbcSource;
import com.streamsets.pipeline.stage.origin.jdbc.CommonSourceConfigBean;
import com.streamsets.pipeline.stage.origin.jdbc.table.TableJdbcConfigBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SQLServerCTSource extends AbstractTableJdbcSource {
  private static final Logger LOG = LoggerFactory.getLogger(SQLServerCTSource.class);
  public static final String OFFSET_VERSION =
      "$com.streamsets.pipeline.stage.origin.jdbc.CT.sqlserver.SQLServerCTSource.offset.version$";
  public static final String OFFSET_VERSION_1 = "1";

  private final CTTableJdbcConfigBean ctTableJdbcConfigBean;

  public SQLServerCTSource(
      HikariPoolConfigBean hikariConfigBean,
      CommonSourceConfigBean commonSourceConfigBean,
      CTTableJdbcConfigBean ctTableJdbcConfigBean,
      TableJdbcConfigBean tableJdbcConfigBean) {
    this(
        hikariConfigBean,
        commonSourceConfigBean,
        ctTableJdbcConfigBean,
        tableJdbcConfigBean,
        UtilsProvider.getTableContextUtil()
    );
  }

  public SQLServerCTSource(
      HikariPoolConfigBean hikariConfigBean,
      CommonSourceConfigBean commonSourceConfigBean,
      CTTableJdbcConfigBean ctTableJdbcConfigBean,
      TableJdbcConfigBean tableJdbcConfigBean,
      TableContextUtil tableContextUtil) {
    super(hikariConfigBean, commonSourceConfigBean, tableJdbcConfigBean, tableContextUtil);
    this.ctTableJdbcConfigBean = ctTableJdbcConfigBean;
  }

  @Override
  protected void validateTableJdbcConfigBean(PushSource.Context context, DatabaseVendor vendor, List<ConfigIssue> issues) {
    // no-op
  }

  @Override
  protected void handleLastOffset(Map<String, String> lastOffsets) throws StageException {
    if (lastOffsets == null) {
      return;
    }
    OffsetQueryUtil.validateV1Offset(getAllTableContexts(), getOffsets());
    getOffsets().putAll(lastOffsets);
    getOffsets().forEach((tableName, tableOffset) -> getContext().commitOffset(tableName, tableOffset));
  }

  @Override
  protected Map<String, TableContext> listTablesForConfig(PushSource.Context context, List<ConfigIssue> issues, ConnectionManager connectionManager) throws SQLException, StageException {
    Map<String, TableContext> allTableContexts = new HashMap<>();
    for (CTTableConfigBean tableConfigBean : ctTableJdbcConfigBean.tableConfigs) {
      //No duplicates even though a table matches multiple configurations, we will add it only once.
      allTableContexts.putAll(
          tableContextUtil.listCTTablesForConfig(
              connectionManager.getConnection(),
              tableConfigBean
          ));
    }

    return allTableContexts;
  }

  @Override
  protected CacheLoader<TableRuntimeContext, TableReadContext> getTableReadContextCache(
      ConnectionManager connectionManager,
      Map<String, String> offsets
  ) {
    return new SQLServerCTContextLoader(
        connectionManager,
        offsets,
        ctTableJdbcConfigBean.fetchSize,
        ctTableJdbcConfigBean.includeJoin
    );
  }

  @Override
  public void updateMaxOffsetsForTable(TableContext tableContext) {
    try {
      String schema = "[" + tableContext.getSchema() + "]";
      String tableName = "[" + tableContext.getTableName() + "]";
      tableContext.updateOffsetColumnToMaxValues(JdbcUtil.getMaximumOffsetValues(
          tableContext.getVendor(),
          connectionManager.getConnection(),
          schema,
          tableName,
          tableContext.getQuoteChar(),
          tableContext.getOffsetColumns()
      ));
    } catch (SQLException e) {
      if (!e.getMessage().contains(String.format("Invalid column name '%s'", MSQueryUtil.SYS_CHANGE_VERSION))) {
        LOG.error("SQLException attempting to update max offsets for TableContext {}: {}",
            tableContext,
            e.getMessage(),
            e
        );
      }
    }
  }

}
