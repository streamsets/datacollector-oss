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
package com.streamsets.pipeline.stage.origin.jdbc.cdc.sqlserver;

import com.google.common.cache.CacheLoader;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.multithread.ConnectionManager;
import com.streamsets.pipeline.lib.jdbc.multithread.TableContext;
import com.streamsets.pipeline.lib.jdbc.multithread.TableContextUtil;
import com.streamsets.pipeline.lib.jdbc.multithread.TableReadContext;
import com.streamsets.pipeline.lib.jdbc.multithread.TableRuntimeContext;
import com.streamsets.pipeline.lib.jdbc.multithread.util.OffsetQueryUtil;
import com.streamsets.pipeline.lib.jdbc.multithread.cache.SQLServerCDCContextLoader;
import com.streamsets.pipeline.stage.origin.jdbc.AbstractTableJdbcSource;
import com.streamsets.pipeline.stage.origin.jdbc.CT.sqlserver.SQLServerCTSource;
import com.streamsets.pipeline.stage.origin.jdbc.CommonSourceConfigBean;
import com.streamsets.pipeline.stage.origin.jdbc.table.TableJdbcConfigBean;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SQLServerCDCSource extends AbstractTableJdbcSource {
  private static final Logger LOG = LoggerFactory.getLogger(SQLServerCTSource.class);
  public static final String OFFSET_VERSION =
      "$com.streamsets.pipeline.stage.origin.jdbc.CDC.sqlserver.SQLServerCDCSource.offset.version$";
  public static final String OFFSET_VERSION_1 = "1";

  private final CommonSourceConfigBean commonSourceConfigBean;
  private final TableJdbcConfigBean tableJdbcConfigBean;
  private final CDCTableJdbcConfigBean cdcTableJdbcConfigBean;

  public SQLServerCDCSource(
      HikariPoolConfigBean hikariConfigBean,
      CommonSourceConfigBean commonSourceConfigBean,
      CDCTableJdbcConfigBean cdcTableJdbcConfigBean,
      TableJdbcConfigBean tableJdbcConfigBean
  ) {
    super(hikariConfigBean, commonSourceConfigBean, tableJdbcConfigBean);
    this.commonSourceConfigBean = commonSourceConfigBean;
    this.cdcTableJdbcConfigBean = cdcTableJdbcConfigBean;
    this.tableJdbcConfigBean = tableJdbcConfigBean;
  }

  @Override
  protected void handleLastOffset(Map<String, String> lastOffsets) throws StageException {
    if (lastOffsets != null) {
      getOffsets().putAll(lastOffsets);
      //Only if it is not already committed
      if (!lastOffsets.containsKey(OFFSET_VERSION)) {
        //Version the offset so as to allow for future evolution.
        getContext().commitOffset(OFFSET_VERSION, OFFSET_VERSION_1);
      }
    }

    OffsetQueryUtil.validateV1Offset(getAllTableContexts(), getOffsets());
  }

  @Override
  protected void validateTableJdbcConfigBean(PushSource.Context context, List<Stage.ConfigIssue> issues) {
    //no-op
  }

  @Override
  protected Map<String, TableContext> listTablesForConfig(
      PushSource.Context context,
      List<ConfigIssue> issues,
      ConnectionManager connectionManager
  ) throws SQLException, StageException {
    Map<String, TableContext> allTableContexts = new HashMap<>();
    for (CDCTableConfigBean tableConfigBean : cdcTableJdbcConfigBean.tableConfigs) {
      //No duplicates even though a table matches multiple configurations, we will add it only once.
      allTableContexts.putAll(
          TableContextUtil.listCDCTablesForConfig(
              connectionManager.getConnection(),
              tableConfigBean,
              commonSourceConfigBean.enableSchemaChanges
          ));
    }

    return allTableContexts;
  }

  @Override
  protected CacheLoader<TableRuntimeContext, TableReadContext> getTableReadContextCache(
      ConnectionManager connectionManager,
      Map<String, String> offsets
  ) {
    return new SQLServerCDCContextLoader(
        connectionManager,
        offsets,
        cdcTableJdbcConfigBean.fetchSize,
        commonSourceConfigBean.allowLateTable,
        commonSourceConfigBean.enableSchemaChanges
    );
  }
}
