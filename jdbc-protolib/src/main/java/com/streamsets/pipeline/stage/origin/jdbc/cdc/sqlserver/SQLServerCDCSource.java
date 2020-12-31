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
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.jdbc.DataType;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.UtilsProvider;
import com.streamsets.pipeline.lib.jdbc.multithread.ConnectionManager;
import com.streamsets.pipeline.lib.jdbc.multithread.DatabaseVendor;
import com.streamsets.pipeline.lib.jdbc.multithread.TableContext;
import com.streamsets.pipeline.lib.jdbc.multithread.TableContextUtil;
import com.streamsets.pipeline.lib.jdbc.multithread.TableReadContext;
import com.streamsets.pipeline.lib.jdbc.multithread.TableRuntimeContext;
import com.streamsets.pipeline.lib.jdbc.multithread.util.MSQueryUtil;
import com.streamsets.pipeline.lib.jdbc.multithread.util.OffsetQueryUtil;
import com.streamsets.pipeline.lib.jdbc.multithread.cache.SQLServerCDCContextLoader;
import com.streamsets.pipeline.stage.origin.jdbc.AbstractTableJdbcSource;
import com.streamsets.pipeline.stage.origin.jdbc.CT.sqlserver.SQLServerCTSource;
import com.streamsets.pipeline.stage.origin.jdbc.CommonSourceConfigBean;
import com.streamsets.pipeline.stage.origin.jdbc.table.TableJdbcConfigBean;
import org.joda.time.DateTime;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SQLServerCDCSource extends AbstractTableJdbcSource {
  private static final Logger LOG = LoggerFactory.getLogger(SQLServerCTSource.class);
  public static final String OFFSET_VERSION =
      "$com.streamsets.pipeline.stage.origin.jdbc.CDC.sqlserver.SQLServerCDCSource.offset.version$";
  public static final String OFFSET_VERSION_1 = "1";

  private final CDCTableJdbcConfigBean cdcTableJdbcConfigBean;

  public SQLServerCDCSource(
      HikariPoolConfigBean hikariConfigBean,
      CommonSourceConfigBean commonSourceConfigBean,
      CDCTableJdbcConfigBean cdcTableJdbcConfigBean,
      TableJdbcConfigBean tableJdbcConfigBean
  ) {
    this(
        hikariConfigBean,
        commonSourceConfigBean,
        cdcTableJdbcConfigBean,
        tableJdbcConfigBean,
        UtilsProvider.getTableContextUtil()
    );
  }

  public SQLServerCDCSource(
      HikariPoolConfigBean hikariConfigBean,
      CommonSourceConfigBean commonSourceConfigBean,
      CDCTableJdbcConfigBean cdcTableJdbcConfigBean,
      TableJdbcConfigBean tableJdbcConfigBean,
      TableContextUtil tableContextUtil
  ) {
    super(hikariConfigBean, commonSourceConfigBean, tableJdbcConfigBean, tableContextUtil);
    this.cdcTableJdbcConfigBean = cdcTableJdbcConfigBean;
    this.isReconnect = cdcTableJdbcConfigBean == null ? false : cdcTableJdbcConfigBean.isReconnect;
  }

  @Override
  protected List<Stage.ConfigIssue> init() {
    List<Stage.ConfigIssue> issues = super.init();

    if (issues.isEmpty()) {
      this.sourceTableInfoMap = listSourceTables();
    }

    return issues;
  }

  @Override
  protected void handleLastOffset(Map<String, String> lastOffsets) throws StageException {
    if (lastOffsets != null) {
      for (String key : lastOffsets.keySet()) {
        if (key.equals(Source.POLL_SOURCE_OFFSET_KEY)) {
          // skip
          continue;
        }
        if (lastOffsets.get(key) != null) {
          getOffsets().put(key, lastOffsets.get(key));
        } else {
          LOG.warn("last offset value is null: '{}'", key);
        }
      }
      //Only if it is not already committed
      if (!lastOffsets.containsKey(OFFSET_VERSION)) {
        //Version the offset so as to allow for future evolution.
        getContext().commitOffset(OFFSET_VERSION, OFFSET_VERSION_1);
      }
    }

    OffsetQueryUtil.validateV1Offset(getAllTableContexts(), getOffsets());
  }

  @Override
  protected void validateTableJdbcConfigBean(PushSource.Context context, DatabaseVendor vendor, List<Stage.ConfigIssue> issues) {
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
          tableContextUtil.listCDCTablesForConfig(
              connectionManager.getConnection(),
              tableConfigBean,
              commonSourceConfigBean.enableSchemaChanges
          ));
    }

    return allTableContexts;
  }

  public class SourceTableInfo {
    public String schemaName;
    public String tableName;
  }

  private Map<String, SourceTableInfo> listSourceTables() {
    Map<String, SourceTableInfo> sourceTables = new HashMap<>();
    String query = MSQueryUtil.buildCDCSourceTableQuery(cdcTableJdbcConfigBean.tableConfigs);

    try {
      PreparedStatement ps = connectionManager.getConnection().prepareStatement(query);

      LOG.debug("Executing Query: ", query);
      ResultSet rs = ps.executeQuery();

      while (!rs.isClosed() && rs.next()) {
        if (rs.isClosed()) {
          LOG.trace("ResultSet is closed");
        }

        String captureInstanceName = rs.getString(MSQueryUtil.CAPTURE_INSTANCE_NAME);
        String schema = rs.getString(MSQueryUtil.SOURCE_SCHEMA_NAME);
        String table = rs.getString(MSQueryUtil.SOURCE_NAME);

        SourceTableInfo sourceTableInfo = new SourceTableInfo();
        sourceTableInfo.schemaName = schema;
        sourceTableInfo.tableName = table;

        sourceTables.put(captureInstanceName, sourceTableInfo);
      }

      jdbcUtil.closeQuietly(rs);
      jdbcUtil.closeQuietly(ps);
      connectionManager.closeConnection();
    } catch (SQLException ex) {
      LOG.error("Failed to get source table infos: ", ex);
    }
    return sourceTables;
  }

  @Override
  protected CacheLoader<TableRuntimeContext, TableReadContext> getTableReadContextCache(
      ConnectionManager connectionManager,
      Map<String, String> offsets
  ) {
    return new SQLServerCDCContextLoader(
        connectionManager,
        getOffsets(),
        cdcTableJdbcConfigBean.fetchSize,
        commonSourceConfigBean.allowLateTable,
        commonSourceConfigBean.enableSchemaChanges,
        cdcTableJdbcConfigBean.useTable,
        commonSourceConfigBean.txnWindow,
        cdcTableJdbcConfigBean.isReconnect
    );
  }
}
