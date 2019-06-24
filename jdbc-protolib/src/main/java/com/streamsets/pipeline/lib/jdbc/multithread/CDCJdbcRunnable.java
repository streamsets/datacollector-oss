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
package com.streamsets.pipeline.lib.jdbc.multithread;

import com.google.common.cache.CacheLoader;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.RateLimiter;
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.lib.jdbc.JdbcUtil;
import com.streamsets.pipeline.lib.jdbc.MSOperationCode;
import com.streamsets.pipeline.lib.jdbc.multithread.util.MSQueryUtil;
import com.streamsets.pipeline.lib.jdbc.multithread.util.OffsetQueryUtil;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.streamsets.pipeline.stage.origin.jdbc.CommonSourceConfigBean;
import com.streamsets.pipeline.stage.origin.jdbc.JdbcEvents;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.sqlserver.SQLServerCDCSource;
import com.streamsets.pipeline.stage.origin.jdbc.table.TableJdbcConfigBean;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class CDCJdbcRunnable extends JdbcBaseRunnable {
  private static final Logger LOG = LoggerFactory.getLogger(CTJdbcRunnable.class);

  private final Set<String> recordHeader;
  private final Map<String, SQLServerCDCSource.SourceTableInfo> infoMap;

  protected static final String CDC_NAMESPACE_HEADER = "cdc.";

  public CDCJdbcRunnable(
      PushSource.Context context,
      int threadNumber,
      int batchSize,
      Map<String, String> offsets,
      MultithreadedTableProvider tableProvider,
      ConnectionManager connectionManager,
      TableJdbcConfigBean tableJdbcConfigBean,
      CommonSourceConfigBean commonSourceConfigBean,
      CacheLoader<TableRuntimeContext, TableReadContext> tableReadContextCache,
      RateLimiter queryRateLimiter,
      boolean isReconnect,
      Map<String, SQLServerCDCSource.SourceTableInfo> infoMap
  ) {
    super(
        context,
        threadNumber,
        batchSize,
        offsets,
        tableProvider,
        connectionManager,
        tableJdbcConfigBean,
        commonSourceConfigBean,
        tableReadContextCache,
        queryRateLimiter,
        isReconnect
    );

    this.recordHeader = ImmutableSet.of(
        MSQueryUtil.CDC_START_LSN,
        MSQueryUtil.CDC_END_LSN,
        MSQueryUtil.CDC_SEQVAL,
        MSQueryUtil.CDC_OPERATION,
        MSQueryUtil.CDC_UPDATE_MASK,
        MSQueryUtil.CDC_COMMAND_ID
    );
    this.infoMap = infoMap;
  }

  @Override
  public void createAndAddRecord(
      ResultSet rs,
      TableRuntimeContext tableRuntimeContext,
      BatchContext batchContext
  ) throws SQLException, StageException {
    ResultSetMetaData md = rs.getMetaData();

    LinkedHashMap<String, Field> fields = jdbcUtil.resultSetToFields(
        rs,
        commonSourceConfigBean,
        errorRecordHandler,
        tableJdbcConfigBean.unknownTypeAction,
        recordHeader,
        DatabaseVendor.SQL_SERVER
    );

    Map<String, String> columnOffsets = new HashMap<>();

    // Generate Offset includes __$start_lsn, __$seqval, and __$operation
    columnOffsets.put(MSQueryUtil.CDC_START_LSN, rs.getString(MSQueryUtil.CDC_START_LSN));
    columnOffsets.put(MSQueryUtil.CDC_SEQVAL, rs.getString(MSQueryUtil.CDC_SEQVAL));

    try {
      columnOffsets.put(MSQueryUtil.CDC_OPERATION, rs.getString(MSQueryUtil.CDC_OPERATION));
    } catch (Exception ex) {
      LOG.trace("$__operation is not supported in this SQL Server");
    }

    if (commonSourceConfigBean.txnWindow > 0) {
      columnOffsets.put(MSQueryUtil.CDC_TXN_WINDOW, Integer.toString(commonSourceConfigBean.txnWindow));
    }

    String offsetFormat = OffsetQueryUtil.getOffsetFormat(columnOffsets);

    Record record = context.createRecord(tableRuntimeContext.getQualifiedName() + "::" + offsetFormat);
    record.set(Field.createListMap(fields));

    //Set Column Headers
    jdbcUtil.setColumnSpecificHeaders(
        record,
        Collections.singleton(tableRuntimeContext.getSourceTableContext().getTableName()),
        md,
        JDBC_NAMESPACE_HEADER
    );

    //Set SDC Operation Header
    int op = MSOperationCode.convertToJDBCCode(rs.getInt(MSQueryUtil.CDC_OPERATION));
    record.getHeader().setAttribute(OperationType.SDC_OPERATION_TYPE, String.valueOf(op));

    for (String fieldName : recordHeader) {
      try {
        record.getHeader().setAttribute(JDBC_NAMESPACE_HEADER + fieldName,
            rs.getString(fieldName) != null ? rs.getString(fieldName) : "NULL"
        );
      } catch (SQLException ex) {
        //no-op
        LOG.trace("the column name {} does not exists in the table: {}", fieldName, tableRuntimeContext.getQualifiedName());
      }
    }

    //Set Source Table Info Headers
    String tableName = tableRuntimeContext.getSourceTableContext().getTableName();
    String captureInstanceName = tableName.substring(0, tableName.length() - "_CT".length());
    SQLServerCDCSource.SourceTableInfo sourceTableInfo = infoMap.get(captureInstanceName);
    if (sourceTableInfo != null) {
      record.getHeader().setAttribute(
          JDBC_NAMESPACE_HEADER + CDC_NAMESPACE_HEADER + MSQueryUtil.SOURCE_SCHEMA_NAME,
          sourceTableInfo.schemaName
      );
      record.getHeader().setAttribute(
          JDBC_NAMESPACE_HEADER + CDC_NAMESPACE_HEADER + MSQueryUtil.SOURCE_NAME,
          sourceTableInfo.tableName
      );
    }

    batchContext.getBatchMaker().addRecord(record);

    offsets.put(tableRuntimeContext.getOffsetKey(), offsetFormat);
  }

  @Override
  protected void handlePostBatchAsNeeded(
      boolean resultSetEndReached,
      int recordCount,
      int eventCount,
      BatchContext batchContext
  ) {
    if (commonSourceConfigBean.txnWindow > 0) {
      // update the initial offset
      int oldTnxWindow = 0;
      Map<String, String> columnOffsets = OffsetQueryUtil.getColumnsToOffsetMapFromOffsetFormat(offsets.get(
          tableRuntimeContext.getOffsetKey())
      );

      if (recordCount == 0) {
        // add CDC_TXN_WINDOW by commonSourceConfigBean.txnWindow
        if (columnOffsets.get(MSQueryUtil.CDC_TXN_WINDOW) != null) {
          oldTnxWindow = Integer.parseInt(columnOffsets.get(MSQueryUtil.CDC_TXN_WINDOW));
        }
      }

      columnOffsets.put(MSQueryUtil.CDC_TXN_WINDOW, Integer.toString(oldTnxWindow + commonSourceConfigBean.txnWindow));
      String offsetFormat = OffsetQueryUtil.getOffsetFormat(columnOffsets);
      offsets.put(tableRuntimeContext.getOffsetKey(), offsetFormat);
    }

    super.handlePostBatchAsNeeded(resultSetEndReached, recordCount, eventCount, batchContext);
  }

  @Override
  public void generateSchemaChanges(BatchContext batchContext) throws SQLException {
    Map<String, Integer> source = new HashMap<>();
    ResultSet rs = tableReadContext.getMoreResultSet();
    String schemaName = "";
    String tableName = "";
    String captureInstanceName = "";

    if (rs != null && rs.next()) {
      ResultSetMetaData data = rs.getMetaData();

      for (int i = 1; i <= data.getColumnCount(); i++) {
        String label = data.getColumnLabel(i);
        if (label.equals(MSQueryUtil.CDC_SOURCE_SCHEMA_NAME)) {
          schemaName = rs.getString(label);
        } else if (label.equals(MSQueryUtil.CDC_SOURCE_TABLE_NAME)) {
          tableName = rs.getString(label);
        } else if (label.equals(MSQueryUtil.CDC_CAPTURE_INSTANCE_NAME)) {
          captureInstanceName = rs.getString(label);
        } else {
          int type = data.getColumnType(i);
          source.put(label, type);
        }
      }

      boolean schemaChanges = getDiff(captureInstanceName, source, tableRuntimeContext.getSourceTableContext().getColumnToType());

      if (schemaChanges) {
        JdbcEvents.SCHEMA_CHANGE.create(context, batchContext)
            .with("source-table-schema-name", schemaName)
            .with("source-table-name", tableName)
            .with("capture-instance-name", captureInstanceName)
            .createAndSend();
        context.processBatch(batchContext);
      }
    }
  }

  private boolean getDiff(String captureInstanceName, Map<String, Integer> sourceTableColumnInfo, Map<String, Integer> cdcTableColumnInfo) {
    MapDifference<String, Integer> diff = Maps.difference(sourceTableColumnInfo, cdcTableColumnInfo);

    if (!diff.areEqual()) {
      if (LOG.isTraceEnabled()) {
        LOG.trace(
            "Detected drift for table {} - new columns: {}, drop columns: {}",
            captureInstanceName,
            StringUtils.join(diff.entriesOnlyOnLeft().keySet(), ","),
            StringUtils.join(diff.entriesOnlyOnRight().keySet(), ",")
        );
      }
      return true;
    }

    return false;
  }
}
