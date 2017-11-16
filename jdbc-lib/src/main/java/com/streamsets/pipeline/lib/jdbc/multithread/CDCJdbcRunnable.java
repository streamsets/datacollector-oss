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
import com.streamsets.pipeline.lib.jdbc.JdbcUtil;
import com.streamsets.pipeline.lib.jdbc.MSOperationCode;
import com.streamsets.pipeline.lib.jdbc.multithread.util.MSQueryUtil;
import com.streamsets.pipeline.lib.jdbc.multithread.util.OffsetQueryUtil;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.streamsets.pipeline.stage.origin.jdbc.CommonSourceConfigBean;
import com.streamsets.pipeline.stage.origin.jdbc.JdbcEvents;
import com.streamsets.pipeline.stage.origin.jdbc.table.TableJdbcConfigBean;
import org.apache.commons.lang.StringUtils;
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
      RateLimiter queryRateLimiter
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
        queryRateLimiter
    );

    this.recordHeader = ImmutableSet.of(
        MSQueryUtil.CDC_START_LSN,
        MSQueryUtil.CDC_END_LSN,
        MSQueryUtil.CDC_SEQVAL,
        MSQueryUtil.CDC_OPERATION,
        MSQueryUtil.CDC_UPDATE_MASK,
        MSQueryUtil.CDC_COMMAND_ID
    );
  }

  @Override
  public void createAndAddRecord(
      ResultSet rs,
      TableRuntimeContext tableRuntimeContext,
      BatchContext batchContext
  ) throws SQLException, StageException {
    ResultSetMetaData md = rs.getMetaData();

    LinkedHashMap<String, Field> fields = JdbcUtil.resultSetToFields(
        rs,
        commonSourceConfigBean.maxClobSize,
        commonSourceConfigBean.maxBlobSize,
        errorRecordHandler,
        tableJdbcConfigBean.unknownTypeAction,
        recordHeader
    );

    Map<String, String> columnOffsets = new HashMap<>();

    // Generate Offset includes __$start_lsn and __$seqval
    for (String key : tableRuntimeContext.getSourceTableContext().getOffsetColumns()) {
      columnOffsets.put(key, rs.getString(key));
    }

    String offsetFormat = OffsetQueryUtil.getOffsetFormat(columnOffsets);

    Record record = context.createRecord(tableRuntimeContext.getQualifiedName() + "::" + offsetFormat);
    record.set(Field.createListMap(fields));

    //Set Column Headers
    JdbcUtil.setColumnSpecificHeaders(
        record,
        Collections.singleton(tableRuntimeContext.getSourceTableContext().getTableName()),
        md,
        JDBC_NAMESPACE_HEADER
    );

    for (String fieldName : recordHeader) {
      record.getHeader().setAttribute(JDBC_NAMESPACE_HEADER + fieldName, rs.getString(fieldName) != null ? rs.getString(fieldName) : "NULL" );
    }

    //Set SDC Operation Header
    int op = MSOperationCode.convertToJDBCCode(rs.getInt(MSQueryUtil.CDC_OPERATION));
    record.getHeader().setAttribute(OperationType.SDC_OPERATION_TYPE, String.valueOf(op));

    for (String fieldName : recordHeader) {
      record.getHeader().setAttribute(JDBC_NAMESPACE_HEADER + fieldName, rs.getString(fieldName) != null ? rs.getString(fieldName) : "NULL" );
    }

    batchContext.getBatchMaker().addRecord(record);

    offsets.put(tableRuntimeContext.getOffsetKey(), offsetFormat);
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
