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
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.jdbc.JdbcUtil;
import com.streamsets.pipeline.lib.jdbc.MSOperationCode;
import com.streamsets.pipeline.lib.jdbc.multithread.util.OffsetQueryUtil;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.streamsets.pipeline.stage.origin.jdbc.CommonSourceConfigBean;
import com.streamsets.pipeline.stage.origin.jdbc.table.TableJdbcConfigBean;

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

public final class CTJdbcRunnable extends JdbcBaseRunnable {
  private static final Logger LOG = LoggerFactory.getLogger(CTJdbcRunnable.class);

  private static final String SYS_CHANGE_VERSION = "SYS_CHANGE_VERSION";
  private static final String SYS_CHANGE_CREATION_VERSION = "SYS_CHANGE_CREATION_VERSION";
  private static final String SYS_CHANGE_OPERATION = "SYS_CHANGE_OPERATION";
  private static final String SYS_CHANGE_COLUMNS = "SYS_CHANGE_COLUMNS";
  private static final String SYS_CHANGE_CONTEXT = "SYS_CHANGE_CONTEXT";

  private final Set<String> recordHeader;

  public CTJdbcRunnable(
      PushSource.Context context,
      int threadNumber,
      int batchSize,
      Map<String, String> offsets,
      MultithreadedTableProvider tableProvider,
      ConnectionManager connectionManager,
      TableJdbcConfigBean tableJdbcConfigBean,
      CommonSourceConfigBean commonSourceConfigBean,
      CacheLoader<TableRuntimeContext, TableReadContext> tableReadContextCache
  ) {
    super(context, threadNumber, batchSize, offsets, tableProvider, connectionManager, tableJdbcConfigBean, commonSourceConfigBean, tableReadContextCache);

    this.recordHeader = ImmutableSet.of(
        SYS_CHANGE_VERSION,
        SYS_CHANGE_CREATION_VERSION,
        SYS_CHANGE_OPERATION,
        SYS_CHANGE_COLUMNS,
        SYS_CHANGE_CONTEXT
    );
  }

  /**
   * Create record and add it to {@link com.streamsets.pipeline.api.BatchMaker}
   */
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


    // Generate Offset includes primary keys, sys_change_version, and sys_change_operation
    for (String key : tableRuntimeContext.getSourceTableContext().getOffsetColumns()) {
      columnOffsets.put(key, rs.getString(key));
    }

    columnOffsets.put(SYS_CHANGE_OPERATION, rs.getString(SYS_CHANGE_OPERATION));

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

    //Set Operation Headers
    int op = MSOperationCode.convertToJDBCCode(rs.getString(SYS_CHANGE_OPERATION));
    record.getHeader().setAttribute(OperationType.SDC_OPERATION_TYPE, String.valueOf(op));

    for (String fieldName : recordHeader) {
      record.getHeader().setAttribute(JDBC_NAMESPACE_HEADER + fieldName, rs.getString(fieldName) != null ? rs.getString(fieldName) : "NULL" );
    }

    batchContext.getBatchMaker().addRecord(record);

    offsets.put(tableRuntimeContext.getOffsetKey(), offsetFormat);
  }
}
