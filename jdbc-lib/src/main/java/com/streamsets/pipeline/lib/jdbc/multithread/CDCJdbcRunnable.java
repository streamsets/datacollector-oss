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
import com.streamsets.pipeline.lib.jdbc.multithread.util.MSQueryUtil;
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
      CacheLoader<TableRuntimeContext, TableReadContext> tableReadContextCache
  ) {
    super(context, threadNumber, batchSize, offsets, tableProvider, connectionManager, tableJdbcConfigBean, commonSourceConfigBean, tableReadContextCache);

    this.recordHeader = ImmutableSet.of(
        MSQueryUtil.CDC_START_LSN,
        MSQueryUtil.CDC_END_LSN,
        MSQueryUtil.CDC_SEQVAL,
        MSQueryUtil.CDC_OPERATION,
        MSQueryUtil.CDC_UPDATE_MASK
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
}
