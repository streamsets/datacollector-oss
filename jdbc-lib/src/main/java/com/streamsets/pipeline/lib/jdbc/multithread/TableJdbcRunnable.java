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
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.jdbc.JdbcUtil;
import com.streamsets.pipeline.stage.origin.jdbc.CommonSourceConfigBean;
import com.streamsets.pipeline.lib.jdbc.multithread.util.OffsetQueryUtil;
import com.streamsets.pipeline.stage.origin.jdbc.table.TableJdbcConfigBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public final class TableJdbcRunnable extends JdbcBaseRunnable {
  private static final Logger LOG = LoggerFactory.getLogger(TableJdbcRunnable.class);

  public TableJdbcRunnable(PushSource.Context context,
      int threadNumber,
      int batchSize,
      Map<String, String> offsets,
      MultithreadedTableProvider tableProvider,
      ConnectionManager connectionManager,
      TableJdbcConfigBean tableJdbcConfigBean,
      CommonSourceConfigBean commonSourceConfigBean,
      CacheLoader<TableRuntimeContext, TableReadContext> tableReadContextCache) {
    super(context, threadNumber, batchSize, offsets, tableProvider, connectionManager, tableJdbcConfigBean, commonSourceConfigBean, tableReadContextCache);

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
        tableJdbcConfigBean.unknownTypeAction
    );

    // TODO: change offset format here for incremental mode (finished=true if result set end reached)

    String offsetValue = null;

    if (!tableRuntimeContext.isUsingNonIncrementalLoad()) {
      final Map<String, String> columnsToOffsets = OffsetQueryUtil.getOffsetsFromColumns(tableRuntimeContext, fields);
      columnsToOffsets.forEach((col, off) -> tableRuntimeContext.recordColumnOffset(col, String.valueOf(off)));

      offsetValue = OffsetQueryUtil.getOffsetFormat(columnsToOffsets);
    }

    Record record = context.createRecord(tableRuntimeContext.getQualifiedName() + ":" + offsetValue);
    record.set(Field.createListMap(fields));

    //Set Column Headers
    JdbcUtil.setColumnSpecificHeaders(
        record,
        Collections.singleton(tableRuntimeContext.getSourceTableContext().getTableName()),
        md,
        JDBC_NAMESPACE_HEADER
    );

    record.getHeader().setAttribute(PARTITION_ATTRIBUTE, tableRuntimeContext.getDescription());
    record.getHeader().setAttribute(THREAD_NUMBER_ATTRIBUTE, String.valueOf(threadNumber));

    batchContext.getBatchMaker().addRecord(record);

    if (offsetValue != null) {
      offsets.put(tableRuntimeContext.getOffsetKey(), offsetValue);
    }
  }
}
