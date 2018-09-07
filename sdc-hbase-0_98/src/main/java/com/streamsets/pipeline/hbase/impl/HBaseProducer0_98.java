/*
 * Copyright 2018 StreamSets Inc.
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

package com.streamsets.pipeline.hbase.impl;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.hbase.api.common.producer.ColumnInfo;
import com.streamsets.pipeline.hbase.api.common.Errors;
import com.streamsets.pipeline.hbase.api.common.producer.Groups;
import com.streamsets.pipeline.hbase.api.common.producer.HBaseConnectionConfig;
import com.streamsets.pipeline.hbase.api.common.producer.StorageType;
import com.streamsets.pipeline.hbase.api.impl.AbstractHBaseConnectionHelper;
import com.streamsets.pipeline.hbase.api.impl.AbstractHBaseProducer;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class HBaseProducer0_98 extends AbstractHBaseProducer {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseProducer0_98.class);

  private HTable hTable = null;

  public HBaseProducer0_98(
      Stage.Context context, HBaseConnectionConfig conf, ErrorRecordHandler errorRecordHandler
  ) {
    super(context, new HBaseConnectionHelper0_98(), conf, errorRecordHandler);
  }

  @Override
  public void writeRecordsInHBase(
      Batch batch,
      StorageType rowKeyStorageType,
      String hbaseRowKey,
      String timeDriver,
      Map<String, ColumnInfo> columnMappings,
      boolean ignoreMissingField,
      boolean implicitFieldMapping,
      boolean ignoreInvalidColumn
  ) throws StageException {
    Iterator<Record> it = batch.getRecords();
    Map<String, Record> rowKeyToRecord = new HashMap<>();
    try {
      hTable.setAutoFlushTo(false);
      while (it.hasNext()) {
        Record record = it.next();
        rowKeyToRecord = doPut(
            rowKeyToRecord,
            hTable,
            record,
            rowKeyStorageType,
            hbaseRowKey,
            timeDriver,
            columnMappings,
            ignoreMissingField,
            implicitFieldMapping,
            ignoreInvalidColumn
        );
      }
      hTable.flushCommits();
    } catch (RetriesExhaustedWithDetailsException rex) {
      LOG.debug("Got exception while flushing commits to HBase", rex);
      AbstractHBaseConnectionHelper.handleHBaseException(rex, null, rowKeyToRecord, errorRecordHandler);
    } catch (OnRecordErrorException ex) {
      LOG.debug("Got exception while writing to HBase", ex);
      errorRecordHandler.onError(ex);
    } catch (IOException ex) {
      LOG.debug("Got exception while flushing commits to HBase", ex);
      throw new StageException(Errors.HBASE_02, ex);
    }
  }

  @Override
  public void destroyTable() {
    if (hTable != null) {
      try {
        hbaseConnectionHelper.getUGI().doAs((PrivilegedExceptionAction<Void>) () -> {
          hTable.close();
          return null;
        });

      } catch (InterruptedException | IOException ex) {
        LOG.debug("error closing HBase table {}", ex.getMessage(), ex);
      }
    }
  }

  @Override
  public void createTable(String tableName) throws InterruptedException, IOException {
    hbaseConnectionHelper.getUGI().doAs((PrivilegedExceptionAction<Void>) () -> {
      hTable = new HTable(hbaseConnectionHelper.getHBaseConfiguration(), tableName);
      return null;
    });
  }

  @Override
  public void checkHBaseAvailable(List<Stage.ConfigIssue> issues) {
    try {
      HBaseAdmin.checkHBaseAvailable(hbaseConnectionHelper.getHBaseConfiguration());
    } catch (Exception ex) {
      LOG.warn("Received exception while connecting to cluster: ", ex);
      issues.add(getContext().createConfigIssue(Groups.HBASE.name(), null, Errors.HBASE_06, ex.toString(), ex));
    }
  }

  @Override
  public void addCell(Put p, byte[] columnFamily, byte[] qualifier, Date recordTime, byte[] value) {
    if (recordTime != null) {
      p.add(columnFamily, qualifier, recordTime.getTime(), value);
    } else {
      p.add(columnFamily, qualifier, value);
    }
  }

  private Map<String, Record> doPut(
      Map<String, Record> rowKeyToRecord,
      HTable hTable,
      Record record,
      StorageType rowKeyStorageType,
      String hbaseRowKey,
      String timeDriver,
      Map<String, ColumnInfo> columnMappings,
      boolean ignoreMissingField,
      boolean implicitFieldMapping,
      boolean ignoreInvalidColumn
  ) throws IOException, StageException {
    try {
      byte[] rowKeyBytes = getBytesForRowKey(record, rowKeyStorageType, hbaseRowKey);
      // Map hbase rows to sdc records.
      Put p = getHBasePut(
          record,
          rowKeyBytes,
          timeDriver,
          columnMappings,
          ignoreMissingField,
          implicitFieldMapping,
          ignoreInvalidColumn,
          hbaseRowKey
      );
      rowKeyToRecord.put(Bytes.toString(rowKeyBytes), record);
      performPut(hTable, record, p);
    } catch (OnRecordErrorException ex) {
      LOG.debug("Got exception while writing to HBase", ex);
      errorRecordHandler.onError(ex);
    }
    return rowKeyToRecord;
  }

  private void performPut(HTable hTable, Record record, Put p) throws StageException, IOException {
    try {
      // HTable internally keeps a buffer, a put() will keep on buffering till the buffer
      // limit is reached
      // Once it hits the buffer limit or autoflush is set to true, commit will happen
      hTable.put(p);
    } catch (RetriesExhaustedWithDetailsException rex) {
      // There may be more than one row which failed to persist
      AbstractHBaseConnectionHelper.handleHBaseException(rex, record, null, errorRecordHandler);
    }
  }

}
