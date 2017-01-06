/**
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.streamsets.pipeline.stage.destination.datalake.writer;

import com.microsoft.azure.datalake.store.ADLException;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.stage.destination.datalake.DataLakeTarget;
import com.streamsets.pipeline.stage.destination.datalake.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class DataLakeWriterThread implements Callable<List<OnRecordErrorException>> {
  private static final Logger LOG = LoggerFactory.getLogger(DataLakeTarget.class);

  private String filePath;
  private List<Record> records;
  private RecordWriter writer;

  public DataLakeWriterThread(RecordWriter writer, String filePath, List<Record> records) {
    this.writer = writer;
    this.filePath = filePath;
    this.records = records;
  }

  @Override
  public List<OnRecordErrorException> call() {
    long threadId = Thread.currentThread().getId();
    int numErrorRecords = 0;
    LOG.debug("Thread {} starts to write {} records to {}", threadId, records.size(), filePath);
    List<OnRecordErrorException> errorRecords = new ArrayList<>();

    for (int i = 0; i < records.size(); i++) {
      Record record = records.get(i);
      try {
        String dirPath = filePath.substring(0, filePath.lastIndexOf("/"));
        writer.write(filePath, record);

        if (writer.shouldRoll(record, dirPath)) {
          writer.flush(filePath);
          writer.commitOldFile(dirPath, filePath);
        }

      } catch (ADLException ex) {
        // acutal throwing the error happening on the main thread
        LOG.debug(Errors.ADLS_03.getMessage(), ex.remoteExceptionMessage, ex);
        errorRecords.add(new OnRecordErrorException(record, Errors.ADLS_03, ex.remoteExceptionMessage));
        numErrorRecords++;
      } catch (IOException | StageException ex) {
        // acutal throwing the error happening on the main thread
        LOG.debug(Errors.ADLS_03.getMessage(), ex.toString(), ex);
        errorRecords.add(new OnRecordErrorException(record, Errors.ADLS_03, ex.toString()));
        numErrorRecords++;
      }
    }

    LOG.debug("Thread {} ends to write {} records to {}", threadId, records.size() - numErrorRecords, filePath);
    return errorRecords;
  }
}