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

  private String tmpFilePath;
  private List<Record> records;
  private RecordWriter writer;
  private final long threadId;
  private final int MAX_RETRY = 3;

  public DataLakeWriterThread(RecordWriter writer, String tmpFilePath, List<Record> records) {
    this.writer = writer;
    this.tmpFilePath = tmpFilePath;
    this.records = records;
    threadId = Thread.currentThread().getId();
  }

  @Override
  public List<OnRecordErrorException> call() {
    int numErrorRecords = 0;
    LOG.debug("Thread {} starts to write {} records to {}", threadId, records.size(), tmpFilePath);
    List<OnRecordErrorException> errorRecords = new ArrayList<>();
    boolean isFlushed = false;

    for (int i = 0; i < records.size(); i++) {
      Record record = records.get(i);
      String dirPath = tmpFilePath.substring(0, tmpFilePath.lastIndexOf("/"));

      try {
        handleError(() -> writer.write(tmpFilePath, record));
      } catch (IOException | StageException ex) {

        if (!(ex instanceof ADLException)) {
          LOG.debug(Errors.ADLS_03.getMessage(), ex.toString(), ex);
        }

        // acutal throwing the error happening on the main thread
        errorRecords.add(new OnRecordErrorException(record, Errors.ADLS_03, ex.toString()));
        numErrorRecords++;
      }


      try {
        if (writer.shouldRoll(record, dirPath)) {
          handleError(() -> writer.commitOldFile(tmpFilePath));
          isFlushed = true;
        } else {
          isFlushed = false;
        }
      } catch (IOException | StageException ex) {

        if (!(ex instanceof ADLException)) {
          LOG.debug(Errors.ADLS_13.getMessage(), tmpFilePath, ex.toString(), ex);
        }
        // acutal throwing the error happening on the main thread
        errorRecords.add(new OnRecordErrorException(record, Errors.ADLS_13, ex.toString()));
        numErrorRecords++;
      }
    }


    if (!isFlushed) {
      try {
        handleError(() -> writer.flush(tmpFilePath));
        isFlushed = true;
      } catch (IOException | StageException ex) {
        // reset error records
        errorRecords.clear();
        numErrorRecords = 0;

        if (!(ex instanceof ADLException)) {
          LOG.debug(Errors.ADLS_03.getMessage(), ex.toString(), ex);
        }

        // send to error records
        for (Record record : records) {
          errorRecords.add(new OnRecordErrorException(record, Errors.ADLS_03, ex.toString()));
          numErrorRecords++;
        }
      }
    }

    if (isFlushed) {
      LOG.debug(
          "Thread {} ends to write {} out of {} records to {}",
          threadId,
          records.size() - numErrorRecords,
          records.size(),
          tmpFilePath
      );
    }

    return errorRecords;
  }

  private void handleError(RetryFunctionHandler retryFunctionHandler) throws IOException, StageException{
    int retry = 0;

    while (retry < MAX_RETRY) {
      try {
        retry++;
        retryFunctionHandler.run();
        return;
      } catch (ADLException ex) {
        int httpResponseCode = ex.httpResponseCode;

        LOG.debug(
            Errors.ADLS_14.getMessage(),
            ex.requestId,
            ex.httpResponseCode,
            ex.httpResponseMessage,
            ex.remoteExceptionMessage,
            ex.remoteExceptionName,
            ex.remoteExceptionJavaClassName,
            ex
        );

        if (httpResponseCode == 401 || httpResponseCode == 400) {
          try {
            writer.updateToken();
            LOG.debug("Thread {} obtained a renewed access token", threadId);
          } catch (IOException ex1) {
            LOG.error("Thread {} obtained a renewed access token failed retrying..", threadId);
          }
        } else {
          throw ex;
        }
      }
    }
  }

  private interface RetryFunctionHandler {
    void run() throws IOException, StageException;
  }
}
