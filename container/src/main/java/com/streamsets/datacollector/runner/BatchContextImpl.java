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
package com.streamsets.datacollector.runner;

import com.google.common.base.Preconditions;
import com.streamsets.datacollector.record.EventRecordImpl;
import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.ErrorMessage;

import java.util.Collection;
import java.util.List;

/**
 * BatchContext implementation keeping all the state for given Batch while the pipeline is running the origin's code.
 */
public class BatchContextImpl implements BatchContext {

  /**
   * Pipe batch associated with this batch.
   */
  private FullPipeBatch pipeBatch;

  /**
   * Batch maker associated with the origin's stage.
   */
  private BatchMaker batchMaker;

  /**
   * Start time of the batch execution.
   */
  private long startTime;

  /**
   * Internal and unique stage name of the source to properly route event and error records.
   */
  private String originStageName;

  /**
   * User stage label of the source to properly route event and error records.
   */
  private String originStageLabel;

  /**
   * Flag identifying whether this batch was already processed or not.
   */
  private boolean processed;

  /**
   * Record Cloner that knows how the records should be cloned (if even).
   */
  private final RecordCloner recordCloner;

  public BatchContextImpl(FullPipeBatch pipeBatch, boolean recordByRef) {
    this.pipeBatch = pipeBatch;
    this.processed = false;
    this.startTime = System.currentTimeMillis();
    this.recordCloner = new RecordCloner(recordByRef);
  }

  @Override
  @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
  public void toError(Record record, Exception ex) {
    Preconditions.checkNotNull(record, "record cannot be null");
    Preconditions.checkNotNull(ex, "exception cannot be null");
    if (ex instanceof StageException) {
      toError(record, new ErrorMessage((StageException) ex));
    } else {
      toError(record, new ErrorMessage(ContainerError.CONTAINER_0001, ex.toString(), ex));
    }
  }

  @Override
  public void toError(Record record, String errorMessage) {
    Preconditions.checkNotNull(record, "record cannot be null");
    Preconditions.checkNotNull(errorMessage, "errorMessage cannot be null");
    toError(record, new ErrorMessage(ContainerError.CONTAINER_0001, errorMessage));
  }

  @Override
  public void toError(Record record, ErrorCode errorCode, Object... args) {
    Preconditions.checkNotNull(record, "record cannot be null");
    Preconditions.checkNotNull(errorCode, "errorId cannot be null");
    // the last args needs to be Exception in order to show stack trace
    toError(record, new ErrorMessage(errorCode, args));
  }

  private void toError(Record record, ErrorMessage errorMessage) {
    RecordImpl recordImpl = recordCloner.cloneRecordIfNeeded(record);
    if (recordImpl.isInitialRecord()) {
      recordImpl.getHeader().setSourceRecord(recordImpl);
      recordImpl.setInitialRecord(false);
    }
    recordImpl.getHeader().setError(originStageName, originStageLabel, errorMessage);
    pipeBatch.getErrorSink().addRecord(originStageName, recordImpl);
  }

  @Override
  public void toEvent(EventRecord record) {
    EventRecordImpl recordImpl = recordCloner.cloneEventIfNeeded(record);
    if (recordImpl.isInitialRecord()) {
      recordImpl.getHeader().setSourceRecord(recordImpl);
      recordImpl.setInitialRecord(false);
    }
    pipeBatch.getEventSink().addEvent(originStageName, recordImpl);
  }

  @Override
  public void complete(Record record) {
    pipeBatch.getProcessedSink().addRecord(originStageName, record);
  }

  @Override
  public void complete(Collection<Record> records) {
    pipeBatch.getProcessedSink().addRecords(originStageName, records);
  }

  @Override
  public BatchMaker getBatchMaker() {
    return batchMaker;
  }

  public void setBatchMaker(BatchMaker batchMaker) {
    this.batchMaker = batchMaker;
  }

  public void setOriginStageName(String originStage, String originStageLabel) {
    this.originStageName = originStage;
    this.originStageLabel = originStageLabel;
  }

  public FullPipeBatch getPipeBatch() {
    return pipeBatch;
  }

  public long getStartTime() {
    return startTime;
  }

  @Override
  public List<Record> getSourceResponseRecords() {
    return pipeBatch.getSourceResponseSink().getResponseRecords();
  }

  public void setProcessed(boolean processed) {
    this.processed = processed;
  }

  public void ensureState() {
    Preconditions.checkState(!processed, "Batch was already processed and can't be processed again.");
  }
}
