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
package com.streamsets.pipeline.stage.processor.spark.cluster;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneProcessor;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.processor.spark.util.RecordCloner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Semaphore;

import static com.streamsets.pipeline.stage.processor.spark.Errors.SPARK_04;

public class ClusterExecutorSparkProcessor extends SingleLaneProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(ClusterExecutorSparkProcessor.class);
  public static final String CLUSTER_ERROR_REASON_HDR = "streamsetsInternalClusterErrorReason";

  private final Semaphore batchReceived = new Semaphore(0);
  private final Semaphore batchTransformed = new Semaphore(0);

  private static final boolean IS_DEBUG_ENABLED = LOG.isDebugEnabled();

  private ErrorRecordHandler errorRecordHandler;

  private Iterator<Record> batch;
  private SingleLaneBatchMaker currentBatchMaker;

  @Override
  public List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    // No op.
    return issues;
  }

  public Iterator<Record> getBatch() {
    try {
      LOG.debug("Trying to read batch at {}", System.currentTimeMillis());
      batchReceived.acquire();
    } catch (InterruptedException ex) { // NOSONAR
      LOG.warn("Interrupted while waiting for batch to be received", ex);
      return null;
    }
    LOG.debug("Returning received batch");
    synchronized (this) {
      if (batch == null) {
        return Collections.emptyIterator();
      }
      return batch;
    }
  }

  @Override
  public void process(Batch batch, SingleLaneBatchMaker singleLaneBatchMaker) throws StageException {
    synchronized (this) {
      this.batch = batch.getRecords();
    }
    this.currentBatchMaker = singleLaneBatchMaker;
    batchReceived.release();

    try {
      batchTransformed.acquire();
    } catch (InterruptedException ex) { // NOSONAR
      LOG.error("Interrupted while waiting for batch to be processed", ex);
      throw new RuntimeException(ex);
    }

  }

  private Iterator<Record> clone(Iterator<Object> records) {
    List<Record> cloned = new ArrayList<>();
    records.forEachRemaining(record -> cloned.add(RecordCloner.clone(record, getContext())));
    return cloned.iterator();
  }

  @SuppressWarnings("unchecked")
  public void setErrors(Iterator<Object> errors) throws StageException {
    errors.forEachRemaining(error -> {
      try {
        Record cloned = RecordCloner.clone(error, getContext());
        String reason = cloned.getHeader().getAttribute(CLUSTER_ERROR_REASON_HDR);
        errorRecordHandler.onError(
            new OnRecordErrorException(cloned, SPARK_04, reason));
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    });
  }

  public void continueProcessing(Iterator<Object> transformed) {
    clone(transformed).forEachRemaining(currentBatchMaker::addRecord);
    currentBatchMaker = null;
    synchronized (this) {
      batch = null;
    }
    batchTransformed.release();
  }

}
