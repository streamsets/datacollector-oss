/**
 * Copyright 2016 StreamSets Inc.
 * <p>
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.processor.spark.cluster;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
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
import scala.Tuple2;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Semaphore;

import static com.streamsets.pipeline.stage.processor.spark.Errors.SPARK_04;

public class ClusterExecutorSparkProcessor extends SingleLaneProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(ClusterExecutorSparkProcessor.class);

  private final Semaphore batchReceived = new Semaphore(0);
  private final Semaphore batchTransformed = new Semaphore(0);

  private ErrorRecordHandler errorRecordHandler;

  private Iterator<Record> batch;
  private SingleLaneBatchMaker currentBatchMaker;
  private Iterator<Tuple2<Record, String>> errors;

  @Override
  public List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    // No op.
    return issues;
  }

  public Iterator<Record> getBatch() {
    try {
      batchReceived.acquire();
    } catch (InterruptedException ex) { // NOSONAR
      LOG.warn("Interrupted while waiting for batch to be received", ex);
      return null;
    }
    return batch;
  }

  @Override
  public void process(Batch batch, SingleLaneBatchMaker singleLaneBatchMaker) throws StageException {
    this.batch = batch.getRecords();
    this.currentBatchMaker = singleLaneBatchMaker;
    batchReceived.release();

    try {
      batchTransformed.acquire();
    } catch (InterruptedException ex) { // NOSONAR
      LOG.error("Interrupted while waiting for batch to be processed", ex);
      throw Throwables.propagate(ex);
    }

    if (errors != null) {
      while (errors.hasNext()) {
        Tuple2<Record, String> error = errors.next();
        errorRecordHandler.onError(
            new OnRecordErrorException(RecordCloner.clone(error._1(), getContext()), SPARK_04, error._2()));
      }
    }
  }

  public void setErrors(Iterator<Tuple2<Record, String>> errors) throws StageException {
    this.errors = errors;
  }

  public void continueProcessing(Iterator<Record> transformed) {
    while(transformed.hasNext()) {
      currentBatchMaker.addRecord(transformed.next());
    }
    currentBatchMaker = null;
    batchTransformed.release();
  }
}
