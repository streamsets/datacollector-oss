/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.runner.production;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.DeliveryGuarantee;
import com.streamsets.pipeline.config.StageType;
import com.streamsets.pipeline.metrics.MetricsConfigurator;
import com.streamsets.pipeline.runner.*;
import com.streamsets.pipeline.snapshotstore.SnapshotStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ProductionPipelineRunner implements PipelineRunner {

  private static final Logger LOG = LoggerFactory.getLogger(ProductionPipelineRunner.class);

  private final MetricRegistry metrics;
  private final SourceOffsetTracker offsetTracker;
  private final SnapshotStore snapshotStore;
  private int batchSize;
  private Timer processingTimer;
  private String sourceOffset;
  private String newSourceOffset;
  private DeliveryGuarantee deliveryGuarantee;


  /*For each batch of data, holds output from each stage for that batch*/
  private List<List<StageOutput>> batchOutputs;
  /*indicates if the execution must be stopped after the current batch*/
  private volatile boolean stop = false;
  /*indicates if the next batch of data should be captured, only the next batch*/
  private volatile boolean captureNextBatch = false;
  /*indicates the batch size to be captured*/
  private volatile int snapshotBatchSize;

  public ProductionPipelineRunner(SnapshotStore snapshotStore, SourceOffsetTracker offsetTracker
    , int batchSize, DeliveryGuarantee deliveryGuarantee) {
    this.metrics = new MetricRegistry();
    this.offsetTracker = offsetTracker;
    this.batchSize = batchSize;
    processingTimer = MetricsConfigurator.createTimer(metrics, "pipeline.batchProcessing");
    this.deliveryGuarantee = deliveryGuarantee;
    batchOutputs = new ArrayList<List<StageOutput>>();
    this.snapshotStore = snapshotStore;
  }

  @Override
  public MetricRegistry getMetrics() {
    return metrics;
  }

  @Override
  public void run(Pipe[] pipes) throws StageException, PipelineRuntimeException {
    while(!offsetTracker.isFinished() && !stop) {
      runBatch(pipes);
    }
  }

  @Override
  public List<List<StageOutput>> getBatchesOutput() {
    return batchOutputs;
  }

  public String getSourceOffset() {
    return sourceOffset;
  }

  public String getNewSourceOffset() {
    return newSourceOffset;
  }

  public String getCommittedOffset() {
    return offsetTracker.getOffset();
  }

  /**
   * Stops execution of the pipeline after the current batch completes
   */
  public void stop() {
    this.stop = true;
  }

  public boolean wasStopped() {
    return stop;
  }

  public void captureNextBatch(int batchSize) {
    Preconditions.checkArgument(batchSize > 0);
    this.snapshotBatchSize = batchSize;
    this.captureNextBatch = true;

  }

  private void runBatch(Pipe[] pipes) throws PipelineRuntimeException, StageException {
    boolean committed = false;
    PipeBatch pipeBatch;

    if(captureNextBatch) {
      pipeBatch = new FullPipeBatch(offsetTracker, snapshotBatchSize, true /*snapshot stage output*/);
    } else {
      pipeBatch = new FullPipeBatch(offsetTracker, batchSize, false /*snapshot stage output*/);
    }

    long start = System.currentTimeMillis();
    sourceOffset = pipeBatch.getPreviousOffset();
    for (Pipe pipe : pipes) {
      //TODO Define an interface to handle delivery guarantee
      if (deliveryGuarantee == DeliveryGuarantee.AT_MOST_ONCE
          && pipe.getStage().getDefinition().getType() == StageType.TARGET && !committed) {
        offsetTracker.commitOffset();
        committed = true;
      }
      pipe.process(pipeBatch);
    }
    if (deliveryGuarantee == DeliveryGuarantee.AT_LEAST_ONCE) {
      offsetTracker.commitOffset();
    }
    processingTimer.update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
    newSourceOffset = offsetTracker.getOffset();
    List<StageOutput> snapShot = pipeBatch.getSnapshotsOfAllStagesOutput();
    if(snapShot != null) {
      snapshotStore.storeSnapshot(snapShot);
      batchOutputs.add(snapShot);
      afterSnapshot();
    }

  }

  private void afterSnapshot() {
    /*
    * Reset the capture snapshot variable only after capturing the snapshot
    * This guarantees that once captureSnapshot is called, the output is captured exactly once
    * */
    captureNextBatch = false;
    snapshotBatchSize = 0;
  }

}
