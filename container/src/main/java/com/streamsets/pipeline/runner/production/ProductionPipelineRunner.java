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
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.DeliveryGuarantee;
import com.streamsets.pipeline.config.StageType;
import com.streamsets.pipeline.metrics.MetricsConfigurator;
import com.streamsets.pipeline.runner.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ProductionPipelineRunner implements PipelineRunner {

  private final MetricRegistry metrics;
  private final SourceOffsetTracker offsetTracker;
  private final int batchSize;
  private List<List<StageOutput>> stageOutput;
  private Timer processingTimer;
  private String sourceOffset;
  private String newSourceOffset;
  private DeliveryGuarantee deliveryGuarantee;
  private volatile boolean stop = false;


  public ProductionPipelineRunner(SourceOffsetTracker offsetTracker
    , int batchSize, DeliveryGuarantee deliveryGuarantee) {
    this.metrics = new MetricRegistry();
    this.offsetTracker = offsetTracker;
    this.batchSize = batchSize;
    processingTimer = MetricsConfigurator.createTimer(metrics, "pipeline.batchProcessing");
    this.deliveryGuarantee = deliveryGuarantee;
    stageOutput = new ArrayList<List<StageOutput>>();
  }

  @Override
  public MetricRegistry getMetrics() {
    return metrics;
  }

  @Override
  public void run(Pipe[] pipes) throws StageException, PipelineRuntimeException {
    while(!offsetTracker.isFinished() && !stop) {
      PipeBatch pipeBatch = new PipeBatch(offsetTracker, batchSize, false /*snapshot stage output*/);
      long start = System.currentTimeMillis();

      sourceOffset = pipeBatch.getPreviousOffset();
      for (Pipe pipe : pipes) {
        if (deliveryGuarantee == DeliveryGuarantee.AT_MOST_ONCE
          && pipe.getStage().getDefinition().getType() == StageType.TARGET) {
          //AT_MOST ONCE delivery
          offsetTracker.commitOffset();
        }
        pipe.process(pipeBatch);
      }
      if (deliveryGuarantee == DeliveryGuarantee.AT_LEAST_ONCE) {
        offsetTracker.commitOffset();
      }
      processingTimer.update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
      newSourceOffset = offsetTracker.getOffset();
      stageOutput.add(pipeBatch.getSnapshotsOfAllStagesOutput());
    }
  }

  @Override
  public List<List<StageOutput>> getBatchesOutput() {
    return stageOutput;
  }

  public String getSourceOffset() {
    return sourceOffset;
  }

  public String getNewSourceOffset() {
    return newSourceOffset;
  }

  public void stop() {
    this.stop = true;
  }
}
