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
package com.streamsets.pipeline.runner.preview;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.metrics.MetricsConfigurator;
import com.streamsets.pipeline.runner.Pipe;
import com.streamsets.pipeline.runner.PipeBatch;
import com.streamsets.pipeline.runner.PipelineRunner;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.runner.SourceOffsetTracker;
import com.streamsets.pipeline.runner.StageOutput;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class PreviewPipelineRunner implements PipelineRunner {
  private final SourceOffsetTracker offsetTracker;
  private final int batchSize;
  private final MetricRegistry metrics;
  private List<StageOutput> stageOutput;
  private String sourceOffset;
  private String newSourceOffset;
  private Timer processingTimer;

  public PreviewPipelineRunner(SourceOffsetTracker offsetTracker, int batchSize) {
    this.offsetTracker = offsetTracker;
    this.batchSize = batchSize;
    this.metrics = new MetricRegistry();
    processingTimer = MetricsConfigurator.createTimer(metrics, "pipeline.batchProcessing");
  }

  @Override
  public MetricRegistry getMetrics() {
    return metrics;
  }

  @Override
  public void run(Pipe[] pipes) throws StageException, PipelineRuntimeException {
    PipeBatch pipeBatch = new PipeBatch(offsetTracker, metrics, batchSize, true);
    long start = System.currentTimeMillis();
    sourceOffset = pipeBatch.getPreviousOffset();
    for (Pipe pipe : pipes) {
      pipe.process(pipeBatch);
    }
    offsetTracker.commitOffset();
    processingTimer.update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
    newSourceOffset = offsetTracker.getOffset();
    stageOutput = pipeBatch.getSnapshotsOfAllStagesOutput();
  }

  @Override
  public List<StageOutput> getStagesOutputSnapshot() {
    return stageOutput;
  }


  public String getSourceOffset() {
    return sourceOffset;
  }

  public String getNewSourceOffset() {
    return newSourceOffset;
  }
}
