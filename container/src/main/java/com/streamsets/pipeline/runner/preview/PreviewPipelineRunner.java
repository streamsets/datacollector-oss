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
import com.streamsets.pipeline.config.StageType;
import com.streamsets.pipeline.metrics.MetricsConfigurator;
import com.streamsets.pipeline.runner.FullPipeBatch;
import com.streamsets.pipeline.runner.Pipe;
import com.streamsets.pipeline.runner.PipeBatch;
import com.streamsets.pipeline.runner.PipelineRunner;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.runner.SourceOffsetTracker;
import com.streamsets.pipeline.runner.StageOutput;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class PreviewPipelineRunner implements PipelineRunner {
  private final SourceOffsetTracker offsetTracker;
  private final int batchSize;
  private final int batches;
  private final boolean skipTargets;
  private final MetricRegistry metrics;
  private final List<StageOutput> batchesOuptut;
  private String sourceOffset;
  private String newSourceOffset;
  private Timer processingTimer;

  public PreviewPipelineRunner(SourceOffsetTracker offsetTracker, int batchSize, int batches, boolean skipTargets) {
    this.offsetTracker = offsetTracker;
    this.batchSize = batchSize;
    this.batches = batches;
    this.skipTargets = skipTargets;
    this.metrics = new MetricRegistry();
    processingTimer = MetricsConfigurator.createTimer(metrics, "pipeline.batchProcessing");
    batchesOuptut = new ArrayList<StageOutput>();
  }

  @Override
  public MetricRegistry getMetrics() {
    return metrics;
  }

  @Override
  public void run(Pipe[] pipes) throws StageException, PipelineRuntimeException {
    for (int i = 0; i < batches; i++) {
      PipeBatch pipeBatch = new FullPipeBatch(offsetTracker, batchSize, true);
      long start = System.currentTimeMillis();
      sourceOffset = pipeBatch.getPreviousOffset();
      for (Pipe pipe : pipes) {
        if (!skipTargets || pipe.getStage().getDefinition().getType() != StageType.TARGET) {
          pipe.process(pipeBatch);
        }
      }
      offsetTracker.commitOffset();
      processingTimer.update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
      newSourceOffset = offsetTracker.getOffset();
      batchesOuptut.addAll(pipeBatch.getSnapshotsOfAllStagesOutput());
    }
  }

  @Override
  public List<StageOutput> getBatchesOutput() {
    return batchesOuptut;
  }


  public String getSourceOffset() {
    return sourceOffset;
  }

  public String getNewSourceOffset() {
    return newSourceOffset;
  }
}
