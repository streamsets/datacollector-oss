/**
 * Copyright 2015 StreamSets Inc.
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
package com.streamsets.datacollector.runner.preview;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.streamsets.datacollector.config.StageType;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.metrics.MetricsConfigurator;
import com.streamsets.datacollector.restapi.bean.MetricRegistryJson;
import com.streamsets.datacollector.runner.BatchContextImpl;
import com.streamsets.datacollector.runner.BatchListener;
import com.streamsets.datacollector.runner.FullPipeBatch;
import com.streamsets.datacollector.runner.MultiplexerPipe;
import com.streamsets.datacollector.runner.Observer;
import com.streamsets.datacollector.runner.ObserverPipe;
import com.streamsets.datacollector.runner.Pipe;
import com.streamsets.datacollector.runner.PipeBatch;
import com.streamsets.datacollector.runner.PipeContext;
import com.streamsets.datacollector.runner.PipelineRunner;
import com.streamsets.datacollector.runner.PipelineRuntimeException;
import com.streamsets.datacollector.runner.PushSourceContextDelegate;
import com.streamsets.datacollector.runner.RunnerPool;
import com.streamsets.datacollector.runner.SourceOffsetTracker;
import com.streamsets.datacollector.runner.SourcePipe;
import com.streamsets.datacollector.runner.StageContext;
import com.streamsets.datacollector.runner.StageOutput;
import com.streamsets.datacollector.runner.StagePipe;
import com.streamsets.datacollector.runner.production.BadRecordsHandler;
import com.streamsets.datacollector.runner.production.StatsAggregationHandler;
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.StageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class PreviewPipelineRunner implements PipelineRunner, PushSourceContextDelegate {

  private static final Logger LOG = LoggerFactory.getLogger(PreviewPipelineRunner.class);

  private final RuntimeInfo runtimeInfo;
  private final SourceOffsetTracker offsetTracker;
  private final int batchSize;
  private final int batches;
  private final boolean skipTargets;
  private final MetricRegistry metrics;
  private final List<List<StageOutput>> batchesOutput;
  private final String name;
  private final String rev;
  private String sourceOffset;
  private String newSourceOffset;
  private final Timer processingTimer;

  private SourcePipe originPipe;
  private List<List<Pipe>> pipes;
  private RunnerPool<List<Pipe>> runnerPool;
  private BadRecordsHandler badRecordsHandler;
  private StatsAggregationHandler statsAggregationHandler;
  private Map<String, StageOutput> stagesToSkip;
  private AtomicInteger batchesProcessed;

  public PreviewPipelineRunner(String name, String rev, RuntimeInfo runtimeInfo, SourceOffsetTracker offsetTracker,
                               int batchSize, int batches, boolean skipTargets) {
    this.name = name;
    this.rev = rev;
    this.runtimeInfo = runtimeInfo;
    this.offsetTracker = offsetTracker;
    this.batchSize = batchSize;
    this.batches = batches;
    this.skipTargets = skipTargets;
    this.metrics = new MetricRegistry();
    processingTimer = MetricsConfigurator.createTimer(metrics, "pipeline.batchProcessing", name, rev);
    batchesOutput = Collections.synchronizedList(new ArrayList<List<StageOutput>>());
  }

  @Override
  public MetricRegistryJson getMetricRegistryJson() {
    return null;
  }

  @Override
  public void errorNotification(SourcePipe originPipe, List<List<Pipe>> pipes, Throwable throwable) {
  }

  @Override
  public RuntimeInfo getRuntimeInfo() {
    return runtimeInfo;
  }

  @Override
  public boolean isPreview() {
    return true;
  }

  @Override
  public MetricRegistry getMetrics() {
    return metrics;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void run(
    SourcePipe originPipe,
    List<List<Pipe>> pipes,
    BadRecordsHandler badRecordsHandler,
    StatsAggregationHandler statsAggregationHandler
  ) throws StageException, PipelineRuntimeException {
    run(originPipe, pipes, badRecordsHandler, Collections.EMPTY_LIST, statsAggregationHandler);
  }

  @Override
  public void run(
    SourcePipe originPipe,
    List<List<Pipe>> pipes,
    BadRecordsHandler badRecordsHandler,
    List<StageOutput> stageOutputsToOverride,
    StatsAggregationHandler statsAggregationHandler
  ) throws StageException, PipelineRuntimeException {
    this.originPipe = originPipe;
    this.pipes = pipes;
    this.badRecordsHandler = badRecordsHandler;
    this.statsAggregationHandler = statsAggregationHandler;
    this.runnerPool = new RunnerPool<>(pipes);

    stagesToSkip = new HashMap<>();
    for (StageOutput stageOutput : stageOutputsToOverride) {
      stagesToSkip.put(stageOutput.getInstanceName(), stageOutput);
    }

    if (originPipe.getStage().getStage() instanceof PushSource) {
      runPushSource();
    } else {
      runPollSource();
    }
  }

  private void runPushSource() throws StageException, PipelineRuntimeException {
    // This object will receive delegated calls from the push origin callbacks
    originPipe.getStage().setPushSourceContextDelegate(this);

    // Counter of batches that were already processed
    batchesProcessed = new AtomicInteger(0);

    if(stagesToSkip.containsKey(originPipe.getStage().getInfo().getInstanceName())) {
      // We're skipping the origin's execution, so let's run the pipeline in "usual" manner
      runPollSource();
    } else {
      // Push origin will block on the call until the either all data have been consumed or the pipeline stopped
      originPipe.process(offsetTracker.getOffsets(), batchSize);
    }
  }

  @Override
  public BatchContext startBatch() {
    FullPipeBatch pipeBatch = new FullPipeBatch(offsetTracker, batchSize, true);
    BatchContextImpl batchContext = new BatchContextImpl(pipeBatch);

    originPipe.prepareBatchContext(batchContext);

    return batchContext;
  }

  @Override
  public boolean processBatch(BatchContext batchCtx, String entityName, String entityOffset) {
    try {
      BatchContextImpl batchContext = (BatchContextImpl) batchCtx;

      // Finish origin processing
      originPipe.finishBatchContext(batchContext);

      // Run rest of the pipeline
      runSourceLessBatch(batchContext.getStartTime(), batchContext.getPipeBatch());

      // Increment amount of intercepted batches by one and end the processing if we have desirable amount
      int count = batchesProcessed.incrementAndGet();
      if(count >= batches) {
        ((StageContext)originPipe.getStage().getContext()).setStop(true);
      }

      // Not doing any commits in the preview
      return true;
    } catch (StageException|PipelineRuntimeException e) {
      LOG.error("Error while executing preview", e);
      return  false;
    }
  }

  @Override
  public void commitOffset(String entityName, String entityOffset) {
    // Not doing anything in preview
  }

  private void runPollSource() throws StageException, PipelineRuntimeException {
    for (int i = 0; i < batches; i++) {
      FullPipeBatch pipeBatch = new FullPipeBatch(offsetTracker, batchSize, true);
      long start = System.currentTimeMillis();

        // Process origin data
        StageOutput originOutput = stagesToSkip.get(originPipe.getStage().getInfo().getInstanceName());
        if(originOutput == null) {
          originPipe.process(pipeBatch);
        } else {
          pipeBatch.overrideStageOutput(originPipe, originOutput);
        }

        runSourceLessBatch(start, pipeBatch);
      }
    }

  private void runSourceLessBatch(long start, FullPipeBatch pipeBatch) throws StageException, PipelineRuntimeException {
    sourceOffset = pipeBatch.getPreviousOffset();

    List<Pipe> runnerPipes = null;
    try {
      runnerPipes = runnerPool.getRunner();
      for (Pipe pipe : pipes.get(0)) {
        StageOutput stageOutput = stagesToSkip.get(pipe.getStage().getInfo().getInstanceName());
        if (stageOutput == null || (pipe instanceof ObserverPipe) || (pipe instanceof MultiplexerPipe)) {
          if (!skipTargets || !pipe.getStage().getDefinition().getType().isOneOf(StageType.TARGET, StageType.EXECUTOR)) {
            pipe.process(pipeBatch);
          } else {
            pipeBatch.skipStage(pipe);
          }
        } else {
          if (pipe instanceof StagePipe) {
            pipeBatch.overrideStageOutput((StagePipe) pipe, stageOutput);
          }
        }
      }
    } finally {
      if(runnerPipes != null) {
        runnerPool.returnRunner(runnerPipes);
      }
    }

    offsetTracker.commitOffset();
    //TODO badRecordsHandler HANDLE ERRORS
    processingTimer.update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
    newSourceOffset = offsetTracker.getOffset();
    batchesOutput.add(pipeBatch.getSnapshotsOfAllStagesOutput());
  }

  @Override
  public void destroy(
    SourcePipe originPipe,
    List<List<Pipe>> pipes,
    BadRecordsHandler badRecordsHandler,
    StatsAggregationHandler statsAggregationHandler
  ) throws StageException, PipelineRuntimeException {
    // We're not doing any special event propagation during preview destroy phase

    // Destroy origin on it's own
    PipeBatch pipeBatch = new FullPipeBatch(offsetTracker, batchSize, true);
    originPipe.destroy(pipeBatch);

    // And destroy each pipeline instance separately
    for(List<Pipe> pipeRunner: pipes) {
      pipeBatch = new FullPipeBatch(offsetTracker, batchSize, true);
      pipeBatch.skipStage(originPipe);
      for(Pipe pipe : pipeRunner) {
        pipe.destroy(pipeBatch);
      }
    }
  }

  @Override
  public List<List<StageOutput>> getBatchesOutput() {
    return batchesOutput;
  }


  @Override
  public String getSourceOffset() {
    return sourceOffset;
  }

  @Override
  public String getNewSourceOffset() {
    return newSourceOffset;
  }

  @Override
  public void setObserver(Observer observer) {

  }

  @Override
  public void registerListener(BatchListener batchListener) {
    // TODO Auto-generated method stub

  }

  public void setPipeContext(PipeContext pipeContext) {

  }
}
