/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.preview;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.StageType;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.metrics.MetricsConfigurator;
import com.streamsets.pipeline.runner.BatchListener;
import com.streamsets.pipeline.runner.FullPipeBatch;
import com.streamsets.pipeline.runner.MultiplexerPipe;
import com.streamsets.pipeline.runner.Observer;
import com.streamsets.pipeline.runner.ObserverPipe;
import com.streamsets.pipeline.runner.Pipe;
import com.streamsets.pipeline.runner.PipeBatch;
import com.streamsets.pipeline.runner.PipelineRunner;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.runner.SourceOffsetTracker;
import com.streamsets.pipeline.runner.StageOutput;
import com.streamsets.pipeline.runner.StagePipe;
import com.streamsets.pipeline.runner.production.BadRecordsHandler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class PreviewPipelineRunner implements PipelineRunner {
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
  private Timer processingTimer;
  private List<BatchListener> batchListenerList = new ArrayList<BatchListener>();

  public PreviewPipelineRunner(String name, String rev, RuntimeInfo runtimeInfo, SourceOffsetTracker offsetTracker, int batchSize, int batches,
      boolean skipTargets) {
    this.name = name;
    this.rev = rev;
    this.runtimeInfo = runtimeInfo;
    this.offsetTracker = offsetTracker;
    this.batchSize = batchSize;
    this.batches = batches;
    this.skipTargets = skipTargets;
    this.metrics = new MetricRegistry();
    processingTimer = MetricsConfigurator.createTimer(metrics, "pipeline.batchProcessing", name, rev);
    batchesOutput = new ArrayList<>();
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
  public void run(Pipe[] pipes, BadRecordsHandler badRecordsHandler) throws StageException, PipelineRuntimeException {
    run(pipes, badRecordsHandler, Collections.EMPTY_LIST);
  }

  @Override
  public void run(Pipe[] pipes, BadRecordsHandler badRecordsHandler, List<StageOutput> stageOutputsToOverride)
      throws StageException, PipelineRuntimeException {
    Map<String, StageOutput> stagesToSkip = new HashMap<>();
    for (StageOutput stageOutput : stageOutputsToOverride) {
      stagesToSkip.put(stageOutput.getInstanceName(), stageOutput);
    }
    for (int i = 0; i < batches; i++) {
      PipeBatch pipeBatch = new FullPipeBatch(offsetTracker, batchSize, true);
      long start = System.currentTimeMillis();
      sourceOffset = pipeBatch.getPreviousOffset();
      for (Pipe pipe : pipes) {
        StageOutput stageOutput = stagesToSkip.get(pipe.getStage().getInfo().getInstanceName());
        if (stageOutput == null || (pipe instanceof ObserverPipe) || (pipe instanceof MultiplexerPipe) ) {
          if (!skipTargets || pipe.getStage().getDefinition().getType() != StageType.TARGET) {
            pipe.process(pipeBatch);
          }
        } else {
          if (pipe instanceof StagePipe) {
            pipeBatch.overrideStageOutput((StagePipe) pipe, stageOutput);
          }
        }
      }
      offsetTracker.commitOffset();
      //TODO badRecordsHandler HANDLE ERRORS
      processingTimer.update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
      newSourceOffset = offsetTracker.getOffset();
      batchesOutput.add(pipeBatch.getSnapshotsOfAllStagesOutput());
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
}
