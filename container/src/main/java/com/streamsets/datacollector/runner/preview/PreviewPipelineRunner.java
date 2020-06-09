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
package com.streamsets.datacollector.runner.preview;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.creation.PipelineConfigBean;
import com.streamsets.datacollector.el.JobEL;
import com.streamsets.datacollector.el.PipelineEL;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.metrics.MetricsConfigurator;
import com.streamsets.datacollector.event.json.MetricRegistryJson;
import com.streamsets.datacollector.runner.BatchContextImpl;
import com.streamsets.datacollector.runner.BatchImpl;
import com.streamsets.datacollector.runner.BatchListener;
import com.streamsets.datacollector.runner.ErrorSink;
import com.streamsets.datacollector.runner.EventSink;
import com.streamsets.datacollector.runner.FullPipeBatch;
import com.streamsets.datacollector.runner.MultiplexerPipe;
import com.streamsets.datacollector.runner.Observer;
import com.streamsets.datacollector.runner.ObserverPipe;
import com.streamsets.datacollector.runner.PipeContext;
import com.streamsets.datacollector.runner.PipeRunner;
import com.streamsets.datacollector.runner.PipelineRunner;
import com.streamsets.datacollector.runner.PipelineRuntimeException;
import com.streamsets.datacollector.runner.ProcessedSink;
import com.streamsets.datacollector.runner.PushSourceContextDelegate;
import com.streamsets.datacollector.runner.RunnerPool;
import com.streamsets.datacollector.runner.RuntimeStats;
import com.streamsets.datacollector.runner.SourceOffsetTracker;
import com.streamsets.datacollector.runner.SourcePipe;
import com.streamsets.datacollector.runner.SourceResponseSinkImpl;
import com.streamsets.datacollector.runner.StageContext;
import com.streamsets.datacollector.runner.StageOutput;
import com.streamsets.datacollector.runner.StagePipe;
import com.streamsets.datacollector.runner.StageRuntime;
import com.streamsets.datacollector.runner.production.BadRecordsHandler;
import com.streamsets.datacollector.runner.production.ReportErrorDelegate;
import com.streamsets.datacollector.runner.production.StatsAggregationHandler;
import com.streamsets.datacollector.util.ValidationUtil;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageType;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class PreviewPipelineRunner implements PipelineRunner, PushSourceContextDelegate, ReportErrorDelegate {

  private static final Logger LOG = LoggerFactory.getLogger(PreviewPipelineRunner.class);

  private final BuildInfo buildInfo;
  private final RuntimeInfo runtimeInfo;
  private final SourceOffsetTracker offsetTracker;
  private final int batchSize;
  private final int batches;
  private final boolean skipTargets;
  private final boolean skipLifecycleEvents;
  private final boolean testOrigin;
  private final MetricRegistry metrics;
  private final List<List<StageOutput>> batchesOutput;
  private final String name;
  private final String rev;
  private final Timer processingTimer;
  private final Map<String, List<ErrorMessage>> reportedErrors;

  // Exception thrown while executing the pipeline
  private volatile Throwable exceptionFromExecution = null;

  private SourcePipe originPipe;
  private List<PipeRunner> pipes;
  private RunnerPool<PipeRunner> runnerPool;
  private BadRecordsHandler badRecordsHandler;
  private StatsAggregationHandler statsAggregationHandler;
  private Map<String, StageOutput> stagesToSkip;
  private AtomicInteger batchesProcessed;
  private PipelineConfiguration pipelineConfiguration;

  public PreviewPipelineRunner(
      String name,
      String rev,
      BuildInfo buildInfo,
      RuntimeInfo runtimeInfo,
      SourceOffsetTracker offsetTracker,
      int batchSize,
      int batches,
      boolean skipTargets,
      boolean skipLifecycleEvents,
      boolean testOrigin
  ) {
    this.name = name;
    this.rev = rev;
    this.buildInfo = buildInfo;
    this.runtimeInfo = runtimeInfo;
    this.offsetTracker = offsetTracker;
    this.batchSize = batchSize;
    this.batches = batches;
    this.skipTargets = skipTargets;
    this.skipLifecycleEvents = skipLifecycleEvents;
    this.testOrigin = testOrigin;
    this.metrics = new MetricRegistry();
    processingTimer = MetricsConfigurator.createTimer(metrics, "pipeline.batchProcessing", name, rev);
    batchesOutput = Collections.synchronizedList(new ArrayList<>());
    this.reportedErrors = new HashMap<>();
  }

  @Override
  public MetricRegistryJson getMetricRegistryJson() {
    return null;
  }

  @Override
  public void errorNotification(SourcePipe originPipe, List<PipeRunner> pipes, Throwable throwable) {
  }

  @Override
  public BuildInfo getBuildInfo() {
    return buildInfo;
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
  public void runLifecycleEvent(
    Record eventRecord,
    StageRuntime stageRuntime
  ) throws StageException {
    if(skipLifecycleEvents) {
      return;
    }

   // One record batch with empty offsets
    Batch batch = new BatchImpl(
      stageRuntime.getConfiguration().getInstanceName(),
      "",
      "",
      ImmutableList.of(eventRecord)
    );

    // We're only supporting Executor and Target types
    Preconditions.checkArgument(
      stageRuntime.getDefinition().getType().isOneOf(StageType.EXECUTOR, StageType.TARGET),
      "Invalid lifecycle event stage type: " + stageRuntime.getDefinition().getType()
    );
    stageRuntime.execute(null, 1000, batch, null, new ErrorSink(), new EventSink(),
        new ProcessedSink(), new SourceResponseSinkImpl());
  }

  @Override
  @SuppressWarnings("unchecked")
  public void run(
    SourcePipe originPipe,
    List<PipeRunner> pipes,
    BadRecordsHandler badRecordsHandler,
    StatsAggregationHandler statsAggregationHandler
  ) throws StageException, PipelineRuntimeException {
    run(originPipe, pipes, badRecordsHandler, Collections.emptyList(), statsAggregationHandler);
  }

  @Override
  public void run(
    SourcePipe originPipe,
    List<PipeRunner> pipes,
    BadRecordsHandler badRecordsHandler,
    List<StageOutput> stageOutputsToOverride,
    StatsAggregationHandler statsAggregationHandler
  ) throws StageException, PipelineRuntimeException {
    this.originPipe = originPipe;
    this.pipes = pipes;
    this.badRecordsHandler = badRecordsHandler;
    this.statsAggregationHandler = statsAggregationHandler;
    this.runnerPool = new RunnerPool<>(pipes, new RuntimeStats(), new Histogram(new ExponentiallyDecayingReservoir()));

    // Counter of batches that were already processed
    batchesProcessed = new AtomicInteger(0);

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

    if(stagesToSkip.containsKey(originPipe.getStage().getInfo().getInstanceName())) {
      // We're skipping the origin's execution, so let's run the pipeline in "usual" manner
      runPollSource();
    } else {
      // Push origin will block on the call until the either all data have been consumed or the pipeline stopped
      originPipe.process(offsetTracker.getOffsets(), batchSize, this);
    }

    // If execution failed on exception, we should propagate it up
    if(exceptionFromExecution != null) {
      Throwables.propagateIfInstanceOf(exceptionFromExecution, StageException.class);
      Throwables.propagateIfInstanceOf(exceptionFromExecution, PipelineRuntimeException.class);
      Throwables.propagate(exceptionFromExecution);
    }
  }

  @Override
  public BatchContext startBatch() {
    FullPipeBatch pipeBatch = new FullPipeBatch(null, null, batchSize, true);
    BatchContextImpl batchContext = new BatchContextImpl(pipeBatch, originPipe.getStage().getDefinition().getRecordsByRef());

    originPipe.prepareBatchContext(batchContext);

    // Since the origin owns the threads in PushSource, need to re-populate the PipelineEL on every batch
    PipelineEL.setConstantsInContext(
        pipelineConfiguration,
        originPipe.getStage().getContext().getUserContext(),
        System.currentTimeMillis()
    );
    JobEL.setConstantsInContext(null);

    return batchContext;
  }

  @Override
  public boolean processBatch(BatchContext batchCtx, String entityName, String entityOffset) {
    BatchContextImpl batchContext = (BatchContextImpl) batchCtx;
    try {
      batchContext.ensureState();

      // Finish origin processing
      originPipe.finishBatchContext(batchContext);

      // Run rest of the pipeline
      runSourceLessBatch(
        batchContext.getStartTime(),
        batchContext.getPipeBatch(),
        entityName,
        entityOffset
      );

      // Increment amount of intercepted batches by one and end the processing if we have desirable amount
      if (batchesProcessed.get() >= batches) {
        ((StageContext) originPipe.getStage().getContext()).setStop(true);
      }

      // Not doing any commits in the preview
      return true;
    } catch (Throwable e) {
      LOG.error("Error while executing preview", e);
      // Remember the exception so that we can re-throw it later
      synchronized (this) {
        if(exceptionFromExecution == null) {
          exceptionFromExecution = e;
        }
      }

      // We got exception while executing pipeline which is a signal that we should stop processing
      ((StageContext)originPipe.getStage().getContext()).setStop(true);

      return  false;
    } finally {
      batchContext.setProcessed(true);
      PipelineEL.unsetConstantsInContext();
      JobEL.unsetConstantsInContext();
    }
  }

  @Override
  public void commitOffset(String entityName, String entityOffset) {
    // Not doing anything in preview
  }

  private void runPollSource() throws StageException, PipelineRuntimeException {
    while(batchesProcessed.get() < batches) {
      FullPipeBatch pipeBatch = new FullPipeBatch(
        Source.POLL_SOURCE_OFFSET_KEY,
        offsetTracker.getOffsets().get(Source.POLL_SOURCE_OFFSET_KEY),
        batchSize,
        true
      );

      long start = System.currentTimeMillis();

        // Process origin data
        StageOutput originOutput = stagesToSkip.get(originPipe.getStage().getInfo().getInstanceName());
        if(originOutput == null) {
          originPipe.process(pipeBatch);
        } else {
          pipeBatch.overrideStageOutput(originPipe, originOutput);
        }

        runSourceLessBatch(
          start,
          pipeBatch,
          Source.POLL_SOURCE_OFFSET_KEY,
          pipeBatch.getNewOffset()
        );
      }
    }

  private void runSourceLessBatch(
    long start,
    FullPipeBatch pipeBatch,
    String offsetEntity,
    String newOffset
  ) throws StageException, PipelineRuntimeException {
    PipeRunner pipeRunner = null;
    try {
      pipeRunner = runnerPool.getRunner();
      pipeRunner.executeBatch(offsetEntity, newOffset, start, pipe -> {
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
      });
    } finally {
      if(pipeRunner != null) {
        runnerPool.returnRunner(pipeRunner);
      }
    }

    offsetTracker.commitOffset(offsetEntity, newOffset);
    //TODO badRecordsHandler HANDLE ERRORS
    processingTimer.update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);

    List<StageOutput> stageOutputs = pipeBatch.getSnapshotsOfAllStagesOutput();
    if(ValidationUtil.isSnapshotOutputUsable(stageOutputs)) {
      batchesOutput.add(addReportedErrorsIfNeeded(pipeBatch.getSnapshotsOfAllStagesOutput()));
      batchesProcessed.incrementAndGet();
    }
  }

  /**
   * Preview only returns data associated with batches, however errors are reported outside of batch context for
   * multi-threaded pipelines. Thus we 'emulate' the behavior by simply adding into the current batch all 'so-far'
   * reported errors.
   */
  private List<StageOutput> addReportedErrorsIfNeeded(List<StageOutput> snapshotsOfAllStagesOutput) {
    synchronized (this.reportedErrors) {
      if(reportedErrors.isEmpty()) {
        return snapshotsOfAllStagesOutput;
      }

      try {
        return snapshotsOfAllStagesOutput.stream()
          .map(so -> new StageOutput(
            so.getInstanceName(),
            so.getOutput(),
            so.getErrorRecords(),
            reportedErrors.get(so.getInstanceName()),
            so.getEventRecords()
          ))
          .collect(Collectors.toList());
      } finally {
        reportedErrors.clear();
      }
    }
  }

  @Override
  public void destroy(
    SourcePipe originPipe,
    List<PipeRunner> pipeRunners,
    BadRecordsHandler badRecordsHandler,
    StatsAggregationHandler statsAggregationHandler
  ) throws StageException, PipelineRuntimeException {
    // Stop the origin even if it did not stopped on it's own
    ((StageContext)originPipe.getStage().getContext()).setStop(true);

    // Firstly destroy the runner, to make sure that any potential run away thread from origin will be denied
    // further processing.
    if(runnerPool != null) {
      runnerPool.destroy();
    }

    // We're not doing any special event propagation during preview destroy phase
    long start = System.currentTimeMillis();

    // Destroy origin on it's own
    FullPipeBatch originBatch = new FullPipeBatch(null, null, batchSize, false);
    originBatch.skipStage(originPipe);
    originPipe.destroy(originBatch);

    // And destroy each pipeline instance separately
    for(PipeRunner pipeRunner: pipeRunners) {
      final FullPipeBatch pipeBatch = new FullPipeBatch(null,null, batchSize, true);
      pipeBatch.skipStage(originPipe);
      pipeRunner.executeBatch(null, null, start, p -> {
        if(p instanceof StagePipe) {
          pipeBatch.startStage((StagePipe)p);
        }
        p.destroy(pipeBatch);
      });
    }
  }

  @Override
  public List<List<StageOutput>> getBatchesOutput() {
    return batchesOutput;
  }

  @Override
  public void setObserver(Observer observer) {

  }

  @Override
  public void registerListener(BatchListener batchListener) {
    // TODO Auto-generated method stub

  }

  public void setRuntimeConfiguration(
    PipeContext pipeContext,
    PipelineConfiguration pipelineConfiguration,
    PipelineConfigBean pipelineConfigBean
  ) {
    this.pipelineConfiguration = pipelineConfiguration;
    // Preview does not need the remaining runtime structures
  }

  @Override
  public void reportError(String stage, ErrorMessage errorMessage) {
    synchronized(this.reportedErrors) {
      this.reportedErrors.computeIfAbsent(stage, x -> new LinkedList<>()).add(errorMessage);
    }
  }

  @Override
  public void setFinished(boolean resetOffset) {
    LOG.info("PreviewPipelineRunner: setFinished({}) was called and ignored.", resetOffset);
  }

}
