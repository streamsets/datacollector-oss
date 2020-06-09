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
package com.streamsets.datacollector.execution.runner.common;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.EvictingQueue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.RateLimiter;
import com.streamsets.datacollector.bundles.SupportBundleManager;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.creation.PipelineConfigBean;
import com.streamsets.datacollector.el.JobEL;
import com.streamsets.datacollector.el.PipelineEL;
import com.streamsets.datacollector.execution.SnapshotInfo;
import com.streamsets.datacollector.execution.SnapshotStore;
import com.streamsets.datacollector.execution.metrics.MetricsEventRunnable;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.metrics.MetricsConfigurator;
import com.streamsets.datacollector.event.json.HistogramJson;
import com.streamsets.datacollector.event.json.MeterJson;
import com.streamsets.datacollector.event.json.MetricRegistryJson;
import com.streamsets.datacollector.runner.BatchContextImpl;
import com.streamsets.datacollector.runner.BatchImpl;
import com.streamsets.datacollector.runner.BatchListener;
import com.streamsets.datacollector.runner.ErrorSink;
import com.streamsets.datacollector.runner.EventSink;
import com.streamsets.datacollector.runner.FullPipeBatch;
import com.streamsets.datacollector.runner.Observer;
import com.streamsets.datacollector.runner.Pipe;
import com.streamsets.datacollector.runner.PipeContext;
import com.streamsets.datacollector.runner.PipeRunner;
import com.streamsets.datacollector.runner.PipelineRunner;
import com.streamsets.datacollector.runner.PipelineRuntimeException;
import com.streamsets.datacollector.runner.ProcessedSink;
import com.streamsets.datacollector.runner.PushSourceContextDelegate;
import com.streamsets.datacollector.runner.RunnerPool;
import com.streamsets.datacollector.runner.SourceOffsetTracker;
import com.streamsets.datacollector.runner.SourcePipe;
import com.streamsets.datacollector.runner.SourceResponseSinkImpl;
import com.streamsets.datacollector.runner.StageContext;
import com.streamsets.datacollector.runner.StageOutput;
import com.streamsets.datacollector.runner.StagePipe;
import com.streamsets.datacollector.runner.StageRuntime;
import com.streamsets.datacollector.runner.production.BadRecordsHandler;
import com.streamsets.datacollector.runner.production.PipelineErrorNotificationRequest;
import com.streamsets.datacollector.runner.production.ReportErrorDelegate;
import com.streamsets.datacollector.runner.production.StatsAggregationHandler;
import com.streamsets.datacollector.usagestats.StatsCollector;
import com.streamsets.datacollector.util.AggregatorUtil;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.datacollector.util.ValidationUtil;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.DeliveryGuarantee;
import com.streamsets.pipeline.api.ErrorListener;
import com.streamsets.pipeline.api.OffsetCommitTrigger;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageType;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;


public class ProductionPipelineRunner implements PipelineRunner, PushSourceContextDelegate, ReportErrorDelegate {

  private static final Logger LOG = LoggerFactory.getLogger(ProductionPipelineRunner.class);

  private final BuildInfo buildInfo;
  private final RuntimeInfo runtimeInfo;
  private final com.streamsets.datacollector.util.Configuration configuration;
  private final MetricRegistry metrics;
  private SourceOffsetTracker offsetTracker;
  private final SnapshotStore snapshotStore;
  private DeliveryGuarantee deliveryGuarantee;
  private final String pipelineName;
  private final String revision;
  private final SupportBundleManager supportBundleManager;
  private final List<ErrorListener> errorListeners;
  private final StatsCollector statsCollector;

  private SourcePipe originPipe;
  private List<PipeRunner> pipes;
  private RunnerPool<PipeRunner> runnerPool;
  private BadRecordsHandler badRecordsHandler;
  private StatsAggregationHandler statsAggregationHandler;

  private final Timer batchProcessingTimer;
  private final Meter batchCountMeter;
  private final Counter batchCountCounter;
  private final Histogram batchInputRecordsHistogram;
  private final Histogram batchOutputRecordsHistogram;
  private final Histogram batchErrorRecordsHistogram;
  private final Histogram batchErrorsHistogram;
  private final Meter batchInputRecordsMeter;
  private final Meter batchOutputRecordsMeter;
  private final Meter batchErrorRecordsMeter;
  private final Meter batchErrorMessagesMeter;
  private final Counter batchInputRecordsCounter;
  private final Counter batchOutputRecordsCounter;
  private final Counter batchErrorRecordsCounter;
  private final Counter batchErrorMessagesCounter;
  private final Histogram runnersHistogram;
  private MetricRegistryJson metricRegistryJson;
  private Long rateLimit;

  private RateLimiter rateLimiter;

  /*indicates if the execution must be stopped after the current batch*/
  private volatile boolean stop = false;
  /*indicates an external stage (eg. PipelineFinisherExecutor) is finishing the pipeline. */
  private volatile boolean finished = false;
  /* Flag if the runner is in "running" state (e.g. inside run() method before destroy(). */
  private volatile boolean running = false;
  /*indicates if the next batch of data should be captured, only the next batch*/
  private volatile int batchesToCapture = 0;
  /* Indicates if we should reset offset at the end of the execution. */
  private volatile boolean resetOffset = false;
  /*indicates the snapshot name to be captured*/
  private volatile String snapshotName;
  /*indicates the batch size to be captured*/
  private volatile int snapshotBatchSize;
  // Exception thrown while executing the pipeline
  private volatile Throwable exceptionFromExecution = null;
  /*Cache last N error records per stage in memory*/
  private final Map<String, EvictingQueue<Record>> stageToErrorRecordsMap;
  /*Cache last N error messages in memory*/
  private final Map<String, EvictingQueue<ErrorMessage>> stageToErrorMessagesMap;
  /**/
  private BlockingQueue<Object> observeRequests;
  private Observer observer;
  private BlockingQueue<Record> statsAggregatorRequests;
  private final List<BatchListener> batchListenerList = new CopyOnWriteArrayList<>();
  private ThreadHealthReporter threadHealthReporter;
  private final List<List<StageOutput>> capturedBatches = new ArrayList<>();
  private PipeContext pipeContext = null;
  private PipelineConfigBean pipelineConfigBean = null;
  private PipelineConfiguration pipelineConfiguration = null;
  private Lock destroyLock = new ReentrantLock();

  private long pipelineStartTime;
  private Map<String, Object> parameters;

  @Inject
  public ProductionPipelineRunner(
      @Named("name") String pipelineName,
      @Named("rev") String revision,
      SupportBundleManager supportBundleManager,
      Configuration configuration,
      BuildInfo buildInfo,
      RuntimeInfo runtimeInfo,
      MetricRegistry metrics,
      SnapshotStore snapshotStore,
      ThreadHealthReporter threadHealthReporter,
      StatsCollector statsCollector
  ) {
    this.buildInfo = buildInfo;
    this.runtimeInfo = runtimeInfo;
    this.configuration = configuration;
    this.metrics = metrics;
    this.threadHealthReporter = threadHealthReporter;
    this.snapshotStore = snapshotStore;
    this.pipelineName = pipelineName;
    this.revision = revision;
    this.supportBundleManager = supportBundleManager;
    stageToErrorRecordsMap = new HashMap<>();
    stageToErrorMessagesMap = new HashMap<>();
    this.errorListeners = new ArrayList<>();
    this.statsCollector = statsCollector;

    MetricsConfigurator.registerPipeline(pipelineName, revision);
    batchProcessingTimer = MetricsConfigurator.createTimer(metrics, "pipeline.batchProcessing", pipelineName, revision);
    batchCountMeter = MetricsConfigurator.createMeter(metrics, "pipeline.batchCount", pipelineName, revision);
    batchCountCounter = MetricsConfigurator.createCounter(metrics, "pipeline.batchCount", pipelineName, revision);
    batchInputRecordsHistogram = MetricsConfigurator.createHistogram5Min(metrics, "pipeline.inputRecordsPerBatch",
      pipelineName, revision);
    batchOutputRecordsHistogram = MetricsConfigurator.createHistogram5Min(metrics, "pipeline.outputRecordsPerBatch",
      pipelineName, revision);
    batchErrorRecordsHistogram = MetricsConfigurator.createHistogram5Min(metrics, "pipeline.errorRecordsPerBatch",
      pipelineName, revision);
    batchErrorsHistogram = MetricsConfigurator.createHistogram5Min(metrics, "pipeline.errorsPerBatch", pipelineName,
      revision);
    batchInputRecordsMeter = MetricsConfigurator.createMeter(metrics, "pipeline.batchInputRecords", pipelineName,
      revision);
    batchOutputRecordsMeter = MetricsConfigurator.createMeter(metrics, "pipeline.batchOutputRecords", pipelineName,
      revision);
    batchErrorRecordsMeter = MetricsConfigurator.createMeter(metrics, "pipeline.batchErrorRecords", pipelineName,
      revision);
    batchErrorMessagesMeter = MetricsConfigurator.createMeter(metrics, "pipeline.batchErrorMessages", pipelineName,
      revision);
    batchInputRecordsCounter = MetricsConfigurator.createCounter(metrics, "pipeline.batchInputRecords", pipelineName,
      revision);
    batchOutputRecordsCounter = MetricsConfigurator.createCounter(metrics, "pipeline.batchOutputRecords", pipelineName,
      revision);
    batchErrorRecordsCounter = MetricsConfigurator.createCounter(metrics, "pipeline.batchErrorRecords", pipelineName,
      revision);
    batchErrorMessagesCounter = MetricsConfigurator.createCounter(metrics, "pipeline.batchErrorMessages", pipelineName,
      revision);
    runnersHistogram = MetricsConfigurator.createHistogram5Min(metrics, "pipeline.runners", pipelineName, revision);
  }

  public void setObserveRequests(BlockingQueue<Object> observeRequests) {
    this.observeRequests = observeRequests;
  }

  public void setStatsAggregatorRequests(BlockingQueue<Record> statsAggregatorRequests) {
    this.statsAggregatorRequests = statsAggregatorRequests;
  }

  public void addErrorListeners(List<ErrorListener> errorListeners) {
    LOG.info("Adding error listeners" + errorListeners.size());
    this.errorListeners.addAll(errorListeners);
  }

  public void updateMetrics(MetricRegistryJson metricRegistryJson) {
    this.metricRegistryJson = metricRegistryJson;
    HistogramJson inputHistogramJson = metricRegistryJson.getHistograms().get("pipeline.inputRecordsPerBatch" + MetricsConfigurator.HISTOGRAM_M5_SUFFIX);
    batchInputRecordsHistogram.update(inputHistogramJson.getCount());
    HistogramJson outputHistogramJson = metricRegistryJson.getHistograms().get("pipeline.outputRecordsPerBatch" + MetricsConfigurator.HISTOGRAM_M5_SUFFIX);
    batchOutputRecordsHistogram.update(outputHistogramJson.getCount());
    HistogramJson errorHistogramJson = metricRegistryJson.getHistograms().get("pipeline.errorRecordsPerBatch" + MetricsConfigurator.HISTOGRAM_M5_SUFFIX);
    batchErrorRecordsHistogram.update(errorHistogramJson.getCount());
    HistogramJson errorPerBatchHistogramJson = metricRegistryJson.getHistograms().get("pipeline.errorsPerBatch" + MetricsConfigurator.HISTOGRAM_M5_SUFFIX);
    batchErrorsHistogram.update(errorPerBatchHistogramJson.getCount());
    MeterJson batchInputRecords = metricRegistryJson.getMeters().get("pipeline.batchInputRecords" + MetricsConfigurator.METER_SUFFIX);
    batchInputRecordsMeter.mark(batchInputRecords.getCount());
    batchInputRecordsCounter.inc(batchInputRecords.getCount());
    MeterJson batchOutputRecords = metricRegistryJson.getMeters().get("pipeline.batchOutputRecords" + MetricsConfigurator.METER_SUFFIX);
    batchOutputRecordsMeter.mark(batchOutputRecords.getCount());
    batchOutputRecordsCounter.inc(batchOutputRecords.getCount());
    MeterJson batchErrorRecords = metricRegistryJson.getMeters().get("pipeline.batchErrorRecords" + MetricsConfigurator.METER_SUFFIX);
    batchErrorRecordsMeter.mark(batchErrorRecords.getCount());
    batchErrorRecordsCounter.inc(batchErrorRecords.getCount());
    MeterJson batchErrorMessagesRecords = metricRegistryJson.getMeters().get("pipeline.batchErrorMessages" + MetricsConfigurator.METER_SUFFIX);
    batchErrorMessagesMeter.mark(batchErrorMessagesRecords.getCount());
    batchErrorMessagesCounter.inc(batchErrorMessagesRecords.getCount());
    HistogramJson runnersJson = metricRegistryJson.getHistograms().get("pipeline.runners" + MetricsConfigurator.HISTOGRAM_M5_SUFFIX);
    runnersHistogram.update(runnersJson.getCount());
  }

  @Override
  public MetricRegistryJson getMetricRegistryJson() {
    return this.metricRegistryJson;
  }

  public void setDeliveryGuarantee(DeliveryGuarantee deliveryGuarantee) {
    this.deliveryGuarantee = deliveryGuarantee;
  }

  public void setRateLimit(Long rateLimit) {
    this.rateLimit = rateLimit;
    rateLimiter = RateLimiter.create(rateLimit.doubleValue());
  }

  public void setOffsetTracker(SourceOffsetTracker offsetTracker) {
    this.offsetTracker = offsetTracker;
  }

  public void setPipelineStartTime(long pipelineStartTime) {
    this.pipelineStartTime = pipelineStartTime;
  }

  public void setParameters(Map<String, Object> parameters) {
    this.parameters = parameters;
  }

  public void setThreadHealthReporter(ThreadHealthReporter threadHealthReporter) {
    this.threadHealthReporter = threadHealthReporter;
  }

  @Override
  public void setObserver(Observer observer) {
    this.observer = observer;
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
    return false;
  }

  @Override
  public MetricRegistry getMetrics() {
    return metrics;
  }

  @Override
  public void runLifecycleEvent(
    Record eventRecord,
    StageRuntime stageRuntime
  ) throws StageException, PipelineRuntimeException {
    // One record batch with empty offsets
    Batch batch = new BatchImpl(
      stageRuntime.getConfiguration().getInstanceName(),
      "",
      "",
      ImmutableList.of(eventRecord)
    );

    ErrorSink errorSink = new ErrorSink();

    // We're only supporting Executor and Target types
    Preconditions.checkArgument(
      stageRuntime.getDefinition().getType().isOneOf(StageType.EXECUTOR, StageType.TARGET),
      "Invalid lifecycle event stage type: " + stageRuntime.getDefinition().getType()
    );
    stageRuntime.execute(null, 1000, batch, null, errorSink, new EventSink(),
        new ProcessedSink(), new SourceResponseSinkImpl());

    // Pipeline lifecycle stage generating error record is fatal error
    if(!errorSink.getErrorRecords().isEmpty()) {
      // Generate list of error string describing what is wrong (in most cases this will be exactly one string - since
      // input is exactly one record).
      String errorMessages = errorSink.getErrorRecords().values().stream()
        .flatMap(List::stream)
        .map(record -> record.getHeader().getErrorMessage())
        .collect(Collectors.joining(", "));
      throw new PipelineRuntimeException(ContainerError.CONTAINER_0792, errorMessages);
    }
  }

  @Override
  public void run(
    SourcePipe originPipe,
    List<PipeRunner> pipes,
    BadRecordsHandler badRecordsHandler,
    StatsAggregationHandler statsAggregationHandler
  ) throws StageException, PipelineRuntimeException {
    this.originPipe = originPipe;
    this.pipes = pipes;
    this.badRecordsHandler = badRecordsHandler;
    this.statsAggregationHandler = statsAggregationHandler;
    this.runnerPool = new RunnerPool<>(pipes, pipeContext.getRuntimeStats(), runnersHistogram);

    // And we're officially running!
    this.running = true;

    try {
      LOG.debug("Staring pipeline with offset: {}", offsetTracker.getOffsets());
      if (originPipe.getStage().getStage() instanceof PushSource) {
        runPushSource();
      } else {
        runPollSource();
      }

    } catch (Throwable throwable) {
      LOG.error("Pipeline execution failed", throwable);
      sendPipelineErrorNotificationRequest(throwable);
      errorNotification(originPipe, pipes, throwable);

      Throwables.propagateIfInstanceOf(throwable, StageException.class);
      Throwables.propagateIfInstanceOf(throwable, PipelineRuntimeException.class);
      Throwables.propagate(throwable);
    }

    if(resetOffset) {
      offsetTracker.resetOffset();
    }
  }

  private void runPushSource() throws StageException, PipelineRuntimeException {
    // This object will receive delegated calls from the push origin callbacks
    originPipe.getStage().setPushSourceContextDelegate(this);

    // Configured maximal batch size
    int batchSize = configuration.get(Constants.MAX_BATCH_SIZE_KEY, Constants.MAX_BATCH_SIZE_DEFAULT);

    // During Snapshot capture use Snapshot batch size
    if (batchesToCapture > 0) {
      batchSize = snapshotBatchSize;
    }

    try {
      // Push origin will block on the call until the either all data have been consumed or the pipeline stopped
      originPipe.process(offsetTracker.getOffsets(), batchSize, this);
    } catch (Throwable ex) {
      LOG.error("Origin Pipe failed", ex);
      // Execution failed, but this could be a "dependent" exception from other pipeline execution failure
      if(exceptionFromExecution == null) {
        exceptionFromExecution = ex;
      }
    }

    // If execution failed on exception, we should propagate it up
    if(exceptionFromExecution != null) {
      // If applicable record the error codes
      if(statsCollector != null) {
        if(exceptionFromExecution instanceof StageException) {
          statsCollector.errorCode(((StageException) exceptionFromExecution).getErrorCode());
        }
        if(exceptionFromExecution instanceof PipelineRuntimeException) {
          statsCollector.errorCode(((PipelineRuntimeException) exceptionFromExecution).getErrorCode());
        }
      }

      Throwables.propagateIfInstanceOf(exceptionFromExecution, StageException.class);
      Throwables.propagateIfInstanceOf(exceptionFromExecution, PipelineRuntimeException.class);
      Throwables.propagate(exceptionFromExecution);
    }
  }

  private FullPipeBatch createFullPipeBatch(String entityName, String previousOffset) {
    FullPipeBatch pipeBatch;
    if(batchesToCapture > 0) {
      pipeBatch = new FullPipeBatch(entityName, previousOffset, snapshotBatchSize, true);
    } else {
      pipeBatch = new FullPipeBatch(entityName, previousOffset, configuration.get(Constants.MAX_BATCH_SIZE_KEY, Constants.MAX_BATCH_SIZE_DEFAULT), false);
    }
    pipeBatch.setRateLimiter(rateLimiter);

    return pipeBatch;
  }

  @Override
  public BatchContext startBatch() {
    // Pick up any recent changes done to the rule definitions
    if(observer != null) {
      observer.reconfigure();
    }

    FullPipeBatch pipeBatch = createFullPipeBatch(null,null);
    BatchContextImpl batchContext = new BatchContextImpl(pipeBatch, originPipe.getStage().getDefinition().getRecordsByRef());

    originPipe.prepareBatchContext(batchContext);

    // Since the origin owns the threads in PushSource, need to re-populate the PipelineEL on every batch
    PipelineEL.setConstantsInContext(
        pipelineConfiguration,
        originPipe.getStage().getContext().getUserContext(),
        pipelineStartTime
    );
    JobEL.setConstantsInContext(parameters);

    // Run batch listeners
    for (BatchListener batchListener : batchListenerList) {
      batchListener.preBatch();
    }

    return batchContext;
  }

  @Override
  public boolean processBatch(BatchContext batchCtx, String entity, String offset) {
    BatchContextImpl batchContext = (BatchContextImpl) batchCtx;

    Map<String, Long> memoryConsumedByStage = new HashMap<>();
    Map<String, Object> stageBatchMetrics = new HashMap<>();

    try {
      batchContext.ensureState();

      Map<String, Object> batchMetrics = originPipe.finishBatchContext(batchContext);

      if (isStatsAggregationEnabled()) {
        stageBatchMetrics.put(originPipe.getStage().getInfo().getInstanceName(), batchMetrics);
      }

      runSourceLessBatch(
        batchContext.getStartTime(),
        batchContext.getPipeBatch(),
        entity,
        offset,
        memoryConsumedByStage,
        stageBatchMetrics
      );

      // Run batch listeners
      for (BatchListener batchListener : batchListenerList) {
        batchListener.postBatch();
      }
    } catch (Throwable e) {
      LOG.error("Can't process batch", e);

      // We try to create partial batch on processing failure
      createFailureBatch(batchContext.getPipeBatch());

      // We got exception while executing pipeline which is a signal that we should stop processing
      ((StageContext)originPipe.getStage().getContext()).setStop(true);

      // Persist the exception so that we can re-throw it later in runPushSource method
      synchronized (this) {
        if(exceptionFromExecution == null) {
          exceptionFromExecution = e;
        }
      }

      // Returning false so that origin can properly communicate back that this request wasn't processed
      return false;
    } finally {
      batchContext.setProcessed(true);
      PipelineEL.unsetConstantsInContext();
      JobEL.unsetConstantsInContext();
    }

    return true;
  }

  @Override
  public void commitOffset(String entity, String offset) {
    offsetTracker.commitOffset(entity, offset);
  }

  @Override
  public void reportError(String stage, ErrorMessage errorMessage) {
    // For PushSource, the runner itself is listening to errors because they can happen outside of batch's scope
    retainErrorMessagesInMemory(ImmutableMap.of(stage, ImmutableList.of(errorMessage)));
  }

  @Override
  public void setFinished(boolean resetOffset) {
    finished = true;
    this.resetOffset = resetOffset;
    ((StageContext)originPipe.getStage().getContext()).setStop(true);
  }

  public void runPollSource() throws StageException, PipelineException {
    while (!offsetTracker.isFinished() && !stop && !finished) {
      if (threadHealthReporter != null) {
        threadHealthReporter.reportHealth(ProductionPipelineRunnable.RUNNABLE_NAME, -1, System.currentTimeMillis());
      }

      for (BatchListener batchListener : batchListenerList) {
        batchListener.preBatch();
      }

      if(observer != null) {
        observer.reconfigure();
      }

      // Start of the batch execution
      long start = System.currentTimeMillis();
      FullPipeBatch pipeBatch = createFullPipeBatch(Source.POLL_SOURCE_OFFSET_KEY, offsetTracker.getOffsets().get(Source.POLL_SOURCE_OFFSET_KEY));

      // Run origin
      Map<String, Long> memoryConsumedByStage = new HashMap<>();
      Map<String, Object> stageBatchMetrics = new HashMap<>();
      processPipe(
        originPipe,
        pipeBatch,
        false,
        null,
        null,
        memoryConsumedByStage,
        stageBatchMetrics
      );

      // Since the origin already run, the FullPipeBatch will have a new offset
      String newOffset = pipeBatch.getNewOffset();

      try {
        // Run rest of the pipeline
        runSourceLessBatch(
          start,
          pipeBatch,
          Source.POLL_SOURCE_OFFSET_KEY,
          newOffset,
          memoryConsumedByStage,
          stageBatchMetrics
        );
      } catch (Throwable t) {
        // We try to create partial batch on processing failure
        createFailureBatch(pipeBatch);

        // If applicable record the error codes
        if(statsCollector != null) {
          if(exceptionFromExecution instanceof StageException) {
            statsCollector.errorCode(((StageException) exceptionFromExecution).getErrorCode());
          }
          if(exceptionFromExecution instanceof PipelineRuntimeException) {
            statsCollector.errorCode(((PipelineRuntimeException) exceptionFromExecution).getErrorCode());
          }
        }

        Throwables.propagateIfInstanceOf(t, StageException.class);
        Throwables.propagateIfInstanceOf(t, PipelineRuntimeException.class);
        Throwables.propagate(t);
      }

      for (BatchListener batchListener : batchListenerList) {
        batchListener.postBatch();
      }
    }
  }

  @Override
  public void errorNotification(SourcePipe originPipe, List<PipeRunner> pipes, Throwable throwable) {
    Set<ErrorListener> listeners = Sets.newIdentityHashSet();
    for (BatchListener batchListener : batchListenerList) {
      batchListener.postBatch();
    }
    listeners.addAll(new ArrayList<>(errorListeners));
    if (originPipe.getStage().getStage() instanceof ErrorListener) {
      listeners.add((ErrorListener) originPipe.getStage().getStage());
    }
    for(PipeRunner pipeRunner : pipes) {
      pipeRunner.forEach("", pipe -> {
        Stage stage = pipe.getStage().getStage();
        if (stage instanceof ErrorListener) {
          listeners.add((ErrorListener) stage);
        }
      });
    }
    for (ErrorListener listener : listeners) {
      try {
        listener.errorNotification(throwable);
      } catch (Exception ex) {
        String msg = Utils.format("Error in calling ErrorListenerStage {}: {}", listener.getClass().getName(), ex);
        LOG.error(msg, ex);
      }
    }
  }

  @Override
  public void run(
    SourcePipe originPipe,
    List<PipeRunner> pipes,
    BadRecordsHandler badRecordsHandler,
    List<StageOutput> stageOutputsToOverride,
    StatsAggregationHandler statsAggregationHandler
  ) throws StageException, PipelineRuntimeException {
    throw new UnsupportedOperationException();
  }

  /**
   * Since stages are allowed to produce events during destroy() phase, we handle the destroy event as simplified
   * runBatch. We go over all the StagePipes and if it's on data path we destroy it immediately, if it's on  event path, we
   * run it one more time. Since the stages are sorted we know that destroyed stage will never be needed again. Non
   * stage pipes are always processed to generate required structures in PipeBatch.
   */
  @Override
  public void destroy(
    SourcePipe originPipe,
    List<PipeRunner> pipeRunners,
    BadRecordsHandler badRecordsHandler,
    StatsAggregationHandler statsAggregationHandler
  ) throws StageException, PipelineRuntimeException {
    // We're no longer running
    running = false;

    // There are two ways a runner can be in use - used by real runner (e.g. when origin produced data) or when it's
    // processing "empty" batch when the runner was idle for too long. The first case is guarded by the framework - this
    // method won't be called until the execution successfully finished. However the second way with idle drivers is run
    // from a separate thread and hence we have to ensure here that it's "done" before moving on with the destroy phase.
    try {
      destroyLock.lock();

      // Firstly destroy the runner, to make sure that any potential run away thread from origin will be denied
      // further processing.
      if (runnerPool != null) {
        try {
          runnerPool.destroy();
        } catch (PipelineRuntimeException e) {
          LOG.warn(e.getMessage());
        }
      }

      int batchSize = configuration.get(Constants.MAX_BATCH_SIZE_KEY, Constants.MAX_BATCH_SIZE_DEFAULT);
      long lastBatchTime = offsetTracker.getLastBatchTime();
      long start = System.currentTimeMillis();
      FullPipeBatch pipeBatch;

      // Destroy origin pipe
      pipeBatch = new FullPipeBatch(null, null, batchSize, false);
      try {
        LOG.trace("Destroying origin pipe");
        pipeBatch.skipStage(originPipe);
        originPipe.destroy(pipeBatch);
      } catch (RuntimeException e) {
        LOG.warn("Exception throw while destroying pipe", e);
      }

      // Now destroy the pipe runners
      //
      // We're destroying them in reverser order to make sure that the last runner to destroy is the one with id '0'
      // that holds reference to all class loaders. Runners with id >0 do not own their class loaders and hence needs to
      // be destroyed before the runner with id '0'.
      for (PipeRunner pipeRunner : Lists.reverse(pipeRunners)) {
        final FullPipeBatch finalPipeBatch = pipeBatch;

        pipeRunner.executeBatch(null, null, start, pipe -> {
          // Set the last batch time in the stage context of each pipe
          ((StageContext) pipe.getStage().getContext()).setLastBatchTime(lastBatchTime);
          String instanceName = pipe.getStage().getConfiguration().getInstanceName();

          if (pipe instanceof StagePipe) {
            // Stage pipes are processed only if they are in event path
            if (pipe.getStage().getConfiguration().isInEventPath()) {
              LOG.trace("Stage pipe {} is in event path, running last process", instanceName);
              pipe.process(finalPipeBatch);
            } else {
              LOG.trace("Stage pipe {} is in data path, skipping it's processing.", instanceName);
              finalPipeBatch.skipStage(pipe);
            }
          } else {
            // Non stage pipes are executed always
            LOG.trace("Non stage pipe {}, running last process", instanceName);
            pipe.process(finalPipeBatch);
          }

          // And finally destroy the pipe
          try {
            LOG.trace("Running destroy for {}", instanceName);
            pipe.destroy(finalPipeBatch);
          } catch (RuntimeException e) {
            LOG.warn("Exception throw while destroying pipe", e);
          }
        });

        badRecordsHandler.handle(null, null, pipeBatch.getErrorSink(), pipeBatch.getSourceResponseSink());

        // Next iteration should have new and empty PipeBatch
        pipeBatch = new FullPipeBatch(null, null, batchSize, false);
        pipeBatch.skipStage(originPipe);
      }
      if (isStatsAggregationEnabled()) {
        List<Record> stats = new ArrayList<>();
        statsAggregatorRequests.drainTo(stats);
        Object timeSeriesString = pipelineConfigBean.constants.get(MetricsEventRunnable.TIME_SERIES_ANALYSIS);
        boolean timeSeriesAnalysis = (timeSeriesString != null) ? (Boolean) timeSeriesString : true;
        String metricRegistryStr;
        try {
          metricRegistryStr = ObjectMapperFactory.get().writer().writeValueAsString(metrics);
        } catch (Exception e) {
          throw new RuntimeException(Utils.format("Error converting metric json to string: {}", e), e);
        }
        LOG.info("Queueing last batch of record to be sent to stats aggregator");
        stats.add(AggregatorUtil.createMetricJsonRecord(
            runtimeInfo.getId(),
            runtimeInfo.getMasterSDCId(),
            pipelineConfiguration.getMetadata(),
            false,
            timeSeriesAnalysis,
            true,
            metricRegistryStr
        ));

        statsAggregationHandler.handle(null, null, stats);
      }
    } finally {
        destroyLock.unlock();
    }
  }

  @Override
  public List<List<StageOutput>> getBatchesOutput() {
    throw new UnsupportedOperationException();
  }

  public Map<String, String> getCommittedOffsets() {
    return offsetTracker.getOffsets();
  }

  /**
   * Stops execution of the pipeline after the current batch completes
   */
  public void stop() throws PipelineException {
    this.stop = true;
    if(batchesToCapture > 0) {
      cancelSnapshot(this.snapshotName);
      snapshotStore.deleteSnapshot(pipelineName, revision, snapshotName);
    }
  }

  public boolean wasStopped() {
    return stop;
  }

  public void capture(String snapshotName, int batchSize, int batches) {
    Preconditions.checkArgument(batchSize > 0);
    this.snapshotName = snapshotName;
    this.snapshotBatchSize = batchSize;
    this.batchesToCapture = batches;
  }

  public void cancelSnapshot(String snapshotName) throws PipelineException {
    Preconditions.checkArgument(this.snapshotName != null && this.snapshotName.equals(snapshotName));
    synchronized (this) {
      this.snapshotBatchSize = 0;
      this.batchesToCapture = 0;
      capturedBatches.clear();
    }
  }

  private boolean processPipe(
    Pipe pipe,
    FullPipeBatch pipeBatch,
    boolean committed,
    String entityName,
    String newOffset,
    Map<String, Long> memoryConsumedByStage,
    Map<String, Object> stageBatchMetrics
  ) throws PipelineRuntimeException, StageException {

    // Set the last batch time in the stage context of each pipe
    ((StageContext)pipe.getStage().getContext()).setLastBatchTime(offsetTracker.getLastBatchTime());

    if(!pipeBatch.isIdleBatch()) {
      if (deliveryGuarantee == DeliveryGuarantee.AT_MOST_ONCE
        && pipe.getStage().getDefinition().getType() == StageType.TARGET
        && !committed
      ) {
        // target cannot control offset commit in AT_MOST_ONCE mode
        offsetTracker.commitOffset(entityName, newOffset);
        committed = true;
      }
    }
    pipe.process(pipeBatch);
    if (pipe instanceof StagePipe) {
      if (isStatsAggregationEnabled()) {
        stageBatchMetrics.put(pipe.getStage().getInfo().getInstanceName(), ((StagePipe) pipe).getBatchMetrics());
      }
    }

    return committed;
  }

  private void runSourceLessBatch(
    long start,
    FullPipeBatch pipeBatch,
    String entityName,
    String newOffset,
    Map<String, Long> memoryConsumedByStage,
    Map<String, Object> stageBatchMetrics
  ) throws PipelineException, StageException {
    PipeRunner pipeRunner = null;
    try {
      pipeRunner = runnerPool.getRunner();
      executeRunner(pipeRunner, start, pipeBatch, entityName, newOffset, memoryConsumedByStage, stageBatchMetrics);
    } finally {
      if(pipeRunner != null) {
        runnerPool.returnRunner(pipeRunner);
      }
    }
  }

  private void executeRunner(
    PipeRunner pipeRunner,
    long start,
    FullPipeBatch pipeBatch,
    String entityName,
    String newOffset,
    Map<String, Long> memoryConsumedByStage,
    Map<String, Object> stageBatchMetrics
  ) throws PipelineException, StageException {
    final AtomicBoolean committed = new AtomicBoolean(false);
    String previousOffset = pipeBatch.getPreviousOffset();

    OffsetCommitTrigger offsetCommitTrigger = pipeRunner.getOffsetCommitTrigger();

    pipeRunner.executeBatch(entityName, newOffset, start, pipe -> {
      committed.set(processPipe(pipe, pipeBatch, committed.get(), entityName, newOffset, memoryConsumedByStage, stageBatchMetrics));

    });

    badRecordsHandler.handle(entityName, newOffset, pipeBatch.getErrorSink(), pipeBatch.getSourceResponseSink());
    if(!pipeBatch.isIdleBatch()) {
      if (deliveryGuarantee == DeliveryGuarantee.AT_LEAST_ONCE) {
        // When AT_LEAST_ONCE commit only if
        // 1. There is no offset commit trigger for this pipeline or
        // 2. there is a commit trigger and it is on
        if (offsetCommitTrigger == null || offsetCommitTrigger.commit()) {
          offsetTracker.commitOffset(entityName, newOffset);
        }
      }
    }

    long batchDuration = System.currentTimeMillis() - start;
    batchProcessingTimer.update(batchDuration, TimeUnit.MILLISECONDS);
    batchCountCounter.inc();
    batchCountMeter.mark();
    batchInputRecordsHistogram.update(pipeBatch.getInputRecords());
    batchOutputRecordsHistogram.update(pipeBatch.getOutputRecords());
    batchErrorRecordsHistogram.update(pipeBatch.getErrorRecords());
    batchErrorsHistogram.update(pipeBatch.getErrorMessages());
    batchInputRecordsMeter.mark(pipeBatch.getInputRecords());
    batchOutputRecordsMeter.mark(pipeBatch.getOutputRecords());
    batchErrorRecordsMeter.mark(pipeBatch.getErrorRecords());
    batchErrorMessagesMeter.mark(pipeBatch.getErrorMessages());
    batchInputRecordsCounter.inc(pipeBatch.getInputRecords());
    batchOutputRecordsCounter.inc(pipeBatch.getOutputRecords());
    batchErrorRecordsCounter.inc(pipeBatch.getErrorRecords());
    batchErrorMessagesCounter.inc(pipeBatch.getErrorMessages());

    if (pipeContext != null) {
      pipeContext.getRuntimeStats().setLastBatchInputRecordsCount(pipeBatch.getInputRecords());
      pipeContext.getRuntimeStats().setLastBatchOutputRecordsCount((pipeBatch.getOutputRecords()));
      pipeContext.getRuntimeStats().setLastBatchErrorRecordsCount(pipeBatch.getErrorRecords());
      pipeContext.getRuntimeStats().setLastBatchErrorMessagesCount(pipeBatch.getErrorMessages());
    }

    if (isStatsAggregationEnabled()) {
      Map<String, Object> pipelineBatchMetrics = new HashMap<>();
      pipelineBatchMetrics.put(AggregatorUtil.PIPELINE_BATCH_DURATION, batchDuration);
      pipelineBatchMetrics.put(AggregatorUtil.BATCH_COUNT, 1);
      pipelineBatchMetrics.put(AggregatorUtil.BATCH_INPUT_RECORDS, pipeBatch.getInputRecords());
      pipelineBatchMetrics.put(AggregatorUtil.BATCH_OUTPUT_RECORDS, pipeBatch.getOutputRecords());
      pipelineBatchMetrics.put(AggregatorUtil.BATCH_ERROR_RECORDS, pipeBatch.getErrorRecords());
      pipelineBatchMetrics.put(AggregatorUtil.BATCH_ERRORS, pipeBatch.getErrorMessages());
      pipelineBatchMetrics.put(AggregatorUtil.STAGE_BATCH_METRICS, stageBatchMetrics);

      AggregatorUtil.enqueStatsRecord(
          AggregatorUtil.createMetricRecord(pipelineBatchMetrics),
          statsAggregatorRequests,
          configuration
      );
    }

    synchronized (this) {
      List<StageOutput> snapshot = pipeBatch.getSnapshotsOfAllStagesOutput();
      if( batchesToCapture > 0 && ValidationUtil.isSnapshotOutputUsable(pipeBatch.getSnapshotsOfAllStagesOutput())) {
        if (!snapshot.isEmpty()) {
          capturedBatches.add(snapshot);
        }
        /*
         * Reset the capture snapshot variable only after capturing the snapshot
         * This guarantees that once captureSnapshot is called, the output is captured exactly once
         * */
        batchesToCapture--;
        if (batchesToCapture == 0) {
          snapshotBatchSize = 0;
          batchesToCapture = 0;
          if (!capturedBatches.isEmpty()) {
            snapshotStore.save(pipelineName, revision, snapshotName, batchCountMeter.getCount(), capturedBatches);
            capturedBatches.clear();
          }
        }
      }
    }

    // Retain X number of error records per stage
    Map<String, List<Record>> errorRecords = pipeBatch.getErrorSink().getErrorRecords();
    Map<String, List<ErrorMessage>> errorMessages = pipeBatch.getErrorSink().getStageErrors();
    retainErrorMessagesInMemory(errorMessages);
    retainErrorRecordsInMemory(errorRecords);

    // Write Pipeline data rule and drift rule results to aggregator target
    if (isStatsAggregationEnabled()) {
      List<Record> stats = new ArrayList<>();
      statsAggregatorRequests.drainTo(stats);
      statsAggregationHandler.handle(entityName, previousOffset, stats);
    }
  }

  /**
   * This method should be called periodically from a scheduler if the pipeline should not allow runners to be "idle"
   * for more then idleTime.
   *
   * @param idleTime Number of milliseconds after which a runner is considered "idle"
   * @return Number of idle batches generated in this iteration
   */
  public int produceEmptyBatchesForIdleRunners(long idleTime) throws PipelineException, StageException {
    LOG.debug("Checking if any active runner is idle");

    // The empty batch is suppose to be fast - almost as a zero time. It could however happened that from some reason it
    // will take a long time (possibly more then idleTime). To avoid infinite loops, this method will only processes up
    // to total number of runners before returning.
    int counter = 0;

    try {
      destroyLock.lock();

      while(running && counter < pipes.size()) {
        counter++;

        PipeRunner runner = null;
        try {
          runner = runnerPool.getIdleRunner(idleTime);

          // No more idle runners, simply stop the idle execution now
          if(runner == null) {
            return counter;
          }

          LOG.debug("Generating empty batch for runner: {}", runner.getRunnerId());
          pipeContext.getRuntimeStats().incIdleBatchCount();

          // Pipe batch to keep the batch info
          FullPipeBatch pipeBatch = new FullPipeBatch(null, null, 0, false);
          pipeBatch.setIdleBatch(true);

          // We're explicitly skipping origin because this is framework generated, empty batch
          pipeBatch.skipStage(originPipe);

          executeRunner(
            runner,
            System.currentTimeMillis(),
            pipeBatch,
            null,
            null,
            new HashMap<>(),
            new HashMap<>()
          );
        } finally {
          if(runner != null) {
            runnerPool.returnRunner(runner);
          }
        }
      }
    } finally {
      destroyLock.unlock();
    }

    return counter;
  }

  private void sendPipelineErrorNotificationRequest(Throwable throwable) {
    boolean offered = false;
    try {
      observeRequests.put(new PipelineErrorNotificationRequest(throwable));
      offered = true;
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while sending pipeline error notification request.");
      Thread.currentThread().interrupt();
    }
    if(!offered) {
      LOG.error("Could not submit alert request for pipeline ending error: {}", throwable, throwable);
    }
  }

  public SourceOffsetTracker getOffSetTracker() {
    return this.offsetTracker;
  }

  private void retainErrorMessagesInMemory(Map<String, List<ErrorMessage>> errorMessages) {
    // Shortcut to avoid synchronization
    if(errorMessages.isEmpty()) {
      return;
    }

    synchronized (stageToErrorMessagesMap) {
      for (Map.Entry<String, List<ErrorMessage>> e : errorMessages.entrySet()) {
        EvictingQueue<ErrorMessage> errorMessageList = stageToErrorMessagesMap.computeIfAbsent(e.getKey(),
            k -> EvictingQueue.create(configuration.get(Constants.MAX_PIPELINE_ERRORS_KEY,
                Constants.MAX_PIPELINE_ERRORS_DEFAULT
            ))
        );
        errorMessageList.addAll(errorMessages.get(e.getKey()));
      }
    }
  }

  private void retainErrorRecordsInMemory(Map<String, List<Record>> errorRecords) {
    // Shortcut to avoid synchronization
    if(errorRecords.isEmpty()) {
      return;
    }

    synchronized (stageToErrorRecordsMap) {
      for (Map.Entry<String, List<Record>> e : errorRecords.entrySet()) {
        EvictingQueue<Record> errorRecordList = stageToErrorRecordsMap.computeIfAbsent(e.getKey(),
            k -> EvictingQueue.create(configuration.get(Constants.MAX_ERROR_RECORDS_PER_STAGE_KEY,
                Constants.MAX_ERROR_RECORDS_PER_STAGE_DEFAULT
            ))
        );
        // replace with a data structure with an upper cap
        errorRecordList.addAll(errorRecords.get(e.getKey()));
      }
    }
  }

  @SuppressWarnings("unchecked")
  public List<Record> getErrorRecords(String instanceName, int size) {
    synchronized (stageToErrorRecordsMap) {
      if (stageToErrorRecordsMap == null || stageToErrorRecordsMap.isEmpty()
        || stageToErrorRecordsMap.get(instanceName) == null || stageToErrorRecordsMap.get(instanceName).isEmpty()) {
        return Collections.emptyList();
      }
      if (stageToErrorRecordsMap.get(instanceName).size() > size) {
        return new CopyOnWriteArrayList<>(stageToErrorRecordsMap.get(instanceName)).subList(0, size);
      } else {
        return new CopyOnWriteArrayList<>(stageToErrorRecordsMap.get(instanceName));
      }
    }
  }

  public List<ErrorMessage> getErrorMessages(String instanceName, int size) {
    synchronized (stageToErrorMessagesMap) {
      if (stageToErrorMessagesMap == null || stageToErrorMessagesMap.isEmpty()
        || stageToErrorMessagesMap.get(instanceName) == null || stageToErrorMessagesMap.get(instanceName).isEmpty()) {
        return Collections.emptyList();
      }
      if (stageToErrorMessagesMap.get(instanceName).size() > size) {
        return new CopyOnWriteArrayList<>(stageToErrorMessagesMap.get(instanceName)).subList(0, size);
      } else {
        return new CopyOnWriteArrayList<>(stageToErrorMessagesMap.get(instanceName));
      }
    }
  }

  /**
   * Create special batch by salvaging memory structures when pipelines gets into un-recoverable error.
   *
   * @param pipeBatch Current batch
   */
  private void createFailureBatch(FullPipeBatch pipeBatch) {
    if(!pipelineConfigBean.shouldCreateFailureSnapshot) {
      return;
    }

    try {
      for(SnapshotInfo info : snapshotStore.getSummaryForPipeline(pipelineName, revision)) {
        // Allow only one failure snapshot to be present on a pipeline
        if(info.isFailureSnapshot()) {
          LOG.trace("Skipping creation of failure snapshot as {} already exists.", info.getId());
          return;
        }
      }

      String snapshotName = "Failure_" + UUID.randomUUID().toString();
      String snapshotLabel = "Failure at " + LocalDateTime.now().toString();
      snapshotStore.create("", pipelineName, revision, snapshotName, snapshotLabel, true);
      snapshotStore.save(pipelineName, revision, snapshotName, -1, ImmutableList.of(pipeBatch.createFailureSnapshot()));
    } catch (PipelineException ex) {
      LOG.error("Can't serialize failure snapshot", ex);
    }
  }

  @Override
  public void registerListener(BatchListener batchListener) {
    batchListenerList.add(batchListener);
  }

  private boolean isStatsAggregationEnabled() {
    return null != statsAggregatorRequests;
  }

  public void setRuntimeConfiguration(
    PipeContext pipeContext,
    PipelineConfiguration pipelineConfiguration,
    PipelineConfigBean pipelineConfigBean
  ) {
    this.pipeContext = pipeContext;
    this.pipelineConfiguration = pipelineConfiguration;
    this.pipelineConfigBean = pipelineConfigBean;
  }
}
