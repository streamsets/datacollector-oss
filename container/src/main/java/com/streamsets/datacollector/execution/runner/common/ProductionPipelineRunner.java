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
package com.streamsets.datacollector.execution.runner.common;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.EvictingQueue;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.RateLimiter;
import com.streamsets.datacollector.config.DeliveryGuarantee;
import com.streamsets.datacollector.config.MemoryLimitConfiguration;
import com.streamsets.datacollector.config.MemoryLimitExceeded;
import com.streamsets.datacollector.config.StageType;
import com.streamsets.datacollector.execution.SnapshotStore;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.metrics.MetricsConfigurator;
import com.streamsets.datacollector.record.HeaderImpl;
import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.datacollector.restapi.bean.CounterJson;
import com.streamsets.datacollector.restapi.bean.HistogramJson;
import com.streamsets.datacollector.restapi.bean.MeterJson;
import com.streamsets.datacollector.restapi.bean.MetricRegistryJson;
import com.streamsets.datacollector.runner.BatchListener;
import com.streamsets.datacollector.runner.ErrorSink;
import com.streamsets.datacollector.runner.FullPipeBatch;
import com.streamsets.datacollector.runner.Observer;
import com.streamsets.datacollector.runner.Pipe;
import com.streamsets.datacollector.runner.PipeBatch;
import com.streamsets.datacollector.runner.PipelineRunner;
import com.streamsets.datacollector.runner.PipelineRuntimeException;
import com.streamsets.datacollector.runner.SourceOffsetTracker;
import com.streamsets.datacollector.runner.StageContext;
import com.streamsets.datacollector.runner.StageOutput;
import com.streamsets.datacollector.runner.StagePipe;
import com.streamsets.datacollector.runner.production.BadRecordsHandler;
import com.streamsets.datacollector.runner.production.PipelineErrorNotificationRequest;
import com.streamsets.datacollector.runner.production.StatsAggregationHandler;
import com.streamsets.datacollector.util.AggregatorUtil;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.pipeline.api.ErrorListener;
import com.streamsets.pipeline.api.OffsetCommitTrigger;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;


public class ProductionPipelineRunner implements PipelineRunner {

  private static final Logger LOG = LoggerFactory.getLogger(ProductionPipelineRunner.class);

  private final RuntimeInfo runtimeInfo;
  private final com.streamsets.datacollector.util.Configuration configuration;
  private final MetricRegistry metrics;
  private SourceOffsetTracker offsetTracker;
  private final SnapshotStore snapshotStore;
  private String sourceOffset;
  private String newSourceOffset;
  private DeliveryGuarantee deliveryGuarantee;
  private final String pipelineName;
  private final String revision;

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
  private final Counter memoryConsumedCounter;
  private MetricRegistryJson metricRegistryJson;
  private Long rateLimit;

  private RateLimiter rateLimiter;

  /*indicates if the execution must be stopped after the current batch*/
  private volatile boolean stop = false;
  /*indicates if the next batch of data should be captured, only the next batch*/
  private volatile int batchesToCapture = 0;
  /*indicates the snapshot name to be captured*/
  private volatile String snapshotName;
  /*indicates the batch size to be captured*/
  private volatile int snapshotBatchSize;
  /*Cache last N error records per stage in memory*/
  private final Map<String, EvictingQueue<Record>> stageToErrorRecordsMap;
  /*Cache last N error messages in memory*/
  private final Map<String, EvictingQueue<ErrorMessage>> stageToErrorMessagesMap;
  /**/
  private BlockingQueue<Object> observeRequests;
  private Observer observer;
  private BlockingQueue<Record> statsAggregatorRequests;
  private final List<BatchListener> batchListenerList = new CopyOnWriteArrayList<>();
  private final Object errorRecordsMutex;
  private MemoryLimitConfiguration memoryLimitConfiguration;
  private long lastMemoryLimitNotification;
  private ThreadHealthReporter threadHealthReporter;
  private final List<List<StageOutput>> capturedBatches = new ArrayList<>();

  @Inject
  public ProductionPipelineRunner(@Named("name") String pipelineName, @Named ("rev") String revision,
                                  com.streamsets.datacollector.util.Configuration configuration,
                                  RuntimeInfo runtimeInfo, MetricRegistry metrics, SnapshotStore snapshotStore,
                                  ThreadHealthReporter threadHealthReporter) {
    this.runtimeInfo = runtimeInfo;
    this.configuration = configuration;
    this.metrics = metrics;
    this.threadHealthReporter = threadHealthReporter;
    this.snapshotStore = snapshotStore;
    this.pipelineName = pipelineName;
    this.revision = revision;
    stageToErrorRecordsMap = new HashMap<>();
    stageToErrorMessagesMap = new HashMap<>();
    errorRecordsMutex = new Object();

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
    memoryConsumedCounter = MetricsConfigurator.createCounter(metrics, "pipeline.memoryConsumed", pipelineName,
      revision);
  }

  public void setObserveRequests(BlockingQueue<Object> observeRequests) {
    this.observeRequests = observeRequests;
  }

  public void setStatsAggregatorRequests(BlockingQueue<Record> statsAggregatorRequests) {
    this.statsAggregatorRequests = statsAggregatorRequests;
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
    CounterJson memoryConsumer = metricRegistryJson.getCounters().get("pipeline.memoryConsumed" + MetricsConfigurator.COUNTER_SUFFIX);
    memoryConsumedCounter.inc(memoryConsumer.getCount());
  }

  @Override
  public MetricRegistryJson getMetricRegistryJson() {
    return this.metricRegistryJson;
  }

  public void setDeliveryGuarantee(DeliveryGuarantee deliveryGuarantee) {
    this.deliveryGuarantee = deliveryGuarantee;
  }

  public void setMemoryLimitConfiguration(MemoryLimitConfiguration memoryLimitConfiguration) {
    this.memoryLimitConfiguration = memoryLimitConfiguration;
  }

  public void setRateLimit(Long rateLimit) {
    this.rateLimit = rateLimit;
    rateLimiter = RateLimiter.create(rateLimit.doubleValue());
  }

  public void setOffsetTracker(SourceOffsetTracker offsetTracker) {
    this.offsetTracker = offsetTracker;
  }

  public void setThreadHealthReporter(ThreadHealthReporter threadHealthReporter) {
    this.threadHealthReporter = threadHealthReporter;
  }

  @Override
  public void setObserver(Observer observer) {
    this.observer = observer;
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
  public void run(
      Pipe[] pipes,
      BadRecordsHandler badRecordsHandler,
      StatsAggregationHandler statsAggregationHandler
  ) throws StageException, PipelineRuntimeException {

    OffsetCommitTrigger offsetCommitTrigger = getOffsetCommitTrigger(pipes);

    while (!offsetTracker.isFinished() && !stop) {
      if (threadHealthReporter != null) {
        threadHealthReporter.reportHealth(ProductionPipelineRunnable.RUNNABLE_NAME, -1, System.currentTimeMillis());
      }
      try {
        for (BatchListener batchListener : batchListenerList) {
          batchListener.preBatch();
        }
        runBatch(pipes, badRecordsHandler, statsAggregationHandler, offsetCommitTrigger);
        for (BatchListener batchListener : batchListenerList) {
          batchListener.postBatch();
        }
      } catch (Throwable throwable) {
        sendPipelineErrorNotificationRequest(throwable);
        errorNotification(pipes, throwable);
        Throwables.propagateIfInstanceOf(throwable, StageException.class);
        Throwables.propagateIfInstanceOf(throwable, PipelineRuntimeException.class);
        Throwables.propagate(throwable);
      }
    }
  }

  @Override
  public void errorNotification(Pipe[] pipes, Throwable throwable) {
    Set<ErrorListener> listeners = Sets.newIdentityHashSet();
    for (BatchListener batchListener : batchListenerList) {
      batchListener.postBatch();
    }
    for (Pipe pipe : pipes) {
      Stage stage = pipe.getStage().getStage();
      if (stage instanceof ErrorListener) {
        listeners.add((ErrorListener) stage);
      }
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
      Pipe[] pipes,
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
  public void destroy(Pipe[] pipes, BadRecordsHandler badRecordsHandler, StatsAggregationHandler statsAggregationHandler) throws StageException, PipelineRuntimeException {
    FullPipeBatch pipeBatch = new FullPipeBatch(
      offsetTracker,
      configuration.get(Constants.MAX_BATCH_SIZE_KEY, Constants.MAX_BATCH_SIZE_DEFAULT),
      false
    );

    pipeBatch.setRateLimiter(rateLimiter);
    long lastBatchTime = offsetTracker.getLastBatchTime();
    for (Pipe pipe : pipes) {
      // Set the last batch time in the stage context of each pipe
      ((StageContext)pipe.getStage().getContext()).setLastBatchTime(lastBatchTime);
      String instanceName = pipe.getStage().getConfiguration().getInstanceName();

      if(pipe instanceof StagePipe) {
        // Stage pipes are processed only if they are in event path
        if(pipe.getStage().getConfiguration().isInEventPath()) {
          LOG.trace("Stage pipe {} is in event path, running last process", instanceName);
          pipe.process(pipeBatch);
        } else {
          LOG.trace("Stage pipe {} is in data path, skipping it's processing.", instanceName);
          pipeBatch.skipStage(pipe);
        }
      } else {
        // Non stage pipes are executed always
        LOG.trace("Non stage pipe {}, running last process", instanceName);
        pipe.process(pipeBatch);
      }

      // And finally destroy the pipe
      try {
        LOG.trace("Running destroy for {}", instanceName);
        pipe.destroy(pipeBatch);
      } catch(RuntimeException e) {
        LOG.warn("Exception throw while destroying pipe", e);
      }
    }
    badRecordsHandler.handle(newSourceOffset, getBadRecords(pipeBatch.getErrorSink()));
  }

  @Override
  public List<List<StageOutput>> getBatchesOutput() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getSourceOffset() {
    return sourceOffset;
  }

  @Override
  public String getNewSourceOffset() {
    return newSourceOffset;
  }

  public String getCommittedOffset() {
    return offsetTracker.getOffset();
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

  private void runBatch(
      Pipe[] pipes,
      BadRecordsHandler badRecordsHandler,
      StatsAggregationHandler statsAggregationHandler,
      OffsetCommitTrigger offsetCommitTrigger
  ) throws PipelineException, StageException {
    boolean committed = false;
    /*value true indicates that this batch is captured */
    boolean batchCaptured = false;
    PipeBatch pipeBatch;
    //pick up any recent changes done to the rule definitions
    if(observer != null) {
      observer.reconfigure();
    }

    if(batchesToCapture > 0) {
      batchCaptured = true;
      pipeBatch = new FullPipeBatch(offsetTracker, snapshotBatchSize, true /*snapshot stage output*/);
    } else {
      pipeBatch = new FullPipeBatch(offsetTracker,
        configuration.get(Constants.MAX_BATCH_SIZE_KEY, Constants.MAX_BATCH_SIZE_DEFAULT),
        false /*snapshot stage output*/);
    }
    ((FullPipeBatch) pipeBatch).setRateLimiter(rateLimiter);
    long start = System.currentTimeMillis();
    sourceOffset = pipeBatch.getPreviousOffset();
    long lastBatchTime = offsetTracker.getLastBatchTime();
    Map<String, Long> memoryConsumedByStage = new HashMap<>();
    Map<String, Object> stageBatchMetrics = new HashMap<>();
    for (Pipe pipe : pipes) {
      //set the last batch time in the stage context of each pipe
      ((StageContext)pipe.getStage().getContext()).setLastBatchTime(lastBatchTime);
      //TODO Define an interface to handle delivery guarantee
      if (deliveryGuarantee == DeliveryGuarantee.AT_MOST_ONCE
          && pipe.getStage().getDefinition().getType() == StageType.TARGET && !committed) {
        // target cannot control offset commit in AT_MOST_ONCE mode
        offsetTracker.commitOffset();
        committed = true;
      }
      pipe.process(pipeBatch);
      if (pipe instanceof StagePipe) {
        memoryConsumedByStage.put(pipe.getStage().getInfo().getInstanceName(), ((StagePipe)pipe).getMemoryConsumed());
        if (isStatsAggregationEnabled()) {
          stageBatchMetrics.put(pipe.getStage().getInfo().getInstanceName(), ((StagePipe) pipe).getBatchMetrics());
        }
      }
    }
    enforceMemoryLimit(memoryConsumedByStage);
    badRecordsHandler.handle(newSourceOffset, getBadRecords(pipeBatch.getErrorSink()));
    if (deliveryGuarantee == DeliveryGuarantee.AT_LEAST_ONCE) {
      // When AT_LEAST_ONCE commit only if
      // 1. There is no offset commit trigger for this pipeline or
      // 2. there is a commit trigger and it is on
      if (offsetCommitTrigger == null || offsetCommitTrigger.commit()) {
        offsetTracker.commitOffset();
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

    newSourceOffset = offsetTracker.getOffset();

    synchronized (this) {
      if(batchCaptured && batchesToCapture > 0) {
        List<StageOutput> snapshot = pipeBatch.getSnapshotsOfAllStagesOutput();
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
            snapshotStore.save(pipelineName, revision, snapshotName, capturedBatches);
            capturedBatches.clear();
          }
        }
      }
    }

    //Retain X number of error records per stage
    Map<String, List<Record>> errorRecords = pipeBatch.getErrorSink().getErrorRecords();
    Map<String, List<ErrorMessage>> errorMessages = pipeBatch.getErrorSink().getStageErrors();
    retainErrorsInMemory(errorRecords, errorMessages);

    // Write Pipeline data rule and drift rule results to aggregator target
    if (isStatsAggregationEnabled()) {
      List<Record> stats = new ArrayList<>();
      statsAggregatorRequests.drainTo(stats);
      statsAggregationHandler.handle(sourceOffset, stats);
    }
  }

  private RecordImpl getSourceRecord(Record record) {
    return (RecordImpl) ((RecordImpl)record).getHeader().getSourceRecord();
  }

  private void injectErrorInfo(RecordImpl sourceRecord, Record record) {
    HeaderImpl header = sourceRecord.getHeader();
    header.copyErrorFrom(record);
    header.setErrorContext(runtimeInfo.getId(), pipelineName);
  }

  private void enforceMemoryLimit(Map<String, Long> memoryConsumedByStage) throws PipelineRuntimeException {
    long totalMemoryConsumed = 0;
    for(Map.Entry<String, Long> entry : memoryConsumedByStage.entrySet()) {
      totalMemoryConsumed += entry.getValue();
    }
    memoryConsumedCounter.inc(totalMemoryConsumed - memoryConsumedCounter.getCount());
    long memoryLimit = memoryLimitConfiguration.getMemoryLimit();
    if (memoryLimit > 0 && totalMemoryConsumed > memoryLimit) {
      String largestConsumer = "unknown";
      long largestConsumed = 0;
      Map<String, String> humanReadbleMemoryConsumptionByStage = new HashMap<>();
      for(Map.Entry<String, Long> entry : memoryConsumedByStage.entrySet()) {
        if (entry.getValue() > largestConsumed) {
          largestConsumed = entry.getValue();
          largestConsumer = entry.getKey();
        }
        humanReadbleMemoryConsumptionByStage.put(entry.getKey(), entry.getValue() + " MB");
      }
      humanReadbleMemoryConsumptionByStage.remove(largestConsumer);
      PipelineRuntimeException ex = new PipelineRuntimeException(ContainerError.CONTAINER_0011, totalMemoryConsumed,
        memoryLimit, largestConsumer, largestConsumed, humanReadbleMemoryConsumptionByStage);
      String msg = "Pipeline memory limit exceeded: " + ex;
      long elapsedTimeSinceLastTriggerMins = TimeUnit.MILLISECONDS.
        toMinutes(System.currentTimeMillis() - lastMemoryLimitNotification);
      lastMemoryLimitNotification = System.currentTimeMillis();
      if (elapsedTimeSinceLastTriggerMins < 60) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Memory limit has been triggered, will take {} in {} mins",
            memoryLimitConfiguration.getMemoryLimitExceeded(), (60 - elapsedTimeSinceLastTriggerMins));
        }
      } else if (memoryLimitConfiguration.getMemoryLimitExceeded() == MemoryLimitExceeded.LOG) {
        LOG.error(msg, ex);
      } else if (memoryLimitConfiguration.getMemoryLimitExceeded() == MemoryLimitExceeded.ALERT) {
        LOG.error(msg, ex);
        sendPipelineErrorNotificationRequest(ex);
      } else if (memoryLimitConfiguration.getMemoryLimitExceeded() == MemoryLimitExceeded.STOP_PIPELINE) {
        throw ex;
      }
    }
  }
  private void sendPipelineErrorNotificationRequest(Throwable throwable) {
    boolean offered = false;
    try {
      observeRequests.put(new PipelineErrorNotificationRequest(throwable));
      offered = true;
    } catch (InterruptedException e) {
    }
    if(!offered) {
      LOG.error("Could not submit alert request for pipeline ending error: " + throwable, throwable);
    }
  }
  private List<Record> getBadRecords(ErrorSink errorSink) throws PipelineRuntimeException {
    List<Record> badRecords = new ArrayList<>();
    for (Map.Entry<String, List<Record>> entry : errorSink.getErrorRecords().entrySet()) {
      for (Record record : entry.getValue()) {
        RecordImpl sourceRecord = getSourceRecord(record);
        injectErrorInfo(sourceRecord, record);
        badRecords.add(sourceRecord);
      }
    }
    return badRecords;
  }

  public SourceOffsetTracker getOffSetTracker() {
    return this.offsetTracker;
  }

  private void retainErrorsInMemory(Map<String, List<Record>> errorRecords, Map<String,
    List<ErrorMessage>> errorMessages) {
    synchronized (errorRecordsMutex) {
      for (Map.Entry<String, List<Record>> e : errorRecords.entrySet()) {
        EvictingQueue<Record> errorRecordList = stageToErrorRecordsMap.get(e.getKey());
        if (errorRecordList == null) {
          //replace with a data structure with an upper cap
          errorRecordList = EvictingQueue.create(configuration.get(Constants.MAX_ERROR_RECORDS_PER_STAGE_KEY,
            Constants.MAX_ERROR_RECORDS_PER_STAGE_DEFAULT));
          stageToErrorRecordsMap.put(e.getKey(), errorRecordList);
        }
        errorRecordList.addAll(errorRecords.get(e.getKey()));
      }
      for (Map.Entry<String, List<ErrorMessage>> e : errorMessages.entrySet()) {
        EvictingQueue<ErrorMessage> errorMessageList = stageToErrorMessagesMap.get(e.getKey());
        if (errorMessageList == null) {
          //replace with a data structure with an upper cap
          errorMessageList = EvictingQueue.create(configuration.get(Constants.MAX_PIPELINE_ERRORS_KEY,
            Constants.MAX_PIPELINE_ERRORS_DEFAULT));
          stageToErrorMessagesMap.put(e.getKey(), errorMessageList);
        }
        errorMessageList.addAll(errorMessages.get(e.getKey()));
      }
    }
  }

  @SuppressWarnings("unchecked")
  public List<Record> getErrorRecords(String instanceName, int size) {
    synchronized (errorRecordsMutex) {
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
    synchronized (errorRecordsMutex) {
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

  @Override
  public void registerListener(BatchListener batchListener) {
    batchListenerList.add(batchListener);
  }

  private boolean isStatsAggregationEnabled() {
    return null != statsAggregatorRequests;
  }

  private OffsetCommitTrigger getOffsetCommitTrigger(Pipe[] pipes) {
    for (Pipe pipe : pipes) {
      Stage stage = pipe.getStage().getStage();
      if (stage instanceof Target &&
        stage instanceof OffsetCommitTrigger) {
        return (OffsetCommitTrigger) stage;
      }
    }
    return null;
  }

}
