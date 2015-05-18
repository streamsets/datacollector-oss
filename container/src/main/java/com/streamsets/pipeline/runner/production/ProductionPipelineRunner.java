/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.production;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.EvictingQueue;
import com.streamsets.pipeline.api.ErrorListener;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.DeliveryGuarantee;
import com.streamsets.pipeline.config.MemoryLimitConfiguration;
import com.streamsets.pipeline.config.MemoryLimitExceeded;
import com.streamsets.pipeline.config.StageType;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.metrics.MetricsConfigurator;
import com.streamsets.pipeline.prodmanager.Constants;
import com.streamsets.pipeline.record.HeaderImpl;
import com.streamsets.pipeline.record.RecordImpl;
import com.streamsets.pipeline.runner.BatchListener;
import com.streamsets.pipeline.runner.ErrorSink;
import com.streamsets.pipeline.runner.FullPipeBatch;
import com.streamsets.pipeline.runner.Observer;
import com.streamsets.pipeline.runner.Pipe;
import com.streamsets.pipeline.runner.PipeBatch;
import com.streamsets.pipeline.runner.PipelineRunner;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.runner.SourceOffsetTracker;
import com.streamsets.pipeline.runner.StageContext;
import com.streamsets.pipeline.runner.StageOutput;
import com.streamsets.pipeline.snapshotstore.SnapshotStatus;
import com.streamsets.pipeline.snapshotstore.SnapshotStore;
import com.streamsets.pipeline.snapshotstore.impl.FileSnapshotStore;
import com.streamsets.pipeline.util.ContainerError;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

public class ProductionPipelineRunner implements PipelineRunner {

  private static final Logger LOG = LoggerFactory.getLogger(ProductionPipelineRunner.class);

  private final RuntimeInfo runtimeInfo;
  private final com.streamsets.pipeline.util.Configuration configuration;
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
  private final Histogram batchInputRecordsHistogram;
  private final Histogram batchOutputRecordsHistogram;
  private final Histogram batchErrorRecordsHistogram;
  private final Histogram batchErrorsHistogram;
  private final Meter batchInputRecordsMeter;
  private final Meter batchOutputRecordsMeter;
  private final Meter batchErrorRecordsMeter;
  private final Meter batchErrorMessagesMeter;
  private final Counter memoryConsumedCounter;

  /*indicates if the execution must be stopped after the current batch*/
  private volatile boolean stop = false;
  /*indicates if the next batch of data should be captured, only the next batch*/
  private volatile boolean captureNextBatch = false;
  /*indicates the snapshot name to be captured*/
  private volatile String snapshotName;
  /*indicates the batch size to be captured*/
  private volatile int snapshotBatchSize;
  /*Cache last N error records per stage in memory*/
  private Map<String, EvictingQueue<Record>> stageToErrorRecordsMap;
  /*Cache last N error messages in memory*/
  private Map<String, EvictingQueue<ErrorMessage>> stageToErrorMessagesMap;
  /**/
  private BlockingQueue<Object> observeRequests;
  private Observer observer;
  private List<BatchListener> batchListenerList = new CopyOnWriteArrayList<BatchListener>();
  private Object errorRecordsMutex;
  private final MemoryLimitConfiguration memoryLimitConfiguration;
  private long lastMemoryLimitNotification;
  private ThreadHealthReporter threadHealthReporter;

  public ProductionPipelineRunner(RuntimeInfo runtimeInfo, SnapshotStore snapshotStore,
                                  DeliveryGuarantee deliveryGuarantee, String pipelineName, String revision,
                                  BlockingQueue<Object> observeRequests,
                                  com.streamsets.pipeline.util.Configuration configuration,
                                  MemoryLimitConfiguration memoryLimitConfiguration) {
    /*//retrieve pipeline properties from the pipeline configuration
    int maxBatchSize = configuration.get(Configuration.MAX_BATCH_SIZE_KEY, Configuration.MAX_BATCH_SIZE_DEFAULT);
    int maxErrorRecordsPerStage = configuration.get(Configuration.MAX_ERROR_RECORDS_PER_STAGE_KEY,
      Configuration.MAX_ERROR_RECORDS_PER_STAGE_DEFAULT);
    int maxPipelineErrors = configuration.get(Configuration.MAX_PIPELINE_ERRORS_KEY,
      Configuration.MAX_PIPELINE_ERRORS_DEFAULT);*/
    this.runtimeInfo = runtimeInfo;
    this.configuration = configuration;
    this.metrics = new MetricRegistry();
    this.observeRequests = observeRequests;
    this.memoryLimitConfiguration = memoryLimitConfiguration;

    batchProcessingTimer = MetricsConfigurator.createTimer(metrics, "pipeline.batchProcessing");
    batchCountMeter = MetricsConfigurator.createMeter(metrics, "pipeline.batchCount");
    batchInputRecordsHistogram = MetricsConfigurator.createHistogram5Min(metrics, "pipeline.inputRecordsPerBatch");
    batchOutputRecordsHistogram = MetricsConfigurator.createHistogram5Min(metrics, "pipeline.outputRecordsPerBatch");
    batchErrorRecordsHistogram = MetricsConfigurator.createHistogram5Min(metrics, "pipeline.errorRecordsPerBatch");
    batchErrorsHistogram = MetricsConfigurator.createHistogram5Min(metrics, "pipeline.errorsPerBatch");
    batchInputRecordsMeter = MetricsConfigurator.createMeter(metrics, "pipeline.batchInputRecords");
    batchOutputRecordsMeter = MetricsConfigurator.createMeter(metrics, "pipeline.batchOutputRecords");
    batchErrorRecordsMeter = MetricsConfigurator.createMeter(metrics, "pipeline.batchErrorRecords");
    batchErrorMessagesMeter = MetricsConfigurator.createMeter(metrics, "pipeline.batchErrorMessages");
    memoryConsumedCounter = MetricsConfigurator.createCounter(metrics, "pipeline.memoryConsumed");

    this.deliveryGuarantee = deliveryGuarantee;
    this.snapshotStore = snapshotStore;
    this.pipelineName = pipelineName;
    this.revision = revision;
    this.stageToErrorRecordsMap = new HashMap<>();
    this.stageToErrorMessagesMap = new HashMap<>();
    errorRecordsMutex = new Object();
  }

  @Override
  public boolean isPreview() {
    return false;
  }

  @Override
  public MetricRegistry getMetrics() {
    return metrics;
  }

  public void setOffsetTracker(SourceOffsetTracker offsetTracker) {
    this.offsetTracker = offsetTracker;
  }

  public void setThreadHealthReporter(ThreadHealthReporter threadHealthReporter) {
    this.threadHealthReporter = threadHealthReporter;
  }

  @Override
  public void run(Pipe[] pipes, BadRecordsHandler badRecordsHandler) throws StageException, PipelineRuntimeException {
    while(!offsetTracker.isFinished() && !stop) {
      if(threadHealthReporter != null) {
        threadHealthReporter.reportHealth(ProductionPipelineRunnable.RUNNABLE_NAME, -1, System.currentTimeMillis());
      }
      try {
        for (BatchListener batchListener: batchListenerList) {
          batchListener.preBatch();
        }
        runBatch(pipes, badRecordsHandler);
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

  private void errorNotification(Pipe[] pipes, Throwable throwable) throws StageException {
    for (Pipe pipe : pipes) {
      Stage stage = pipe.getStage().getStage();
      if (stage instanceof ErrorListener) {
        try {
          ((ErrorListener) stage).errorNotification(throwable);
        } catch (Exception ex) {
          String msg = Utils.format("Error in calling ErrorListenerStage {}: {}", stage.getClass().getName(), ex);
          LOG.error(msg, ex);
        }
      }
    }
  }

  @Override
  public void run(Pipe[] pipes, BadRecordsHandler badRecordsHandler, List<StageOutput> stageOutputsToOverride)
      throws StageException, PipelineRuntimeException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<List<StageOutput>> getBatchesOutput() {
    List<List<StageOutput>> batchOutput = new ArrayList<>();
    SnapshotStatus snapshotStatus = snapshotStore.getSnapshotStatus(pipelineName, revision, snapshotName);
    if(snapshotStatus != null && snapshotStatus.isExists()) {
      batchOutput.add(snapshotStore.retrieveSnapshot(pipelineName, revision, snapshotName));
    }
    return batchOutput;
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
    this.observer = observer;
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

  public void captureNextBatch(String snapshotName, int batchSize) {
    Preconditions.checkArgument(batchSize > 0);
    this.snapshotName = snapshotName;
    this.snapshotBatchSize = batchSize;
    ((FileSnapshotStore)snapshotStore).setInProgress(pipelineName, revision, snapshotName, true);
    this.captureNextBatch = true;
  }

  private void runBatch(Pipe[] pipes, BadRecordsHandler badRecordsHandler) throws PipelineRuntimeException, StageException {
    boolean committed = false;
    /*value true indicates that this batch is captured */
    boolean batchCaptured = false;
    PipeBatch pipeBatch;
    //pick up any recent changes done to the rule definitions
    if(observer != null) {
      observer.reconfigure();
    }

    if(captureNextBatch) {
      batchCaptured = true;
      pipeBatch = new FullPipeBatch(offsetTracker, snapshotBatchSize, true /*snapshot stage output*/);
    } else {
      pipeBatch = new FullPipeBatch(offsetTracker,
        configuration.get(Constants.MAX_BATCH_SIZE_KEY, Constants.MAX_BATCH_SIZE_DEFAULT),
        false /*snapshot stage output*/);
    }

    long start = System.currentTimeMillis();
    sourceOffset = pipeBatch.getPreviousOffset();
    long lastBatchTime = offsetTracker.getLastBatchTime();
    Map<String, Long> memoryConsumedByStage = new HashMap<>();
    for (Pipe pipe : pipes) {
      //set the last batch time in the stage context of each pipe
      ((StageContext)pipe.getStage().getContext()).setLastBatchTime(lastBatchTime);
      //TODO Define an interface to handle delivery guarantee
      if (deliveryGuarantee == DeliveryGuarantee.AT_MOST_ONCE
          && pipe.getStage().getDefinition().getType() == StageType.TARGET && !committed) {
        offsetTracker.commitOffset();
        committed = true;
      }
      pipe.process(pipeBatch);
      long memory = pipe.getMemoryConsumed();
      memoryConsumedByStage.put(pipe.getStage().getInfo().getInstanceName(), memory);
    }
    enforceMemoryLimit(memoryConsumedByStage);
    badRecordsHandler.handle(newSourceOffset, getBadRecords(pipeBatch.getErrorSink()));
    if (deliveryGuarantee == DeliveryGuarantee.AT_LEAST_ONCE) {
      offsetTracker.commitOffset();
    }

    batchProcessingTimer.update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
    batchCountMeter.mark();
    batchInputRecordsHistogram.update(pipeBatch.getInputRecords());
    batchOutputRecordsHistogram.update(pipeBatch.getOutputRecords());
    batchErrorRecordsHistogram.update(pipeBatch.getErrorRecords());
    batchErrorsHistogram.update(pipeBatch.getErrorMessages());
    batchInputRecordsMeter.mark(pipeBatch.getInputRecords());
    batchOutputRecordsMeter.mark(pipeBatch.getOutputRecords());
    batchErrorRecordsMeter.mark(pipeBatch.getErrorRecords());
    batchErrorMessagesMeter.mark(pipeBatch.getErrorMessages());

    newSourceOffset = offsetTracker.getOffset();

    if(batchCaptured) {
      List<StageOutput> snapshot = pipeBatch.getSnapshotsOfAllStagesOutput();
      snapshotStore.storeSnapshot(pipelineName, revision, snapshotName, snapshot);
      /*
       * Reset the capture snapshot variable only after capturing the snapshot
       * This guarantees that once captureSnapshot is called, the output is captured exactly once
       * */
      captureNextBatch = false;
      snapshotBatchSize = 0;
    }

    //Retain X number of error records per stage
    Map<String, List<Record>> errorRecords = pipeBatch.getErrorSink().getErrorRecords();
    Map<String, List<ErrorMessage>> errorMessages = pipeBatch.getErrorSink().getStageErrors();
    retainErrorsInMemory(errorRecords, errorMessages);
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
    long memoryLimit = memoryLimitConfiguration.getMemoryLimit() * 1000 * 1000;
    if (memoryLimit > 0 && totalMemoryConsumed > memoryLimit) {
      String largestConsumer = "unknown";
      long largestConsumed = 0;
      Map<String, String> humanReadbleMemoryConsumptionByStage = new HashMap<>();
      for(Map.Entry<String, Long> entry : memoryConsumedByStage.entrySet()) {
        if (entry.getValue() > largestConsumed) {
          largestConsumed = entry.getValue();
          largestConsumer = entry.getKey();
        }
        humanReadbleMemoryConsumptionByStage.put(entry.getKey(), Utils.humanReadableInt(entry.getValue()));
      }
      humanReadbleMemoryConsumptionByStage.remove(largestConsumer);
      PipelineRuntimeException ex = new PipelineRuntimeException(ContainerError.CONTAINER_0011,
        Utils.humanReadableInt(totalMemoryConsumed), Utils.humanReadableInt(memoryLimit),
        largestConsumer, Utils.humanReadableInt(largestConsumed), humanReadbleMemoryConsumptionByStage);
      String msg = "Pipeline memory limit exceeded: " + ex;
      long elapsedTimeSinceLastTriggerMins = TimeUnit.MILLISECONDS.
        toMinutes(System.currentTimeMillis() - lastMemoryLimitNotification);
      lastMemoryLimitNotification = System.currentTimeMillis();
      if (elapsedTimeSinceLastTriggerMins < 60 ) {
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
    }

    if(stageToErrorRecordsMap.get(instanceName).size() > size) {
      return new CopyOnWriteArrayList<>(stageToErrorRecordsMap.get(instanceName)).subList(0, size);
    } else {
      return new CopyOnWriteArrayList<>(stageToErrorRecordsMap.get(instanceName));
    }
  }

  public List<ErrorMessage> getErrorMessages(String instanceName, int size) {
    synchronized (errorRecordsMutex) {
      if (stageToErrorMessagesMap == null || stageToErrorMessagesMap.isEmpty()
        || stageToErrorMessagesMap.get(instanceName) == null
        || stageToErrorMessagesMap.get(instanceName).isEmpty()) {
        return Collections.emptyList();
      }
    }

    if(stageToErrorMessagesMap.get(instanceName).size() > size) {
      return new CopyOnWriteArrayList<>(stageToErrorMessagesMap.get(instanceName)).subList(0, size);
    } else {
      return new CopyOnWriteArrayList<>(stageToErrorMessagesMap.get(instanceName));
    }
  }

  @Override
  public void registerListener(BatchListener batchListener) {
    batchListenerList.add(batchListener);
  }
}
