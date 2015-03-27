/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.production;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import com.google.common.collect.EvictingQueue;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.config.DeliveryGuarantee;
import com.streamsets.pipeline.config.StageType;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.metrics.MetricsConfigurator;
import com.streamsets.pipeline.prodmanager.Configuration;
import com.streamsets.pipeline.record.HeaderImpl;
import com.streamsets.pipeline.record.RecordImpl;
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
import com.streamsets.pipeline.snapshotstore.SnapshotStore;
import com.streamsets.pipeline.snapshotstore.impl.FileSnapshotStore;
import com.streamsets.pipeline.store.PipelineStoreTask;
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

  private Object errorRecordsMutex;

  public ProductionPipelineRunner(RuntimeInfo runtimeInfo, SnapshotStore snapshotStore,
                                  DeliveryGuarantee deliveryGuarantee, String pipelineName, String revision,
                                  PipelineStoreTask pipelineStore, BlockingQueue<Object> observeRequests,
                                  com.streamsets.pipeline.util.Configuration configuration) {
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

  @Override
  public void run(Pipe[] pipes, BadRecordsHandler badRecordsHandler) throws StageException, PipelineRuntimeException {
    while(!offsetTracker.isFinished() && !stop) {
      runBatch(pipes, badRecordsHandler);
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
    if(snapshotStore.getSnapshotStatus(pipelineName, revision, snapshotName).isExists()) {
      batchOutput.add(snapshotStore.retrieveSnapshot(pipelineName, revision, snapshotName));
    }
    return batchOutput;
  }

  public String getSourceOffset() {
    return sourceOffset;
  }

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
    this.captureNextBatch = true;
    ((FileSnapshotStore)snapshotStore).setInProgress(pipelineName, revision, snapshotName, true);
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
        configuration.get(Configuration.MAX_BATCH_SIZE_KEY, Configuration.MAX_BATCH_SIZE_DEFAULT),
        false /*snapshot stage output*/);
    }

    long start = System.currentTimeMillis();
    sourceOffset = pipeBatch.getPreviousOffset();
    long lastBatchTime = offsetTracker.getLastBatchTime();
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
    }
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

    //fire metric alert evaluation request
    boolean offered;
    try {
      offered = observeRequests.offer(new MetricRulesEvaluationRequest(),
        configuration.get(Configuration.MAX_OBSERVER_REQUEST_OFFER_WAIT_TIME_MS_KEY,
          Configuration.MAX_OBSERVER_REQUEST_OFFER_WAIT_TIME_MS_DEFAULT), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      offered = false;
    }
    if(!offered) {
      LOG.debug("Dropping metric alert evaluation request as observer queue is full. " +
        "Please resize the observer queue or decrease the sampling percentage.");
      //raise alert to say that we dropped batch
      //reconfigure queue size or tune sampling %
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
          errorRecordList = EvictingQueue.create(configuration.get(Configuration.MAX_ERROR_RECORDS_PER_STAGE_KEY,
            Configuration.MAX_ERROR_RECORDS_PER_STAGE_DEFAULT));
          stageToErrorRecordsMap.put(e.getKey(), errorRecordList);
        }
        errorRecordList.addAll(errorRecords.get(e.getKey()));
      }
      for (Map.Entry<String, List<ErrorMessage>> e : errorMessages.entrySet()) {
        EvictingQueue<ErrorMessage> errorMessageList = stageToErrorMessagesMap.get(e.getKey());
        if (errorMessageList == null) {
          //replace with a data structure with an upper cap
          errorMessageList = EvictingQueue.create(configuration.get(Configuration.MAX_PIPELINE_ERRORS_KEY,
            Configuration.MAX_PIPELINE_ERRORS_DEFAULT));
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
}
