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
package com.streamsets.datacollector.runner;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.config.ConfigDefinition;
import com.streamsets.datacollector.config.MemoryLimitConfiguration;
import com.streamsets.datacollector.email.EmailSender;
import com.streamsets.datacollector.lineage.LineageEventImpl;
import com.streamsets.datacollector.lineage.LineagePublisherDelegator;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.record.EventRecordImpl;
import com.streamsets.datacollector.record.HeaderImpl;
import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.datacollector.runner.production.ReportErrorDelegate;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.lib.security.http.RemoteSSOService;
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.DeliveryGuarantee;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageType;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.lineage.LineageEvent;
import com.streamsets.pipeline.api.lineage.LineageEventType;
import com.streamsets.pipeline.api.lineage.LineageSpecificAttribute;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StageContext extends ProtoContext implements
    Source.Context, PushSource.Context, Target.Context, Processor.Context {
  private static final String JOB_ID = "JOB_ID";
  private final int runnerId;
  private final List<Stage.Info> pipelineInfo;
  private final Stage.UserContext userContext;
  private final boolean isPreview;
  private final Stage.Info stageInfo;
  private final List<String> outputLanes;
  private final OnRecordError onRecordError;
  private ErrorSink errorSink;
  private EventSink eventSink;
  private ProcessedSink processedSink;
  private SourceResponseSink sourceResponseSink;
  private long lastBatchTime;
  private final long pipelineMaxMemory;
  private final ExecutionMode executionMode;
  private final DeliveryGuarantee deliveryGuarantee;
  private final String sdcId;
  private final String pipelineTitle;
  private volatile boolean stop;
  private final Map<String, Object> sharedRunnerMap;
  private final long startTime;
  private final LineagePublisherDelegator lineagePublisherDelegator;
  private PipelineFinisherDelegate pipelineFinisherDelegate;
  private RuntimeInfo runtimeInfo;
  private final Map services;
  private final boolean isErrorStage;

  //for SDK
  public StageContext(
      final String instanceName,
      StageType stageType,
      int runnerId,
      boolean isPreview,
      OnRecordError onRecordError,
      List<String> outputLanes,
      Map<String, Class<?>[]> configToElDefMap,
      Map<String, Object> constants,
      ExecutionMode executionMode,
      DeliveryGuarantee deliveryGuarantee,
      String resourcesDir,
      EmailSender emailSender,
      Configuration configuration,
      LineagePublisherDelegator lineagePublisherDelegator,
      RuntimeInfo runtimeInfo,
      Map<Class, Object> services
  ) {
    super(
      configuration,
      configToElDefMap,
      Collections.unmodifiableMap(constants),
      emailSender,
      new MetricRegistry(),
      "myPipeline",
      "0",
      0,
      "x",
      stageType,
      null,
      resourcesDir
    );
    this.pipelineTitle = "My Pipeline";
    this.sdcId = "mySDC";
    // create dummy info for Stage Runners. This is required for stages that expose custom metrics
    this.stageInfo = new Stage.Info() {
      @Override
      public String getName() {
        return "x";
      }

      @Override
      public int getVersion() {
        return 0;
      }

      @Override
      public String getInstanceName() {
        return instanceName;
      }

      @Override
      public String getLabel() {
        return instanceName;
      }
    };
    this.userContext = new UserContext("sdk-user",
        runtimeInfo.isDPMEnabled(),
        configuration.get(
            RemoteSSOService.DPM_USER_ALIAS_NAME_ENABLED,
            RemoteSSOService.DPM_USER_ALIAS_NAME_ENABLED_DEFAULT
        )
    );
    pipelineInfo = ImmutableList.of(stageInfo);
    this.runnerId = runnerId;
    this.isPreview = isPreview;
    this.outputLanes = ImmutableList.copyOf(outputLanes);
    this.onRecordError = onRecordError;
    errorSink = new ErrorSink();
    eventSink = new EventSink();
    this.pipelineMaxMemory = new MemoryLimitConfiguration().getMemoryLimit();
    this.executionMode = executionMode;
    this.deliveryGuarantee = deliveryGuarantee;
    reportErrorDelegate = errorSink;
    this.sharedRunnerMap = new ConcurrentHashMap<>();
    this.runtimeInfo = runtimeInfo;
    this.services = services;
    this.isErrorStage = false;

    this.sourceResponseSink = new SourceResponseSink();

    // sample all records while testing
    this.startTime = System.currentTimeMillis();
    this.lineagePublisherDelegator = lineagePublisherDelegator;
  }

  public StageContext(
      String pipelineId,
      String pipelineTitle,
      String rev,
      List<Stage.Info> pipelineInfo,
      Stage.UserContext userContext,
      StageType stageType,
      int runnerId,
      boolean isPreview,
      MetricRegistry metrics,
      List<ConfigDefinition> configDefinitions,
      OnRecordError onRecordError,
      List<String> outputLanes,
      Map<String, Object> constants,
      Stage.Info stageInfo,
      long pipelineMaxMemory,
      ExecutionMode executionMode,
      DeliveryGuarantee deliveryGuarantee,
      RuntimeInfo runtimeInfo,
      EmailSender emailSender,
      Configuration configuration,
      Map<String, Object> sharedRunnerMap,
      long startTime,
      LineagePublisherDelegator lineagePublisherDelegator,
      Map<Class, ServiceRuntime> services,
      boolean isErrorStage
  ) {
    super(
      configuration,
      getConfigToElDefMap(configDefinitions),
      constants,
      emailSender,
      metrics,
      pipelineId,
      rev,
      runnerId,
      stageInfo.getInstanceName(),
      stageType,
      null,
      runtimeInfo.getResourcesDir()
    );
    this.pipelineTitle = pipelineTitle;
    this.pipelineInfo = pipelineInfo;
    this.userContext = userContext;
    this.runnerId = runnerId;
    this.isPreview = isPreview;
    this.stageInfo = stageInfo;
    this.outputLanes = ImmutableList.copyOf(outputLanes);
    this.onRecordError = onRecordError;
    this.pipelineMaxMemory = pipelineMaxMemory;
    this.executionMode = executionMode;
    this.deliveryGuarantee = deliveryGuarantee;
    this.runtimeInfo = runtimeInfo;
    this.sdcId = runtimeInfo.getId();
    this.sharedRunnerMap = sharedRunnerMap;
    this.startTime = startTime;
    this.lineagePublisherDelegator = lineagePublisherDelegator;
    this.services = services;
    this.isErrorStage = isErrorStage;
  }

  @Override
  public void finishPipeline() {
    pipelineFinisherDelegate.setFinished();
  }

  public void setPipelineFinisherDelegate(PipelineFinisherDelegate runner) {
    pipelineFinisherDelegate = runner;
  }

  PushSourceContextDelegate pushSourceContextDelegate;
  public void setPushSourceContextDelegate(PushSourceContextDelegate delegate) {
    this.pushSourceContextDelegate = delegate;
  }

  @Override
  public BatchContext startBatch() {
    return pushSourceContextDelegate.startBatch();
  }

  @Override
  public boolean processBatch(BatchContext batchContext) {
    return pushSourceContextDelegate.processBatch(batchContext, null, null);
  }

  @Override
  public boolean processBatch(BatchContext batchContext, String entityName, String entityOffset) {
    Preconditions.checkNotNull(entityName);
    return pushSourceContextDelegate.processBatch(batchContext, entityName, entityOffset);
  }

  @Override
  public void commitOffset(String entity, String offset) {
    pushSourceContextDelegate.commitOffset(entity, offset);
  }

  @Override
  public DeliveryGuarantee getDeliveryGuarantee() {
    return deliveryGuarantee;
  }

  @Override
  public Stage.Info getStageInfo() {
    return stageInfo;
  }

  @Override
  public ExecutionMode getExecutionMode() {
    return executionMode;
  }

  @Override
  public long getPipelineMaxMemory() {
    return pipelineMaxMemory;
  }

  @Override
  public boolean isPreview() {
    return isPreview;
  }

  @Override
  public Stage.UserContext getUserContext() {
    return userContext;
  }

  @Override
  public int getRunnerId() {
    return runnerId;
  }

  @Override
  public List<Stage.Info> getPipelineInfo() {
    return pipelineInfo;
  }

  // for SDK
  public ErrorSink getErrorSink() {
    return errorSink;
  }

  public void setErrorSink(ErrorSink errorSink) {
    this.errorSink = errorSink;
  }

  // for SDK
  public EventSink getEventSink() {
    return eventSink;
  }

  public void setEventSink(EventSink sink) {
    this.eventSink = sink;
  }

  // for SDK
  public ProcessedSink getProcessedSink() {
    return processedSink;
  }

  public void setProcessedSink(ProcessedSink sink) {
    processedSink = sink;
  }

  // for SDK
  public SourceResponseSink getSourceResponseSink() {
    return sourceResponseSink;
  }

  public void setSourceResponseSink(SourceResponseSink sourceResponseSink) {
    this.sourceResponseSink = sourceResponseSink;
  }

  ReportErrorDelegate reportErrorDelegate;
  public void setReportErrorDelegate(ReportErrorDelegate delegate) {
    this.reportErrorDelegate = delegate;
  }

  @Override
  @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
  public void reportError(Exception exception) {
    Preconditions.checkNotNull(exception, "exception cannot be null");
    if (exception instanceof StageException) {
      StageException stageException = (StageException)exception;
      reportErrorDelegate.reportError(stageInfo.getInstanceName(), new ErrorMessage(stageException.getErrorCode(), stageException.getParams()));
    } else {
      reportErrorDelegate.reportError(stageInfo.getInstanceName(), new ErrorMessage(ContainerError.CONTAINER_0001, exception.toString()));
    }
  }

  @Override
  public void reportError(String errorMessage) {
    Preconditions.checkNotNull(errorMessage, "errorMessage cannot be null");
    reportErrorDelegate.reportError(stageInfo.getInstanceName(), new ErrorMessage(ContainerError.CONTAINER_0002, errorMessage));
  }

  @Override
  public void reportError(ErrorCode errorCode, Object... args) {
    Preconditions.checkNotNull(errorCode, "errorId cannot be null");
    reportErrorDelegate.reportError(stageInfo.getInstanceName(), new ErrorMessage(errorCode, args));
  }

  @Override
  public OnRecordError getOnErrorRecord() {
    return onRecordError;
  }

  @Override
  @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
  public void toError(Record record, Exception ex) {
    Preconditions.checkNotNull(record, "record cannot be null");
    Preconditions.checkNotNull(ex, "exception cannot be null");
    if (ex instanceof StageException) {
      toError(record, new ErrorMessage((StageException) ex));
    } else {
      toError(record, new ErrorMessage(ContainerError.CONTAINER_0001, ex.toString(), ex));
    }
  }

  @Override
  public void toError(Record record, String errorMessage) {
    Preconditions.checkNotNull(record, "record cannot be null");
    Preconditions.checkNotNull(errorMessage, "errorMessage cannot be null");
    toError(record, new ErrorMessage(ContainerError.CONTAINER_0002, errorMessage));
  }

  @Override
  public void toError(Record record, ErrorCode errorCode, Object... args) {
    Preconditions.checkNotNull(record, "record cannot be null");
    Preconditions.checkNotNull(errorCode, "errorId cannot be null");
    // the last args needs to be Exception in order to show stack trace
    toError(record, new ErrorMessage(errorCode, args));
  }

  private void toError(Record record, ErrorMessage errorMessage) {
    String jobId = (String) getPipelineConstants().get(JOB_ID);
    RecordImpl recordImpl = ((RecordImpl) record).clone();
    if (recordImpl.isInitialRecord()) {
      recordImpl.getHeader().setSourceRecord(recordImpl);
      recordImpl.setInitialRecord(false);
    }
    recordImpl.getHeader().setError(stageInfo.getInstanceName(), stageInfo.getLabel(), errorMessage);
    if (jobId != null) {
      recordImpl.getHeader().setErrorJobId(jobId);
    }
    errorSink.addRecord(stageInfo.getInstanceName(), recordImpl);
  }

  @Override
  public List<String> getOutputLanes() {
    return outputLanes;
  }

  @Override
  public long getLastBatchTime() {
    return lastBatchTime;
  }

  @Override
  public boolean isStopped() {
    return stop;
  }

  @Override
  public boolean isErrorStage() {
    return isErrorStage;
  }

  @Override
  public EventRecord createEventRecord(String type, int version, String recordSourceId) {
    return new EventRecordImpl(type, version, stageInfo.getInstanceName(), recordSourceId, null, null);
  }

  @Override
  public LineageEvent createLineageEvent(LineageEventType type) {
    if (type.isFrameworkOnly()) {
      throw new IllegalArgumentException(Utils.format(ContainerError.CONTAINER_01401.getMessage(), type.getLabel()));
    }

    return new LineageEventImpl(
        type,
        pipelineId,
        getUserContext().getUser(),
        startTime,
        pipelineId,
        getSdcId(),
        runtimeInfo.getBaseHttpUrl() + LineageEventImpl.PARTIAL_URL + pipelineId,
        stageInfo.getInstanceName()
    );

  }
  @Override
  public void publishLineageEvent(LineageEvent event) throws IllegalArgumentException {
    List<LineageSpecificAttribute> missingOrEmpty = new ArrayList<>(event.missingSpecificAttributes());
    if (!missingOrEmpty.isEmpty()) {
      List<String> args = new ArrayList<>();
      for (LineageSpecificAttribute attrib : missingOrEmpty) {
        args.add(attrib.name());
      }
      throw new IllegalArgumentException(Utils.format(ContainerError.CONTAINER_01403.getMessage(),
          StringUtils.join(args, ", ")
      ));
    }
    lineagePublisherDelegator.publishLineageEvent(event);
  }

  @Override
  public String getSdcId() {
    return sdcId;
  }

  @Override
  public String getPipelineId() {
    return pipelineId;
  }

  @Override
  public Map<String, Object> getStageRunnerSharedMap() {
    return sharedRunnerMap;
  }

  @Override
  public <T> T getService(Class<? extends T> serviceInterface) {
    if(!services.containsKey(serviceInterface)) {
      throw new RuntimeException(Utils.format("Trying to retrieve undeclared service: {}", serviceInterface));
    }

    return (T)services.get(serviceInterface);
  }

  @Override
  public void toEvent(EventRecord record) {
    EventRecordImpl recordImpl = ((EventRecordImpl) record).clone();
    if (recordImpl.isInitialRecord()) {
      recordImpl.getHeader().setSourceRecord(recordImpl);
      recordImpl.setInitialRecord(false);
    }
    eventSink.addEvent(stageInfo.getInstanceName(), recordImpl);
  }

  @Override
  public void toSourceResponse(Record record) {
    sourceResponseSink.addResponse(record);
  }

  @Override
  public void complete(Record record) {
    processedSink.addRecord(stageInfo.getInstanceName(), record);
  }

  @Override
  public void complete(Collection<Record> records) {
    processedSink.addRecords(stageInfo.getInstanceName(), records);
  }

  public void setStop(boolean stop) {
    this.stop = stop;
  }

  public void setLastBatchTime(long lastBatchTime) {
    this.lastBatchTime = lastBatchTime;
  }

  //Processor.Context
  @Override
  public Record createRecord(Record originatorRecord) {
    Preconditions.checkNotNull(originatorRecord, "originatorRecord cannot be null");
    RecordImpl record = new RecordImpl(stageInfo.getInstanceName(), originatorRecord, null, null);
    HeaderImpl header = record.getHeader();
    header.setStagesPath("");
    return record;
  }

  //Processor.Context
  @Override
  public Record createRecord(Record originatorRecord, String sourceIdPostfix) {
    Preconditions.checkNotNull(originatorRecord, "originatorRecord cannot be null");
    RecordImpl record = new RecordImpl(stageInfo.getInstanceName(), originatorRecord, null, null);
    HeaderImpl header = record.getHeader();
    header.setSourceId(header.getSourceId() + "_" + sourceIdPostfix);
    header.setStagesPath("");
    return record;
  }

  //Processor.Context
  @Override
  public Record createRecord(Record originatorRecord, byte[] raw, String rawMime) {
    return new RecordImpl(stageInfo.getInstanceName(), originatorRecord, raw, rawMime);
  }

  //Processor.Context
  @Override
  public Record cloneRecord(Record record) {
    RecordImpl clonedRecord = ((RecordImpl) record).clone();
    HeaderImpl header = clonedRecord.getHeader();
    header.setStagesPath("");
    return clonedRecord;
  }

  //Processor.Context
  @Override
  public Record cloneRecord(Record record, String sourceIdPostfix) {
    RecordImpl clonedRecord = ((RecordImpl) record).clone();
    HeaderImpl header = clonedRecord.getHeader();
    header.setSourceId(header.getSourceId() + "_" + sourceIdPostfix);
    header.setStagesPath("");
    return clonedRecord;
  }

  @Override
  public String toString() {
    return Utils.format("StageContext[instance='{}']", stageInfo.getInstanceName());
  }
}
