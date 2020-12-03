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
import com.streamsets.datacollector.antennadoctor.AntennaDoctor;
import com.streamsets.datacollector.antennadoctor.engine.context.AntennaDoctorStageContext;
import com.streamsets.datacollector.config.ConfigDefinition;
import com.streamsets.datacollector.email.EmailSender;
import com.streamsets.datacollector.lineage.LineageEventImpl;
import com.streamsets.datacollector.lineage.LineagePublisherDelegator;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.record.EventRecordImpl;
import com.streamsets.datacollector.record.HeaderImpl;
import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.datacollector.runner.production.ReportErrorDelegate;
import com.streamsets.datacollector.usagestats.StatsCollector;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.lib.security.http.RemoteSSOService;
import com.streamsets.pipeline.api.AntennaDoctorMessage;
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
import com.streamsets.pipeline.api.SourceResponseSink;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageType;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.gateway.GatewayInfo;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.lineage.LineageEvent;
import com.streamsets.pipeline.api.lineage.LineageEventType;
import com.streamsets.pipeline.api.lineage.LineageSpecificAttribute;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StageContext extends ProtoContext implements
    Source.Context, PushSource.Context, Target.Context, Processor.Context {
  private static final String JOB_ID = "JOB_ID";
  private static final String JOB_NAME = "JOB_NAME";
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
  private final ExecutionMode executionMode;
  private final DeliveryGuarantee deliveryGuarantee;
  private final String sdcId;
  private final String pipelineTitle;
  private final String pipelineDescription;
  private final Map<String, Object> pipelineMetadata;
  private volatile boolean stop;
  private final Map<String, Object> sharedRunnerMap;
  private final long startTime;
  private final LineagePublisherDelegator lineagePublisherDelegator;
  private PipelineFinisherDelegate pipelineFinisherDelegate;
  private BuildInfo buildInfo;
  private RuntimeInfo runtimeInfo;
  private final Map services;
  private final boolean isErrorStage;
  private final RecordCloner recordCloner;

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
      resourcesDir,
      null,
      null,
      null
    );
    this.pipelineTitle = "My Pipeline";
    this.pipelineDescription = "Sample Pipeline";
    this.pipelineMetadata = new HashMap<>();
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
    this.executionMode = executionMode;
    this.deliveryGuarantee = deliveryGuarantee;
    reportErrorDelegate = errorSink;
    this.sharedRunnerMap = new ConcurrentHashMap<>();
    this.runtimeInfo = runtimeInfo;
    this.services = services;
    this.isErrorStage = false;
    this.recordCloner = new RecordCloner(false);

    this.sourceResponseSink = new SourceResponseSinkImpl();

    // sample all records while testing
    this.startTime = System.currentTimeMillis();
    this.lineagePublisherDelegator = lineagePublisherDelegator;
  }

  public StageContext(
      String pipelineId,
      String pipelineTitle,
      String pipelineDescription,
      String rev,
      Map<String, Object> metadata,
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
      ExecutionMode executionMode,
      DeliveryGuarantee deliveryGuarantee,
      BuildInfo buildInfo,
      RuntimeInfo runtimeInfo,
      EmailSender emailSender,
      Configuration configuration,
      Map<String, Object> sharedRunnerMap,
      long startTime,
      LineagePublisherDelegator lineagePublisherDelegator,
      Map<Class, ServiceRuntime> services,
      boolean isErrorStage,
      AntennaDoctor antennaDoctor,
      AntennaDoctorStageContext antennaDoctorContext,
      StatsCollector statsCollector,
      boolean recordByRef
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
      runtimeInfo.getResourcesDir(),
      antennaDoctor,
      antennaDoctorContext,
      statsCollector
    );
    this.pipelineTitle = pipelineTitle;
    this.pipelineInfo = pipelineInfo;
    this.pipelineDescription = pipelineDescription;
    this.pipelineMetadata = metadata;
    this.userContext = userContext;
    this.runnerId = runnerId;
    this.isPreview = isPreview;
    this.stageInfo = stageInfo;
    this.outputLanes = ImmutableList.copyOf(outputLanes);
    this.onRecordError = onRecordError;
    this.executionMode = executionMode;
    this.deliveryGuarantee = deliveryGuarantee;
    this.buildInfo = buildInfo;
    this.runtimeInfo = runtimeInfo;
    this.sdcId = runtimeInfo.getId();
    this.sharedRunnerMap = sharedRunnerMap;
    this.startTime = startTime;
    this.lineagePublisherDelegator = lineagePublisherDelegator;
    this.services = services;
    this.isErrorStage = isErrorStage;
    this.recordCloner = new RecordCloner(recordByRef);
  }

  @Override
  public void finishPipeline() {
    finishPipeline(false);
  }

  @Override
  public void finishPipeline(boolean resetOffset) {
    pipelineFinisherDelegate.setFinished(resetOffset);
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
  public String registerApiGateway(GatewayInfo gatewayInfo) {
    return runtimeInfo.registerApiGateway(gatewayInfo);
  }

  @Override
  public void unregisterApiGateway(GatewayInfo gatewayInfo) {
    runtimeInfo.unregisterApiGateway(gatewayInfo);
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
  public String getEnvironmentVersion() {
    return buildInfo.getVersion();
  }

  @Override
  public ExecutionMode getExecutionMode() {
    return executionMode;
  }

  @Override
  @Deprecated
  public long getPipelineMaxMemory() {
    return -1;
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
      if(statsCollector != null) {
        statsCollector.errorCode(stageException.getErrorCode());
      }
      reportErrorDelegate.reportError(stageInfo.getInstanceName(), produceErrorMessage(stageException.getErrorCode(), stageException.getParams()));
    } else {
      reportErrorDelegate.reportError(stageInfo.getInstanceName(), produceErrorMessage(exception));
    }
  }

  @Override
  public void reportError(String errorMessage) {
    Preconditions.checkNotNull(errorMessage, "errorMessage cannot be null");
    reportErrorDelegate.reportError(stageInfo.getInstanceName(), produceErrorMessage(errorMessage));
  }

  @Override
  public void reportError(ErrorCode errorCode, Object... args) {
    Preconditions.checkNotNull(errorCode, "errorId cannot be null");
    if(statsCollector != null) {
      statsCollector.errorCode(errorCode);
    }
    reportErrorDelegate.reportError(stageInfo.getInstanceName(), produceErrorMessage(errorCode, args));
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
    toError(record, new ErrorMessage(ContainerError.CONTAINER_0001, errorMessage));
  }

  @Override
  public void toError(Record record, ErrorCode errorCode, Object... args) {
    Preconditions.checkNotNull(record, "record cannot be null");
    Preconditions.checkNotNull(errorCode, "errorId cannot be null");

    if(statsCollector != null) {
      statsCollector.errorCode(errorCode);
    }

    // the last args needs to be Exception in order to show stack trace
    toError(record, new ErrorMessage(errorCode, args));
  }

  private void toError(Record record, ErrorMessage errorMessage) {
    String jobId = (String) getPipelineConstants().get(JOB_ID);
    String jobName = (String) getPipelineConstants().get(JOB_NAME);
    RecordImpl recordImpl = recordCloner.cloneRecordIfNeeded(record);
    if (recordImpl.isInitialRecord()) {
      recordImpl.getHeader().setSourceRecord(recordImpl);
      recordImpl.setInitialRecord(false);
    }
    recordImpl.getHeader().setError(stageInfo.getInstanceName(), stageInfo.getLabel(), errorMessage);
    if (jobId != null) {
      recordImpl.getHeader().setErrorJobId(jobId);
    }
    if (jobName != null) {
      recordImpl.getHeader().setErrorJobName(jobName);
    }
    errorSink.addRecord(stageInfo.getInstanceName(), recordImpl);
  }

  public ErrorMessage produceErrorMessage(Exception exception) {
    List<AntennaDoctorMessage> messages = Collections.emptyList();
    if(antennaDoctor != null) {
      messages = antennaDoctor.onStage(antennaDoctorContext, exception);
    }

    return new ErrorMessage(messages, ContainerError.CONTAINER_0001, exception.toString());
  }

  public ErrorMessage produceErrorMessage(ErrorCode errorCode, Object ...args) {
    List<AntennaDoctorMessage> messages = Collections.emptyList();
    if(antennaDoctor != null) {
      messages = antennaDoctor.onStage(antennaDoctorContext, errorCode, args);
    }

    return new ErrorMessage(messages, errorCode, args);
  }

  public ErrorMessage produceErrorMessage(String errorMessage) {
    List<AntennaDoctorMessage> messages = Collections.emptyList();
    if(antennaDoctor != null) {
      messages = antennaDoctor.onStage(antennaDoctorContext, errorMessage);
    }

    return new ErrorMessage(messages, ContainerError.CONTAINER_0001, errorMessage);
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
        runtimeInfo.getBaseHttpUrl(true) + LineageEventImpl.PARTIAL_URL + pipelineId,
        stageInfo.getInstanceName(),
        pipelineDescription,
        rev,
        pipelineMetadata,
        getPipelineConstants()
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
    EventRecordImpl recordImpl = recordCloner.cloneEventIfNeeded(record);
    if (recordImpl.isInitialRecord()) {
      recordImpl.getHeader().setSourceRecord(recordImpl);
      recordImpl.setInitialRecord(false);
    }
    eventSink.addEvent(stageInfo.getInstanceName(), recordImpl);
  }

  @Override
  public void toSourceResponse(Record record) {
    if (sourceResponseSink != null) {
      sourceResponseSink.addResponse(record);
    }
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
