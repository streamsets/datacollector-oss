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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.config.ConfigDefinition;
import com.streamsets.datacollector.config.MemoryLimitConfiguration;
import com.streamsets.datacollector.config.StageType;
import com.streamsets.datacollector.el.ELEvaluator;
import com.streamsets.datacollector.el.ELVariables;
import com.streamsets.datacollector.email.EmailException;
import com.streamsets.datacollector.email.EmailSender;
import com.streamsets.datacollector.lineage.LineageEventImpl;
import com.streamsets.datacollector.lineage.LineagePublisherDelegator;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.metrics.MetricsConfigurator;
import com.streamsets.datacollector.record.EventRecordImpl;
import com.streamsets.datacollector.record.HeaderImpl;
import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.datacollector.record.io.JsonWriterReaderFactory;
import com.streamsets.datacollector.record.io.RecordWriterReaderFactory;
import com.streamsets.datacollector.runner.production.ReportErrorDelegate;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.ElUtil;
import com.streamsets.datacollector.validation.Issue;
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
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.api.ext.JsonObjectReader;
import com.streamsets.pipeline.api.ext.JsonRecordWriter;
import com.streamsets.pipeline.api.ext.RecordReader;
import com.streamsets.pipeline.api.ext.RecordWriter;
import com.streamsets.pipeline.api.ext.Sampler;
import com.streamsets.pipeline.api.ext.json.Mode;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.lineage.LineageEvent;
import com.streamsets.pipeline.api.lineage.LineageEventType;
import com.streamsets.pipeline.api.lineage.LineageSpecificAttribute;
import com.streamsets.pipeline.lib.sampling.RecordSampler;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StageContext implements Source.Context, PushSource.Context, Target.Context, Processor.Context, ContextExtensions {

  private static final Logger LOG = LoggerFactory.getLogger(StageContext.class);
  private static final String STAGE_CONF_PREFIX = "stage.conf_";
  private static final String CUSTOM_METRICS_PREFIX = "custom.";
  private static final String SDC_RECORD_SAMPLING_POPULATION_SIZE = "sdc.record.sampling.population.size";
  private static final String SDC_RECORD_SAMPLING_SAMPLE_SIZE = "sdc.record.sampling.sample.size";

  private final Configuration configuration;
  private final int runnerId;
  private final List<Stage.Info> pipelineInfo;
  private final Stage.UserContext userContext;
  private final StageType stageType;
  private final boolean isPreview;
  private final MetricRegistry metrics;
  private final Stage.Info stageInfo;
  private final List<String> outputLanes;
  private final OnRecordError onRecordError;
  private ErrorSink errorSink;
  private EventSink eventSink;
  private long lastBatchTime;
  private final Map<String, Class<?>[]> configToElDefMap;
  private final Map<String, Object> constants;
  private final long pipelineMaxMemory;
  private final ExecutionMode executionMode;
  private final DeliveryGuarantee deliveryGuarantee;
  private final String resourcesDir;
  private final String sdcId;
  private final String pipelineId;
  private final String pipelineTitle;
  private final String rev;
  private volatile boolean stop;
  private final EmailSender emailSender;
  private final Sampler sampler;
  private final Map<String, Object> sharedRunnerMap;
  private final long startTime;
  private final LineagePublisherDelegator lineagePublisherDelegator;
  private PipelineFinisherDelegate pipelineFinisherDelegate;
  private RuntimeInfo runtimeInfo;
  private final Map<Class, ServiceRuntime> services;

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
      RuntimeInfo runtimeInfo
  ) {
    this.pipelineId = "myPipeline";
    this.pipelineTitle = "My Pipeline";
    this.sdcId = "mySDC";
    this.rev = "0";
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
    this.userContext = new UserContext("sdk-user");
    pipelineInfo = ImmutableList.of(stageInfo);
    this.stageType = stageType;
    this.runnerId = runnerId;
    this.isPreview = isPreview;
    metrics = new MetricRegistry();
    this.outputLanes = ImmutableList.copyOf(outputLanes);
    this.onRecordError = onRecordError;
    errorSink = new ErrorSink();
    eventSink = new EventSink();
    this. configToElDefMap = configToElDefMap;
    this.constants = Collections.unmodifiableMap(constants);
    this.pipelineMaxMemory = new MemoryLimitConfiguration().getMemoryLimit();
    this.executionMode = executionMode;
    this.deliveryGuarantee = deliveryGuarantee;
    this.resourcesDir = resourcesDir;
    this.emailSender = emailSender;
    reportErrorDelegate = errorSink;
    this.sharedRunnerMap = new ConcurrentHashMap<>();
    this.runtimeInfo = runtimeInfo;

    // sample all records while testing
    this.configuration = configuration.getSubSetConfiguration(STAGE_CONF_PREFIX);
    this.sampler = new RecordSampler(this, stageType == StageType.SOURCE, 0, 0);
    this.startTime = System.currentTimeMillis();
    this.lineagePublisherDelegator = lineagePublisherDelegator;

    // Services are currently not supported in SDK
    this.services = Collections.emptyMap();
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
      StageRuntime stageRuntime,
      long pipelineMaxMemory,
      ExecutionMode executionMode,
      DeliveryGuarantee deliveryGuarantee,
      RuntimeInfo runtimeInfo,
      EmailSender emailSender,
      Configuration configuration,
      Map<String, Object> sharedRunnerMap,
      long startTime,
      LineagePublisherDelegator lineagePublisherDelegator,
      Map<Class, ServiceRuntime> services
  ) {
    this.pipelineId = pipelineId;
    this.pipelineTitle = pipelineTitle;
    this.rev = rev;
    this.pipelineInfo = pipelineInfo;
    this.userContext = userContext;
    this.stageType = stageType;
    this.runnerId = runnerId;
    this.isPreview = isPreview;
    this.metrics = metrics;
    this.stageInfo = stageRuntime.getInfo();
    this.outputLanes = ImmutableList.copyOf(stageRuntime.getConfiguration().getOutputLanes());
    onRecordError = stageRuntime.getOnRecordError();
    this.configToElDefMap = getConfigToElDefMap(stageRuntime);
    this.constants = stageRuntime.getConstants();
    this.pipelineMaxMemory = pipelineMaxMemory;
    this.executionMode = executionMode;
    this.deliveryGuarantee = deliveryGuarantee;
    this.runtimeInfo = runtimeInfo;
    this.resourcesDir = runtimeInfo.getResourcesDir();
    this.sdcId = runtimeInfo.getId();
    this.emailSender = emailSender;
    this.configuration = configuration.getSubSetConfiguration(STAGE_CONF_PREFIX);
    int sampleSize = configuration.get(SDC_RECORD_SAMPLING_SAMPLE_SIZE, 1);
    int populationSize = configuration.get(SDC_RECORD_SAMPLING_POPULATION_SIZE, 10000);
    this.sampler = new RecordSampler(this, stageType == StageType.SOURCE, sampleSize, populationSize);
    this.sharedRunnerMap = sharedRunnerMap;
    this.startTime = startTime;
    this.lineagePublisherDelegator = lineagePublisherDelegator;
    this.services = services;
  }

  private Map<String, Class<?>[]> getConfigToElDefMap(StageRuntime stageRuntime) {
    Map<String, Class<?>[]> configToElDefMap = new HashMap<>();
    for(ConfigDefinition configDefinition : stageRuntime.getDefinition().getConfigDefinitions()) {
      configToElDefMap.put(configDefinition.getFieldName(),
        ElUtil.getElDefClassArray(configDefinition.getElDefs()));
      if(configDefinition.getModel() != null && configDefinition.getModel().getConfigDefinitions() != null) {
        for(ConfigDefinition configDef : configDefinition.getModel().getConfigDefinitions()) {
          configToElDefMap.put(configDef.getFieldName(),
            ElUtil.getElDefClassArray(configDef.getElDefs()));
        }
      }
    }
    return configToElDefMap;

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

  private static class ConfigIssueImpl extends Issue implements Stage.ConfigIssue {

    public ConfigIssueImpl(String instanceName, String configGroup, String configName, ErrorCode errorCode,
        Object... args) {
      super(instanceName, null, configGroup, configName, errorCode, args);
    }

  }

  private static final Object[] NULL_ONE_ARG = {null};

  @Override
  public Stage.ConfigIssue createConfigIssue(String configGroup, String configName, ErrorCode errorCode,
      Object... args) {
    Preconditions.checkNotNull(errorCode, "errorCode cannot be null");
    args = (args != null) ? args.clone() : NULL_ONE_ARG;
    return new ConfigIssueImpl(stageInfo.getInstanceName(), configGroup, configName, errorCode, args);
  }

  @Override
  public Stage.Info getStageInfo() {
    return stageInfo;
  }

  @Override
  public RecordReader createRecordReader(InputStream inputStream, long initialPosition, int maxObjectLen)
      throws IOException {
    return RecordWriterReaderFactory.createRecordReader(inputStream, initialPosition, maxObjectLen);
  }

  @Override
  public RecordWriter createRecordWriter(OutputStream outputStream) throws IOException {
    return RecordWriterReaderFactory.createRecordWriter(this, outputStream);
  }

  /* JSON Methods */
  @Override
  public JsonObjectReader createJsonObjectReader(
      Reader reader, long initialPosition, Mode mode, Class<?> objectClass
  ) throws IOException {
    return JsonWriterReaderFactory.createObjectReader(reader, initialPosition, mode, objectClass);
  }

  @Override
  public JsonObjectReader createJsonObjectReader(
      Reader reader, long initialPosition, int maxObjectLen, Mode mode, Class<?> objectClass
  ) throws IOException {
    return JsonWriterReaderFactory.createObjectReader(reader, initialPosition, mode, objectClass, maxObjectLen);
  }

  @Override
  public JsonRecordWriter createJsonRecordWriter(
      Writer writer, Mode mode
  ) throws IOException {
    return JsonWriterReaderFactory.createRecordWriter(writer, mode);
  }

  /* End JSON Methods */

  @Override
  public void notify(List<String> addresses, String subject, String body) throws StageException {
    try {
      emailSender.send(addresses, subject, body);
    } catch (EmailException e) {
      LOG.error(Utils.format(ContainerError.CONTAINER_01001.getMessage(), e.toString(), e));
      throw new StageException(ContainerError.CONTAINER_01001, e.toString(), e);
    }
  }

  @Override
  public Sampler getSampler() {
    return sampler;
  }

  @Override
  public String getConfig(String configName) {
    return configuration.get(STAGE_CONF_PREFIX + configName, null);
  }

  @Override
  public Map<String, Object> getPipelineConstants() {
    return constants;
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

  @Override
  public MetricRegistry getMetrics() {
    return metrics;
  }

  @Override
  public Timer createTimer(String name) {
    return MetricsConfigurator.createStageTimer(getMetrics(), CUSTOM_METRICS_PREFIX + stageInfo.getInstanceName() + "." + name, pipelineId,
      rev);
  }

  public Timer getTimer(String name) {
    return MetricsConfigurator.getTimer(getMetrics(), CUSTOM_METRICS_PREFIX + stageInfo.getInstanceName() + "." + name);
  }

  @Override
  public Meter createMeter(String name) {
    return MetricsConfigurator.createStageMeter(getMetrics(), CUSTOM_METRICS_PREFIX + stageInfo.getInstanceName() + "." + name, pipelineId,
      rev);
  }

  public Meter getMeter(String name) {
    return MetricsConfigurator.getMeter(getMetrics(), CUSTOM_METRICS_PREFIX + stageInfo.getInstanceName() + "." + name);
  }

  @Override
  public Counter createCounter(String name) {
    return MetricsConfigurator.createStageCounter(getMetrics(), CUSTOM_METRICS_PREFIX +stageInfo.getInstanceName() + "." + name, pipelineId,
      rev);
  }

  public Counter getCounter(String name) {
    return MetricsConfigurator.getCounter(getMetrics(), CUSTOM_METRICS_PREFIX + stageInfo.getInstanceName() + "." + name);
  }

  @Override
  public Histogram createHistogram(String name) {
    return MetricsConfigurator.createStageHistogram5Min(getMetrics(), CUSTOM_METRICS_PREFIX +stageInfo.getInstanceName() + "." + name, pipelineId, rev);
  }

  @Override
  public Histogram getHistogram(String name) {
    return MetricsConfigurator.getHistogram(getMetrics(), CUSTOM_METRICS_PREFIX + stageInfo.getInstanceName() + "." + name);
  }

  @Override
  public Gauge<Map<String, Object>> createGauge(String name) {
    return MetricsConfigurator.createStageGauge(getMetrics(), CUSTOM_METRICS_PREFIX +stageInfo.getInstanceName() + "." + name, null, pipelineId, rev);
  }

  @Override
  public Gauge<Map<String, Object>> createGauge(String name, Comparator<String> comparator) {
    return MetricsConfigurator.createStageGauge(getMetrics(), CUSTOM_METRICS_PREFIX +stageInfo.getInstanceName() + "." + name, comparator, pipelineId, rev);
  }

  @Override
  public Gauge<Map<String, Object>> getGauge(String name) {
    return MetricsConfigurator.getGauge(getMetrics(), CUSTOM_METRICS_PREFIX +stageInfo.getInstanceName() + "." + name);
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
    RecordImpl recordImpl = ((RecordImpl) record).clone();
    if (recordImpl.isInitialRecord()) {
      recordImpl.getHeader().setSourceRecord(recordImpl);
      recordImpl.setInitialRecord(false);
    }
    recordImpl.getHeader().setError(stageInfo.getInstanceName(), stageInfo.getLabel(), errorMessage);
    errorSink.addRecord(stageInfo.getInstanceName(), recordImpl);
  }

  @Override
  public List<String> getOutputLanes() {
    return outputLanes;
  }

  //Stage.Context
  @Override
  public Record createRecord(String recordSourceId) {
    return new RecordImpl(stageInfo.getInstanceName(), recordSourceId, null, null);
  }

  //Stage.Context
  @Override
  public Record createRecord(String recordSourceId, byte[] raw, String rawMime) {
    return new RecordImpl(stageInfo.getInstanceName(), recordSourceId, raw, rawMime);
  }

  @Override
  public long getLastBatchTime() {
    return lastBatchTime;
  }

  @Override
  public String getResourcesDirectory() {
    return resourcesDir;
  }

  @Override
  public boolean isStopped() {
    return stop;
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

  //ElProvider interface implementation

  @Override
  public void parseEL(String el) throws ELEvalException {
    ELEvaluator.parseEL(el);
  }

  @Override
  public ELVars createELVars() {
    return new ELVariables(constants);
  }

  @Override
  public ELEval createELEval(String configName) {
    return createELEval(configName, configToElDefMap.get(configName));
  }

  @Override
  public ELEval createELEval(String configName, Class<?>... elDefClasses) {
    List<Class> classes = new ArrayList<>();
    Class[] configClasses = configToElDefMap.get(configName);
    if (configClasses != null) {
      Collections.addAll(classes, configClasses);
    }
    if (elDefClasses != null) {
      Collections.addAll(classes, elDefClasses);
    }
    // assert non of the EL functions is implicit only
    return new ELEvaluator(configName, true, constants, classes.toArray(new Class[classes.size()]));
  }

}
