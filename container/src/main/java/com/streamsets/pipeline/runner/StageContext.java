/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.api.ext.RecordReader;
import com.streamsets.pipeline.api.ext.RecordWriter;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.ConfigDefinition;
import com.streamsets.pipeline.config.MemoryLimitConfiguration;
import com.streamsets.pipeline.config.StageType;
import com.streamsets.pipeline.el.ELEvaluator;
import com.streamsets.pipeline.el.ELVariables;
import com.streamsets.pipeline.metrics.MetricsConfigurator;
import com.streamsets.pipeline.record.RecordImpl;
import com.streamsets.pipeline.record.io.RecordWriterReaderFactory;
import com.streamsets.pipeline.util.ContainerError;
import com.streamsets.pipeline.util.ElUtil;
import com.streamsets.pipeline.validation.StageIssue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StageContext implements Source.Context, Target.Context, Processor.Context, ContextExtensions {

  private static final String CUSTOM_METRICS_PREFIX = "custom.";

  private final List<Stage.Info> pipelineInfo;
  private final StageType stageType;
  private final boolean isPreview;
  private final MetricRegistry metrics;
  private final String instanceName;
  private final List<String> outputLanes;
  private final OnRecordError onRecordError;
  private ErrorSink errorSink;
  private long lastBatchTime;
  private final Map<String, Class<?>[]> configToElDefMap;
  private final Map<String, Object> constants;
  private final long pipelineMaxMemory;
  private final boolean isClusterMode;
  private final String resourcesDir;

  //for SDK
  public StageContext(String instanceName, StageType stageType, boolean isPreview, OnRecordError onRecordError,
      List<String> outputLanes, Map<String, Class<?>[]> configToElDefMap, Map<String, Object> constants,
      boolean isClusterMode, String resourcesDir) {
    pipelineInfo = ImmutableList.of();
    this.stageType = stageType;
    this.isPreview = isPreview;
    metrics = new MetricRegistry();
    this.instanceName = instanceName;
    this.outputLanes = ImmutableList.copyOf(outputLanes);
    this.onRecordError = onRecordError;
    errorSink = new ErrorSink();
    this. configToElDefMap = configToElDefMap;
    this.constants = constants;
    this.pipelineMaxMemory = new MemoryLimitConfiguration().getMemoryLimit();
    this.isClusterMode = isClusterMode;
    this.resourcesDir = resourcesDir;
  }

  public StageContext(List<Stage.Info> pipelineInfo, StageType stageType, boolean isPreview, MetricRegistry metrics,
      StageRuntime stageRuntime, long pipelineMaxMemory, boolean isClusterMode, String resourcesDir) {
    this.pipelineInfo = pipelineInfo;
    this.stageType = stageType;
    this.isPreview = isPreview;
    this.metrics = metrics;
    this.instanceName = stageRuntime.getConfiguration().getInstanceName();
    this.outputLanes = ImmutableList.copyOf(stageRuntime.getConfiguration().getOutputLanes());
    onRecordError = stageRuntime.getOnRecordError();
    this.configToElDefMap = getConfigToElDefMap(stageRuntime);
    this.constants = stageRuntime.getConstants();
    this.pipelineMaxMemory = pipelineMaxMemory;
    this.isClusterMode = isClusterMode;
    this.resourcesDir = resourcesDir;
  }

  private Map<String, Class<?>[]> getConfigToElDefMap(StageRuntime stageRuntime) {
    Map<String, Class<?>[]> configToElDefMap = new HashMap<>();
    for(ConfigDefinition configDefinition : stageRuntime.getDefinition().getConfigDefinitions()) {
      configToElDefMap.put(configDefinition.getFieldName(),
        ElUtil.getElDefClassArray(stageRuntime.getDefinition().getStageClassLoader(), configDefinition.getElDefs()));
      if(configDefinition.getModel() != null && configDefinition.getModel().getConfigDefinitions() != null) {
        for(ConfigDefinition configDef : configDefinition.getModel().getConfigDefinitions()) {
          configToElDefMap.put(configDef.getFieldName(),
            ElUtil.getElDefClassArray(stageRuntime.getDefinition().getStageClassLoader(), configDef.getElDefs()));
        }
      }
    }
    return configToElDefMap;

  }

  private static class ConfigIssueImpl extends StageIssue implements Stage.ConfigIssue {

    public ConfigIssueImpl(String instanceName, String configGroup, String configName, ErrorCode errorCode,
        Object... args) {
      super(false, instanceName, configGroup, configName, errorCode, args); //TODO we should use errorStage bit here
    }

  }

  private static final Object[] NULL_ONE_ARG = {null};

  @Override
  public Stage.ConfigIssue createConfigIssue(String configGroup, String configName, ErrorCode errorCode,
      Object... args) {
    Preconditions.checkNotNull(errorCode, "errorCode cannot be null");
    args = (args != null) ? args.clone() : NULL_ONE_ARG;
    return new ConfigIssueImpl(instanceName, configGroup, configName, errorCode, args);
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

  @Override
  public ExecutionMode getExecutionMode() {
    return ExecutionMode.STANDALONE;
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
  public List<Stage.Info> getPipelineInfo() {
    return pipelineInfo;
  }

  @Override
  public MetricRegistry getMetrics() {
    return metrics;
  }

  @Override
  public Timer createTimer(String name) {
    return MetricsConfigurator.createTimer(getMetrics(), CUSTOM_METRICS_PREFIX + instanceName + "." + name);
  }

  @Override
  public Meter createMeter(String name) {
    return MetricsConfigurator.createMeter(getMetrics(), CUSTOM_METRICS_PREFIX + instanceName + "." + name);
  }

  @Override
  public Counter createCounter(String name) {
    return MetricsConfigurator.createCounter(getMetrics(), CUSTOM_METRICS_PREFIX +instanceName + "." + name);
  }

  // for SDK
  public ErrorSink getErrorSink() {
    return errorSink;
  }

  public void setErrorSink(ErrorSink errorSink) {
    this.errorSink = errorSink;
  }

  @Override
  @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
  public void reportError(Exception exception) {
    Preconditions.checkNotNull(exception, "exception cannot be null");
    errorSink.addError(instanceName, new ErrorMessage(ContainerError.CONTAINER_0001, exception.getMessage()));
  }

  @Override
  public void reportError(String errorMessage) {
    Preconditions.checkNotNull(errorMessage, "errorMessage cannot be null");
    errorSink.addError(instanceName, new ErrorMessage(ContainerError.CONTAINER_0002, errorMessage));
  }

  @Override
  public void reportError(ErrorCode errorCode, Object... args) {
    Preconditions.checkNotNull(errorCode, "errorId cannot be null");
    errorSink.addError(instanceName, new ErrorMessage(errorCode, args));
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
      toError(record, new ErrorMessage(ContainerError.CONTAINER_0001, ex.getMessage()));
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
    toError(record, new ErrorMessage(errorCode, args));
  }

  private void toError(Record record, ErrorMessage errorMessage) {
    RecordImpl recordImpl = ((RecordImpl) record).clone();
    if (stageType == StageType.SOURCE) {
      recordImpl.getHeader().setSourceRecord(recordImpl);
    }
    recordImpl.getHeader().setError(instanceName, errorMessage);
    errorSink.addRecord(instanceName, recordImpl);
  }

  @Override
  public List<String> getOutputLanes() {
    return outputLanes;
  }

  //Stage.Context
  @Override
  public Record createRecord(String recordSourceId) {
    return new RecordImpl(instanceName, recordSourceId, null, null);
  }

  //Stage.Context
  @Override
  public Record createRecord(String recordSourceId, byte[] raw, String rawMime) {
    return new RecordImpl(instanceName, recordSourceId, raw, rawMime);
  }

  @Override
  public long getLastBatchTime() {
    return lastBatchTime;
  }

  @Override
  public String getResourcesDirectory() {
    return resourcesDir;
  }

  public void setLastBatchTime(long lastBatchTime) {
    this.lastBatchTime = lastBatchTime;
  }

  //Processor.Context
  @Override
  public Record createRecord(Record originatorRecord) {
    Preconditions.checkNotNull(originatorRecord, "originatorRecord cannot be null");
    return new RecordImpl(instanceName, originatorRecord, null, null);
  }

  //Processor.Context
  @Override
  public Record createRecord(Record originatorRecord, byte[] raw, String rawMime) {
    return new RecordImpl(instanceName, originatorRecord, raw, rawMime);
  }

  //Processor.Context
  @Override
  public Record cloneRecord(Record record) {
    return ((RecordImpl) record).clone();
  }

  @Override
  public String toString() {
    return Utils.format("StageContext[instance='{}']", instanceName);
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
    return new ELEvaluator(configName, constants, configToElDefMap.get(configName));
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
    return new ELEvaluator(configName, constants, classes.toArray(new Class[classes.size()]));
  }

  @Override
  public boolean isClusterMode() {
    return isClusterMode;
  }
}
