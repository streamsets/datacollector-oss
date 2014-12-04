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
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.util.ContainerError;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.container.ErrorMessage;
import com.streamsets.pipeline.container.Utils;
import com.streamsets.pipeline.metrics.MetricsConfigurator;
import com.streamsets.pipeline.record.RecordImpl;

import java.util.List;
import java.util.Set;

public class StageContext implements Source.Context, Target.Context, Processor.Context {

  private static final String CUSTOM_METRICS_PREFIX = "custom.";

  private final List<Stage.Info> pipelineInfo;
  private final MetricRegistry metrics;
  private final String instanceName;
  private final Set<String> outputLanes;
  private ErrorSink errorSink;

  //for SDK
  public StageContext(String instanceName, Set<String> outputLanes) {
    pipelineInfo = ImmutableList.of();
    metrics = new MetricRegistry();
    this.instanceName = instanceName;
    this.outputLanes = ImmutableSet.copyOf(outputLanes);
    errorSink = new ErrorSink();
  }

  public StageContext(List<Stage.Info> pipelineInfo, MetricRegistry metrics, StageRuntime stageRuntime) {
    this.pipelineInfo = pipelineInfo;
    this.metrics = metrics;
    this.instanceName = stageRuntime.getConfiguration().getInstanceName();
    this.outputLanes = ImmutableSet.copyOf(stageRuntime.getConfiguration().getOutputLanes());

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

  public void setErrorSink(ErrorSink errorSink) {
    this.errorSink = errorSink;
  }

  @Override
  public void reportError(Exception exception) {
    Preconditions.checkNotNull(exception, "exception cannot be null");
    errorSink.addError(new ErrorMessage(ContainerError.CONTAINER_0001, instanceName, exception.getMessage()));
  }

  @Override
  public void reportError(String errorMessage) {
    Preconditions.checkNotNull(errorMessage, "errorMessage cannot be null");
    errorSink.addError(new ErrorMessage(ContainerError.CONTAINER_0002, instanceName, errorMessage));
  }

  @Override
  public void reportError(ErrorCode errorCode, String... args) {
    Preconditions.checkNotNull(errorCode, "errorId cannot be null");
    errorSink.addError(new ErrorMessage(errorCode, args));
  }

  @Override
  @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
  public void toError(Record record, Exception ex) {
    Preconditions.checkNotNull(record, "record cannot be null");
    Preconditions.checkNotNull(ex, "exception cannot be null");
    ((RecordImpl)record).getHeader().setErrorId(ContainerError.CONTAINER_0001.getCode());
    ((RecordImpl)record).getHeader().setErrorMessage(new ErrorMessage(ContainerError.CONTAINER_0001, ex.getMessage()));
    errorSink.addRecord(instanceName, record);
  }

  @Override
  public void toError(Record record, String errorMessage) {
    Preconditions.checkNotNull(record, "record cannot be null");
    Preconditions.checkNotNull(errorMessage, "errorMessage cannot be null");
    ((RecordImpl)record).getHeader().setErrorId(ContainerError.CONTAINER_0002.getCode());
    ((RecordImpl)record).getHeader().setErrorMessage(new ErrorMessage(ContainerError.CONTAINER_0002, errorMessage));
    errorSink.addRecord(instanceName, record);
  }

  @Override
  public void toError(Record record, ErrorCode errorCode, String... args) {
    Preconditions.checkNotNull(record, "record cannot be null");
    Preconditions.checkNotNull(errorCode, "errorId cannot be null");
    ((RecordImpl) record).getHeader().setErrorId(errorCode.toString());
    ((RecordImpl) record).getHeader().setErrorMessage(new ErrorMessage(errorCode, args));
    errorSink.addRecord(instanceName, record);
  }

  @Override
  public Set<String> getOutputLanes() {
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

}
