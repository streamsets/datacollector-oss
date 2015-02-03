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
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.util.ContainerError;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.metrics.MetricsConfigurator;
import com.streamsets.pipeline.record.RecordImpl;
import com.streamsets.pipeline.validation.StageIssue;

import java.util.List;

public class StageContext implements Source.Context, Target.Context, Processor.Context {

  private static final String CUSTOM_METRICS_PREFIX = "custom.";

  private final List<Stage.Info> pipelineInfo;
  private final MetricRegistry metrics;
  private final String instanceName;
  private final List<String> outputLanes;
  private ErrorSink errorSink;

  //for SDK
  public StageContext(String instanceName, List<String> outputLanes) {
    pipelineInfo = ImmutableList.of();
    metrics = new MetricRegistry();
    this.instanceName = instanceName;
    this.outputLanes = ImmutableList.copyOf(outputLanes);
    errorSink = new ErrorSink();
  }

  public StageContext(List<Stage.Info> pipelineInfo, MetricRegistry metrics, StageRuntime stageRuntime) {
    this.pipelineInfo = pipelineInfo;
    this.metrics = metrics;
    this.instanceName = stageRuntime.getConfiguration().getInstanceName();
    this.outputLanes = ImmutableList.copyOf(stageRuntime.getConfiguration().getOutputLanes());

  }

  private static class ConfigIssueImpl extends StageIssue implements Stage.ConfigIssue {

    public ConfigIssueImpl(String instanceName, ErrorCode errorCode, Object... args) {
      super(instanceName, errorCode, args);
    }

  }

  private static final Object[] NULL_ONE_ARG = {null};

  @Override
  public Stage.ConfigIssue createConfigIssue(ErrorCode errorCode, Object... args) {
    Preconditions.checkNotNull(errorCode, "errorCode cannot be null");
    args = (args != null) ? args.clone() : NULL_ONE_ARG;
    return new ConfigIssueImpl(instanceName, errorCode, args);
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
  @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
  public void toError(Record record, Exception ex) {
    Preconditions.checkNotNull(record, "record cannot be null");
    Preconditions.checkNotNull(ex, "exception cannot be null");
    RecordImpl recordImpl = ((RecordImpl) record).clone();
    recordImpl.getHeader().setError(instanceName, new ErrorMessage(ContainerError.CONTAINER_0001, instanceName,
                                                                   ex.getMessage()));
    errorSink.addRecord(instanceName, recordImpl);
  }

  @Override
  public void toError(Record record, String errorMessage) {
    Preconditions.checkNotNull(record, "record cannot be null");
    Preconditions.checkNotNull(errorMessage, "errorMessage cannot be null");
    RecordImpl recordImpl = ((RecordImpl) record).clone();
    recordImpl.getHeader().setError(instanceName, new ErrorMessage(ContainerError.CONTAINER_0002, errorMessage));
    errorSink.addRecord(instanceName, recordImpl);
  }

  @Override
  public void toError(Record record, ErrorCode errorCode, Object... args) {
    Preconditions.checkNotNull(record, "record cannot be null");
    Preconditions.checkNotNull(errorCode, "errorId cannot be null");
    RecordImpl recordImpl = ((RecordImpl) record).clone();
    recordImpl.getHeader().setError(instanceName, new ErrorMessage(errorCode, args));
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
