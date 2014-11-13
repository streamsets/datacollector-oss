/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.runner;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.ErrorId;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.container.Utils;
import com.streamsets.pipeline.record.RecordImpl;
import com.streamsets.pipeline.util.Message;
import com.streamsets.pipeline.validation.Issue;

import java.util.List;
import java.util.Set;

public class StageContext implements Source.Context, Target.Context, Processor.Context {
  private static final String STAGE_CAUGHT_ERROR_KEY = "stage.caught.record.error";
  private static final String STAGE_CAUGHT_ERROR_DEFAULT = "Stage caught record error: {}";

  private final ClassLoader classLoader;
  private final String bundleName;
  private final List<Stage.Info> pipelineInfo;
  private final MetricRegistry metrics;
  private final String instanceName;
  private final Set<String> outputLanes;
  private ErrorRecordSink errorRecordSink;

  public StageContext(List<Stage.Info> pipelineInfo, MetricRegistry metrics, StageRuntime stageRuntime) {
    this.pipelineInfo = pipelineInfo;
    this.metrics = metrics;
    classLoader = stageRuntime.getDefinition().getStageClassLoader();
    bundleName = stageRuntime.getDefinition().getBundle();
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

  public void setErrorRecordSink(ErrorRecordSink errorRecordSink) {
    this.errorRecordSink = errorRecordSink;
  }

  @Override
  @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
  public void toError(Record record, Exception exception) {
    Preconditions.checkNotNull(record, "record cannot be null");
    Preconditions.checkNotNull(exception, "exception cannot be null");
    errorRecordSink.addRecord(instanceName, new ErrorRecord(record, null, new Message(
        STAGE_CAUGHT_ERROR_KEY, STAGE_CAUGHT_ERROR_DEFAULT, exception.getMessage())));
  }

  @Override
  public void toError(Record record, String errorMessage) {
    Preconditions.checkNotNull(record, "record cannot be null");
    Preconditions.checkNotNull(errorMessage, "errorMessage cannot be null");
    errorRecordSink.addRecord(instanceName, new ErrorRecord(record, null, new Message(
        STAGE_CAUGHT_ERROR_KEY, STAGE_CAUGHT_ERROR_DEFAULT, errorMessage)));
  }

  @Override
  public void toError(Record record, ErrorId errorId, String... args) {
    Preconditions.checkNotNull(record, "record cannot be null");
    Preconditions.checkNotNull(errorId, "errorId cannot be null");
    String bundleKey = errorId.getClass().getName() + "." + errorId.toString();
    errorRecordSink.addRecord(instanceName, new ErrorRecord(record, errorId, new Message(
        classLoader, bundleName, bundleKey, errorId.getMessageTemplate(), args)));
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
    return ((RecordImpl) record).createCopy();
  }

  @Override
  public String toString() {
    return Utils.format("StageContext[instance='{}']", instanceName);
  }

}
