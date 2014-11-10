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
package com.streamsets.pipeline.sdk.testharness.internal;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.record.RecordImpl;

import java.util.List;
import java.util.Set;

public class ProcessorContextImpl implements Processor.Context {

  private final String instanceName;
  private final Set<String> outputLanes;

  public ProcessorContextImpl(String instanceName, Set<String> outputLanes) {
    this.instanceName = instanceName;
    this.outputLanes = outputLanes;
  }

  @Override
  public Set<String> getOutputLanes() {
    return outputLanes;
  }

  @Override
  public Record createRecord(Record originatorRecord) {
    Preconditions.checkNotNull(originatorRecord, "originatorRecord cannot be null");
    return new RecordImpl(instanceName, originatorRecord.getHeader().getSourceId(), null, null);
  }

  @Override
  public Record createRecord(Record originatorRecord, byte[] raw, String rawMime) {
    Preconditions.checkNotNull(originatorRecord, "originatorRecord cannot be null");
    return new RecordImpl(instanceName, originatorRecord.getHeader().getSourceId(), raw, rawMime);
  }

  @Override
  public Record cloneRecord(Record record) {
    return ((RecordImpl)record).createCopy();
  }

  @Override
  public List<Stage.Info> getPipelineInfo() {
    return ImmutableList.of();
  }

  @Override
  public MetricRegistry getMetrics() {
    return null;
  }

  @Override
  public void toError(Record record, Exception exception) {

  }

  @Override
  public void toError(Record record, String errorMessage) {

  }

}
