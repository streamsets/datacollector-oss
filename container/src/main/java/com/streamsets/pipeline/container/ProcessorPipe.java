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
package com.streamsets.pipeline.container;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.Stage.Info;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Processor.Context;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.record.RecordImpl;

import java.util.List;
import java.util.Set;

public class ProcessorPipe extends Pipe implements Context {

  private Processor processor;

  public ProcessorPipe(List<Info> pipelineInfo, MetricRegistry metrics, Stage.Info moduleInfo, Processor processor,
      Set<String> inputLanes, Set<String> outputLanes) {
    super(pipelineInfo, metrics, moduleInfo, inputLanes, outputLanes);
    Preconditions.checkNotNull(processor, "processor cannot be null");
    this.processor = processor;
  }

  @Override
  public void init() {

    try {
      processor.init(getModuleInfo(), this);
    } catch (StageException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void destroy() {
    processor.destroy();
  }

  @Override
  public void configure(Configuration conf) {
    super.configure(conf);
    //TODO
  }

  @Override
  protected void processBatch(PipeBatch batch) {
    Preconditions.checkNotNull(batch, "batch cannot be null");
    try {
      processor.process(batch, batch);
    } catch (StageException e) {
      e.printStackTrace();
    }
    //LOG warning if !batch.isInputFullyConsumed()
  }

  // Processor.Context

  @Override
  public Record createRecord(String sourceInfo) {
    return new RecordImpl(getModuleInfo().getName(), sourceInfo, null, null);
  }

  @Override
  public Record cloneRecord(Record record) {
    return new RecordImpl("(cloned)", (RecordImpl)record);
  }

  public Processor getProcessor() {
    return processor;
  }
}
