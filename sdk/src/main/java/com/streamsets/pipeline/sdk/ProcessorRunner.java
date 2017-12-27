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
package com.streamsets.pipeline.sdk;

import com.streamsets.datacollector.config.StageType;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.runner.BatchImpl;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.DeliveryGuarantee;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class ProcessorRunner extends StageRunner<Processor> {
  private static final Logger LOG = LoggerFactory.getLogger(Processor.class);

  public ProcessorRunner(
    Class<Processor> processorClass,
    Processor processor,
    Map<String, Object> configuration,
    List<String> outputLanes,
    boolean isPreview,
    OnRecordError onRecordError,
    Map<String, Object> constants,
    Map<String, String> stageSdcConf,
    ExecutionMode executionMode,
    DeliveryGuarantee deliveryGuarantee,
    String resourcesDir,
    RuntimeInfo runtimeInfo,
    List<ServiceRunner> services
  ) {
    super(
      processorClass,
      processor,
      StageType.PROCESSOR,
      configuration,
      outputLanes,
      isPreview,
      onRecordError,
      constants,
      stageSdcConf,
      executionMode,
      deliveryGuarantee,
      resourcesDir,
      runtimeInfo,
      services
    );
  }

  public ProcessorRunner(
    Class<Processor> processorClass,
    Map<String, Object> configuration,
    List<String> outputLanes,
    boolean isPreview,
    OnRecordError onRecordError,
    Map<String, Object> constants,
    Map<String, String> stageSdcConf,
    ExecutionMode executionMode,
    DeliveryGuarantee deliveryGuarantee,
    String resourcesDir,
    RuntimeInfo runtimeInfo,
    List<ServiceRunner> services
  ) {
    super(
      processorClass,
      StageType.PROCESSOR,
      configuration,
      outputLanes,
      isPreview,
      onRecordError,
      constants,
      stageSdcConf,
      executionMode,
      deliveryGuarantee,
      resourcesDir,
      runtimeInfo,
      services
    );
  }

  public Output runProcess(List<Record> inputRecords) throws StageException {
    try {
      LOG.debug("Stage '{}' process starts", getInfo().getInstanceName());
      ensureStatus(Status.INITIALIZED);
      BatchImpl batch = new BatchImpl(getInfo().getInstanceName(), "sdk", "sourceOffset", inputRecords);
      BatchMakerImpl batchMaker = new BatchMakerImpl(((Source.Context) getContext()).getOutputLanes());
      getStage().process(batch, batchMaker);
      return new Output(Source.POLL_SOURCE_OFFSET_KEY, "sdk:sourceOffset", batchMaker.getOutput());
    } finally {
      LOG.debug("Stage '{}' process ends", getInfo().getInstanceName());
    }
  }

  public static class Builder extends StageRunner.Builder<Processor, ProcessorRunner, Builder> {

    public Builder(Class<? extends Processor> processorClass,  Processor processor) {
      super((Class<Processor>)processorClass, processor);
    }

    @SuppressWarnings("unchecked")
    public Builder(Class<? extends Processor> processorClass) {
      super((Class<Processor>) processorClass);
    }

    @Override
    public ProcessorRunner build() {
      Utils.checkState(!outputLanes.isEmpty(), "A Processor must have at least one output stream");
      if (stage != null) {
        return new ProcessorRunner(
          stageClass,
          stage,
          configs,
          outputLanes,
          isPreview,
          onRecordError,
          constants,
          stageSdcConf,
          executionMode,
          deliveryGuarantee,
          resourcesDir,
          runtimeInfo,
          services
        );
      } else {
        return new ProcessorRunner(
          stageClass,
          configs,
          outputLanes,
          isPreview,
          onRecordError,
          constants,
          stageSdcConf,
          executionMode,
          deliveryGuarantee,
          resourcesDir,
          runtimeInfo,
          services
        );
      }
    }

  }

  public static BatchMaker createTestBatchMaker(String... outputLanes) {
    return StageRunner.createTestBatchMaker(outputLanes);
  }

  public static Output getOutput(BatchMaker batchMaker) {
    return StageRunner.getOutput(batchMaker);
  }

}
