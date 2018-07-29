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

import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.DeliveryGuarantee;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageType;
import com.streamsets.pipeline.api.impl.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class SourceRunner extends StageRunner<Source> {
  private static final Logger LOG = LoggerFactory.getLogger(SourceRunner.class);

  public SourceRunner(
    Class<Source> sourceClass,
    Source source,
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
      sourceClass,
      source,
      StageType.SOURCE,
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

  public SourceRunner(
    Class<Source> sourceClass,
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
      sourceClass,
      StageType.SOURCE,
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

  public Output runProduce(String lastOffset, int maxBatchSize) throws StageException {
    try {
      LOG.debug("Stage '{}' produce starts", getInfo().getInstanceName());
      ensureStatus(Status.INITIALIZED);
      BatchMakerImpl batchMaker = new BatchMakerImpl(((Source.Context) getContext()).getOutputLanes());
      String newOffset = getStage().produce(lastOffset, maxBatchSize, batchMaker);
      if (getStage() instanceof OffsetCommitter) {
        ((OffsetCommitter)getStage()).commit(newOffset);
      }
      return new Output(Source.POLL_SOURCE_OFFSET_KEY, newOffset, batchMaker.getOutput());
    } finally {
      LOG.debug("Stage '{}' produce ends", getInfo().getInstanceName());
    }
  }

  public static class Builder extends StageRunner.Builder<Source, SourceRunner, Builder> {

    public Builder(Class<? extends Source> sourceClass,  Source source) {
      super((Class<Source>)sourceClass, source);
    }

    @SuppressWarnings("unchecked")
    public Builder(Class<? extends Source> sourceClass) {
      super((Class<Source>) sourceClass);
    }

    @Override
    public SourceRunner build() {
      Utils.checkState(!outputLanes.isEmpty(), "A Source must have at least one output stream");
      if (stage != null) {
        return new SourceRunner(
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
        return new SourceRunner(
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
