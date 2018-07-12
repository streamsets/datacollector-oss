/*
 * Copyright 2018 StreamSets Inc.
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

import com.streamsets.datacollector.event.dto.PipelineStartEvent;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.BlobStore;
import com.streamsets.pipeline.api.DeliveryGuarantee;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.StageType;
import com.streamsets.pipeline.api.interceptor.InterceptorCreator;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Builder for creating interceptor creator context with all that it needs.
 */
public class InterceptorCreatorContextBuilder {

  /**
   * Actual Context implementation that will be returned when build.
   */
  private static class ContextImpl implements InterceptorCreator.Context {
    private final BlobStore blobStore;
    private final Configuration configuration;
    private final StageType stageType;
    private final InterceptorCreator.InterceptorType interceptorType;
    private final Map<String, String> parameters;
    private final ExecutionMode executionMode;
    private final DeliveryGuarantee deliveryGuarantee;

    ContextImpl(
      BlobStore blobStore,
      Configuration configuration,
      StageType stageType,
      InterceptorCreator.InterceptorType interceptorType,
      Map<String, String> parameters,
      ExecutionMode executionMode,
      DeliveryGuarantee deliveryGuarantee
    ) {
      this.blobStore = blobStore;
      this.configuration = configuration;
      this.stageType = stageType;
      this.interceptorType = interceptorType;
      this.parameters = parameters;
      this.executionMode = executionMode;
      this.deliveryGuarantee = deliveryGuarantee;
    }

    @Override
    public String getConfig(String configName) {
      return configuration.get(configName, null);
    }

    @Override
    public BlobStore getBlobStore() {
      return blobStore;
    }

    @Override
    public StageType getStageType() {
      return stageType;
    }

    @Override
    public InterceptorCreator.InterceptorType getInterceptorType() {
      return interceptorType;
    }

    @Override
    public Map<String, String> getParameters() {
      return parameters;
    }

    @Override
    public ExecutionMode getExecutionMode() {
      return executionMode;
    }

    @Override
    public DeliveryGuarantee getDeliveryGuarantee() {
      return deliveryGuarantee;
    }
  }

  private final BlobStore blobStore;
  private final Configuration sdcConf;
  private final List<PipelineStartEvent.InterceptorConfiguration> interceptorConf;
  private ExecutionMode executionMode;
  private DeliveryGuarantee deliveryGuarantee;

  public InterceptorCreatorContextBuilder(
    BlobStore blobStore,
    Configuration configuration
  ) {
    this(blobStore, configuration, Collections.emptyList());
  }

  public InterceptorCreatorContextBuilder(
    BlobStore blobStore,
    Configuration configuration,
    List<PipelineStartEvent.InterceptorConfiguration> interceptorConf
  ) {
    this.blobStore = blobStore;
    this.sdcConf = configuration;
    this.interceptorConf = interceptorConf;
  }

  public InterceptorCreatorContextBuilder withExecutionMode(ExecutionMode executionMode) {
    this.executionMode = executionMode;
    return this;
  }

  public InterceptorCreatorContextBuilder withDeliveryGuarantee(DeliveryGuarantee deliveryGuarantee) {
    this.deliveryGuarantee = deliveryGuarantee;
    return this;
  }

  public InterceptorCreator.Context buildFor(
    String stageLibrary,
    String className,
    StageType stageType,
    InterceptorCreator.InterceptorType interceptorType
  ) {
    Map<String, String> actualParameters = null;

    // See if this particular interceptor have configuration available
    for(PipelineStartEvent.InterceptorConfiguration conf : interceptorConf) {
      if(conf.getStageLibrary().equals(stageLibrary) && conf.getInterceptorClassName().equals(className)) {
        actualParameters = conf.getParameters();
        break;
      }
    }

    return new ContextImpl(
        blobStore,
        sdcConf,
        stageType,
        interceptorType,
        actualParameters,
        executionMode,
        deliveryGuarantee
    );
  }

}
