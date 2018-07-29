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
package com.streamsets.datacollector.execution;

import com.streamsets.datacollector.event.dto.PipelineStartEvent;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * All information that the framework needs to pass around when starting a new pipeline.
 */
public class StartPipelineContextBuilder {

  /**
   * Actual context implementation that will be returned.
   */
  private static class StartPipelineContextImpl implements Runner.StartPipelineContext {
    private final String user;
    private final Map<String, Object> runtimeParameters;
    private final List<PipelineStartEvent.InterceptorConfiguration> interceptorConfigurations;

    StartPipelineContextImpl(
      String user,
      Map<String, Object> runtimeParameters,
      List<PipelineStartEvent.InterceptorConfiguration> interceptorConfigurations
    ) {
      this.user = user;
      this.runtimeParameters = runtimeParameters;
      this.interceptorConfigurations = interceptorConfigurations;
    }

    @Override
    public String getUser() {
      return user;
    }

    @Override
    public Map<String, Object> getRuntimeParameters() {
      return runtimeParameters;
    }

    @Override
    public List<PipelineStartEvent.InterceptorConfiguration> getInterceptorConfigurations() {
      return interceptorConfigurations;
    }
  }

  private final String user;
  private Map<String, Object> runtimeParameters = null;
  private List<PipelineStartEvent.InterceptorConfiguration> interceptorConfigurations = Collections.emptyList();

  public StartPipelineContextBuilder(String name) {
    this.user = name;
  }

  public StartPipelineContextBuilder withRuntimeParameters(Map<String, Object> runtimeParameters) {
    this.runtimeParameters = runtimeParameters;
    return this;
  }

  public StartPipelineContextBuilder withInterceptorConfigurations(List<PipelineStartEvent.InterceptorConfiguration> configs) {
    if(configs != null) {
      this.interceptorConfigurations = configs;
    }
    return this;
  }

  public Runner.StartPipelineContext build() {
    return new StartPipelineContextImpl(
        user,
        runtimeParameters,
        interceptorConfigurations
    );
  }
}
