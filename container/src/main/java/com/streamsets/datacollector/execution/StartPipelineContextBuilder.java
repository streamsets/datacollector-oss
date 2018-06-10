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

    StartPipelineContextImpl(
      String user,
      Map<String, Object> runtimeParameters
    ) {
      this.user = user;
      this.runtimeParameters = runtimeParameters;
    }

    @Override
    public String getUser() {
      return user;
    }

    @Override
    public Map<String, Object> getRuntimeParameters() {
      return runtimeParameters;
    }
  }

  private final String user;
  private Map<String, Object> runtimeParameters;

  public StartPipelineContextBuilder(String name) {
    this.user = name;
  }

  public StartPipelineContextBuilder withRuntimeParameters(Map<String, Object> runtimeParameters) {
    this.runtimeParameters = runtimeParameters;
    return this;
  }

  public Runner.StartPipelineContext build() {
    return new StartPipelineContextImpl(
      user,
      runtimeParameters
    );
  }
}
