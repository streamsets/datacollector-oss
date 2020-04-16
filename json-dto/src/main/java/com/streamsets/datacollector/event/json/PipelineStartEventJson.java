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
package com.streamsets.datacollector.event.json;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class PipelineStartEventJson extends PipelineBaseEventJson {

  public static class InterceptorConfigurationJson {
    String stageLibrary;
    String interceptorClassName;
    Map<String, String> parameters;

    public String getStageLibrary() {
      return stageLibrary;
    }

    public void setStageLibrary(String stageLibrary) {
      this.stageLibrary = stageLibrary;
    }

    public String getInterceptorClassName() {
      return interceptorClassName;
    }

    public void setInterceptorClassName(String interceptorClassName) {
      this.interceptorClassName = interceptorClassName;
    }

    public Map<String, String> getParameters() {
      return parameters;
    }

    public void setParameters(Map<String, String> parameters) {
      this.parameters = parameters;
    }
  }

  private List<InterceptorConfigurationJson> interceptorConfiguration;

  List<String> groups;

  public List<String> getGroups() {
    return this.groups;
  }

  public void setGroups(List<String> groups) {
    this.groups = groups;
  }

  public List<InterceptorConfigurationJson> getInterceptorConfiguration() {
    return interceptorConfiguration;
  }

  public void setInterceptorConfiguration(List<InterceptorConfigurationJson> interceptorConfiguration) {
    this.interceptorConfiguration = interceptorConfiguration;
  }
}
