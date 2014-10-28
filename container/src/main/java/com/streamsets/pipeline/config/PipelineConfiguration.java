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
package com.streamsets.pipeline.config;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PipelineConfiguration {

  private List<StageConfiguration> runtimeModuleConfigurations = null;
  private String uuid = null;
  private Map<String, List<String>> errorsMap = null;


  public PipelineConfiguration() {
    runtimeModuleConfigurations = new ArrayList<StageConfiguration>();
    errorsMap = new LinkedHashMap<String, List<String>>();
  }

  @JsonFormat()
  public List<StageConfiguration> getRuntimeModuleConfigurations() {
    return runtimeModuleConfigurations;
  }

  public void setRuntimeModuleConfigurations(List<StageConfiguration> runtimeModuleConfigurations) {
    this.runtimeModuleConfigurations = runtimeModuleConfigurations;
  }

  public String getUuid() {
    return uuid;
  }

  public void setUuid(String uuid) {
    this.uuid = uuid;
  }

  public Map<String, List<String>> getErrorsMap() {
    return errorsMap;
  }

  public void setErrorsMap(Map<String, List<String>> errorsMap) {
    this.errorsMap = errorsMap;
  }
}
