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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StageConfiguration {

  //basic info
  private final String instanceName;
  private final String library;
  private final String stageName;
  private final String stageVersion;
  private final List<ConfigConfiguration> configuration;
  private final Map<String, ConfigConfiguration> configurationMap;
  private final Map<String, Object> uiInfo;

  //wiring with other components
  private final String inputLane;
  private final List<String> outputLanes;

  @JsonCreator
  public StageConfiguration(
      @JsonProperty("instanceName") String instanceName,
      @JsonProperty("library") String library,
      @JsonProperty("stageName") String stageName,
      @JsonProperty("stageVersion") String stageVersion,
      @JsonProperty("configuration") List<ConfigConfiguration> configuration,
      @JsonProperty("uiInfo") Map<String, Object> uiInfo,
      @JsonProperty("inputLane") String inputLane,
      @JsonProperty("outputLanes") List<String> outputLanes) {
    this.instanceName = instanceName;
    this.library = library;
    this.stageName = stageName;
    this.stageVersion = stageVersion;
    this.configuration = configuration;
    this.uiInfo = uiInfo;
    this.inputLane = inputLane;
    this.outputLanes = outputLanes;
    configurationMap = new HashMap<String, ConfigConfiguration>();
    for (ConfigConfiguration conf : configuration) {
      configurationMap.put(conf.getName(), conf);
    }
  }

  public String getInstanceName() {
    return instanceName;
  }

  public String getLibrary() {
    return library;
  }

  public String getStageName() {
    return stageName;
  }

  public String getStageVersion() {
    return stageVersion;
  }

  public List<ConfigConfiguration> getConfiguration() {
    return configuration;
  }

  public Map<String, Object> getUiInfo() {
    return uiInfo;
  }

  public String getInputLane() {
    return inputLane;
  }

  public List<String> getOutputLanes() {
    return outputLanes;
  }

  public ConfigConfiguration getConfig(String name) {
    return configurationMap.get(name);
  }

}
