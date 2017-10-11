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
package com.streamsets.datacollector.restapi.bean;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class StageConfigurationJson {

  private final com.streamsets.datacollector.config.StageConfiguration stageConfiguration;

  @JsonCreator
  public StageConfigurationJson(
    @JsonProperty("instanceName") String instanceName,
    @JsonProperty("library") String library,
    @JsonProperty("stageName") String stageName,
    @JsonProperty("stageVersion") String stageVersion,
    @JsonProperty("configuration") List<ConfigConfigurationJson> configuration,
    @JsonProperty("uiInfo") Map<String, Object> uiInfo,
    @JsonProperty("inputLanes") List<String> inputLanes,
    @JsonProperty("outputLanes") List<String> outputLanes,
    @JsonProperty("eventLanes") List<String> eventLanes,
    @JsonProperty("services") List<ServiceConfigurationJson> services
  ) {
    this.stageConfiguration = new com.streamsets.datacollector.config.StageConfiguration(
      instanceName,
      library,
      stageName,
      convertVersion(stageVersion),
      BeanHelper.unwrapConfigConfiguration(configuration),
      uiInfo,
      BeanHelper.unwrapServiceConfiguration(services),
      inputLanes,
      outputLanes,
      eventLanes
    );
  }

  private static int convertVersion(String version) {
    return ("1.0.0".equals(version)) ? 1 : Integer.parseInt(version);
  }
  public StageConfigurationJson(com.streamsets.datacollector.config.StageConfiguration stageConfiguration) {
    Utils.checkNotNull(stageConfiguration, "stageConfiguration");
    this.stageConfiguration = stageConfiguration;
  }

  public String getInstanceName() {
    return stageConfiguration.getInstanceName();
  }

  public String getLibrary() {
    return stageConfiguration.getLibrary();
  }

  public String getStageName() {
    return stageConfiguration.getStageName();
  }

  public String getStageVersion() {
    return Integer.toString(stageConfiguration.getStageVersion());
  }

  public List<ConfigConfigurationJson> getConfiguration() {
    return BeanHelper.wrapConfigConfiguration(stageConfiguration.getConfiguration());
  }

  public Map<String, Object> getUiInfo() {
    return stageConfiguration.getUiInfo();
  }

  public List<ServiceConfigurationJson> getServices() {
    return BeanHelper.wrapServiceConfiguration(stageConfiguration.getServices());
  }

  public List<String> getInputLanes() {
    return stageConfiguration.getInputLanes();
  }

  public List<String> getOutputLanes() {
    return stageConfiguration.getOutputLanes();
  }

  public List<String> getEventLanes() {
    return stageConfiguration.getEventLanes();
  }

  public ConfigConfigurationJson getConfig(String name) {
    return BeanHelper.wrapConfigConfiguration(stageConfiguration.getConfig(name));
  }

  @JsonIgnore
  public com.streamsets.datacollector.config.StageConfiguration getStageConfiguration() {
    return stageConfiguration;
  }

  @JsonIgnore
  public List<String> getOutputAndEventLanes() {
    List<String> outputAndEventLanes = new LinkedList<>();
    if (getOutputLanes() != null) {
      outputAndEventLanes.addAll(getOutputLanes());
    }
    if (getEventLanes() != null) {
      outputAndEventLanes.addAll(getEventLanes());
    }
    return outputAndEventLanes;
  }
}
