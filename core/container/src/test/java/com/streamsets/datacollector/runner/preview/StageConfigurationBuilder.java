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
package com.streamsets.datacollector.runner.preview;

import com.streamsets.datacollector.config.ServiceConfiguration;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.pipeline.api.Config;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class StageConfigurationBuilder {
  private String instanceName;
  private String library = "default";
  private String stageName;
  private int stageVersion = 1;
  private List<Config> configuration = Collections.emptyList();
  private Map<String, Object> uiInfo = null;
  private List<ServiceConfiguration> services = Collections.emptyList();
  private List<String> inputLanes = Collections.emptyList();
  private List<String> outputLanes = Collections.emptyList();
  private List<String> eventLanes = Collections.emptyList();

  public StageConfigurationBuilder(String instanceName, String stageName) {
    this.instanceName = instanceName;
    this.stageName = stageName;
  }

  public StageConfigurationBuilder withLibrary(String library) {
    this.library = library;
    return this;
  }

  public StageConfigurationBuilder withStageVersion(int version) {
    this.stageVersion = version;
    return this;
  }

  public StageConfigurationBuilder withConfig(Config ...configs) {
    this.configuration = Arrays.asList(configs);
    return this;
  }

  public StageConfigurationBuilder withServices(ServiceConfiguration ...services) {
    this.services = Arrays.asList(services);
    return this;
  }

  public StageConfigurationBuilder withServices(List<ServiceConfiguration> services) {
    this.services = services;
    return this;
  }
  public StageConfigurationBuilder withInputLanes(String ...lanes) {
    this.inputLanes = Arrays.asList(lanes);
    return this;
  }

  public StageConfigurationBuilder withInputLanes(List<String> lanes) {
    this.inputLanes = lanes;
    return this;
  }

  public StageConfigurationBuilder withOutputLanes(String ...lanes) {
    this.outputLanes = Arrays.asList(lanes);
    return this;
  }

  public StageConfigurationBuilder withOutputLanes(List<String> lanes) {
    this.outputLanes = lanes;
    return this;
  }

  public StageConfigurationBuilder withEventLanes(String ...lanes) {
    this.eventLanes = Arrays.asList(lanes);
    return this;
  }

  public StageConfigurationBuilder withEventLanes(List<String> lanes) {
    this.eventLanes = lanes;
    return this;
  }

  public StageConfiguration build() {
    return new StageConfiguration(
      instanceName,
      library,
      stageName,
      stageVersion,
      configuration,
      uiInfo,
      services,
      inputLanes,
      outputLanes,
      eventLanes
    );
  }

}
