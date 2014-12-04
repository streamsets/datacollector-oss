/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.pipeline.container.Utils;

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
  private final List<String> inputLanes;
  private final List<String> outputLanes;

  private boolean systemGenerated;

  @JsonCreator
  public StageConfiguration(
      @JsonProperty("instanceName") String instanceName,
      @JsonProperty("library") String library,
      @JsonProperty("stageName") String stageName,
      @JsonProperty("stageVersion") String stageVersion,
      @JsonProperty("configuration") List<ConfigConfiguration> configuration,
      @JsonProperty("uiInfo") Map<String, Object> uiInfo,
      @JsonProperty("inputLanes") List<String> inputLanes,
      @JsonProperty("outputLanes") List<String> outputLanes) {
    this.instanceName = instanceName;
    this.library = library;
    this.stageName = stageName;
    this.stageVersion = stageVersion;
    this.configuration = configuration;
    this.uiInfo = uiInfo;
    this.inputLanes = inputLanes;
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

  public List<String> getInputLanes() {
    return inputLanes;
  }

  public List<String> getOutputLanes() {
    return outputLanes;
  }

  public ConfigConfiguration getConfig(String name) {
    return configurationMap.get(name);
  }

  public void setSystemGenerated() {
    systemGenerated = true;
  }

  @JsonIgnore
  public boolean isSystemGenerated() {
    return systemGenerated;
  }

  @Override
  public String toString() {
    return Utils.format(
        "StageConfiguration[instanceName='{}' library='{}' name='{}' version='{}' input='{}' output='{}']",
        getInstanceName(), getLibrary(), getStageName(), getStageVersion(), getInputLanes(), getOutputLanes());
  }

}
