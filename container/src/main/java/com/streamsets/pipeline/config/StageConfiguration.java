/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.ArrayList;
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

  public StageConfiguration(String instanceName, String library, String stageName, String stageVersion,
      List<ConfigConfiguration> configuration, Map<String, Object> uiInfo, List<String> inputLanes,
      List<String> outputLanes) {
    this.instanceName = instanceName;
    this.library = library;
    this.stageName = stageName;
    this.stageVersion = stageVersion;
    this.uiInfo = uiInfo;
    this.inputLanes = inputLanes;
    this.outputLanes = outputLanes;
    this.configuration = new ArrayList<>();
    this.configurationMap = new HashMap<>();
    setConfig(configuration);
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

  @VisibleForTesting
  public void setConfig(List<ConfigConfiguration> configList) {
    configuration.clear();
    configuration.addAll(configList);
    configurationMap.clear();
    for (ConfigConfiguration conf : configuration) {
      configurationMap.put(conf.getName(), conf);
    }
  }

  public void setSystemGenerated() {
    systemGenerated = true;
  }

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
