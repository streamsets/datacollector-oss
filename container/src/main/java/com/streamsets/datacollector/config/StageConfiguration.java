/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.config;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.impl.Utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StageConfiguration implements Serializable {

  //basic info
  private final String instanceName;
  private String library;
  private final String stageName;
  private int stageVersion;
  private final List<Config> configuration;
  private final Map<String, Config> configurationMap;
  private final Map<String, Object> uiInfo;

  //wiring with other components
  private final List<String> inputLanes;
  private final List<String> outputLanes;

  private boolean systemGenerated;

  public StageConfiguration(String instanceName, String library, String stageName, int stageVersion,
      List<Config> configuration, Map<String, Object> uiInfo, List<String> inputLanes,
      List<String> outputLanes) {
    this.instanceName = instanceName;
    this.library = library;
    this.stageName = stageName;
    this.stageVersion = stageVersion;
    this.uiInfo = (uiInfo != null) ? new HashMap<>(uiInfo) : new HashMap<String, Object>();
    this.inputLanes = inputLanes;
    this.outputLanes = outputLanes;
    this.configuration = new ArrayList<>();
    this.configurationMap = new HashMap<>();
    setConfig(configuration);
  }

  public String getInstanceName() {
    return instanceName;
  }

  public void setLibrary(String name) {
    library = name;
  }

  public String getLibrary() {
    return library;
  }

  public String getStageName() {
    return stageName;
  }

  public void setStageVersion(int version) {
    stageVersion = version;
  }

  public int getStageVersion() {
    return stageVersion;
  }

  public List<Config> getConfiguration() {
    return new ArrayList<>(configuration);
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

  public Config getConfig(String name) {
    return configurationMap.get(name);
  }

  public void setConfig(List<Config> configList) {
    configuration.clear();
    configuration.addAll(configList);
    configurationMap.clear();
    for (Config conf : configuration) {
      configurationMap.put(conf.getName(), conf);
    }
  }

  public void addConfig(Config config) {
    Config prevConfig = configurationMap.put(config.getName(), config);
    if (prevConfig != null) {
      configuration.remove(prevConfig);
    }
    configuration.add(config);
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
