package com.streamsets.pipeline.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Created by harikiran on 10/20/14.
 */
public class RuntimeStageConfiguration {

  //basic info
  private final String instanceName;
  private final String moduleName;
  private final String moduleVersion;
  private final String moduleDescription;

  //configuration values
  private List<ConfigOption> configOptions = null;

  //ui options
  int xPos;
  int yPos;

  //wiring with other components
  private List<String> inputLanes;
  private List<String> outputLanes;

  @JsonCreator
  public RuntimeStageConfiguration(
      @JsonProperty("instanceName") String instanceName,
      @JsonProperty("moduleName") String moduleName,
      @JsonProperty("moduleVersion") String moduleVersion,
      @JsonProperty("moduleDescription") String moduleDescription,
      @JsonProperty("configOptions") List<ConfigOption> configOptions,
      @JsonProperty("xPos") int xPos,
      @JsonProperty("yPos") int yPos,
      @JsonProperty("inputLanes") List<String> inputLanes,
      @JsonProperty("outputLanes") List<String> outputLanes) {
    this.instanceName = instanceName;
    this.moduleName = moduleName;
    this.moduleVersion = moduleVersion;
    this.moduleDescription = moduleDescription;
    this.configOptions = configOptions;
    this.xPos = xPos;
    this.yPos = yPos;
    this.inputLanes = inputLanes;
    this.outputLanes = outputLanes;
  }

  public String getInstanceName() {
    return instanceName;
  }

  public String getModuleName() {
    return moduleName;
  }

  public String getModuleVersion() {
    return moduleVersion;
  }

  public String getModuleDescription() {
    return moduleDescription;
  }

  public List<ConfigOption> getConfigOptions() {
    return configOptions;
  }

  public void setConfigOptions(List<ConfigOption> configOptions) {
    this.configOptions = configOptions;
  }

  public int getxPos() {
    return xPos;
  }

  public void setxPos(int xPos) {
    this.xPos = xPos;
  }

  public int getyPos() {
    return yPos;
  }

  public void setyPos(int yPos) {
    this.yPos = yPos;
  }

  public List<String> getInputLanes() {
    return inputLanes;
  }

  public void setInputLanes(List<String> inputLanes) {
    this.inputLanes = inputLanes;
  }

  public List<String> getOutputLanes() {
    return outputLanes;
  }

  public void setOutputLanes(List<String> outputLanes) {
    this.outputLanes = outputLanes;
  }
}
