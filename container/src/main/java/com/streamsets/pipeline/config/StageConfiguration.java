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

import java.util.List;

public class StageConfiguration {

  //basic info
  private final String instanceName;
  private final String moduleName;
  private final String moduleVersion;
  private final String moduleDescription;

  //configuration values
  private List<ConfigDefinition> configDefinitions = null;

  //ui options
  int xPos;
  int yPos;

  //wiring with other components
  private List<String> inputLanes;
  private List<String> outputLanes;

  @JsonCreator
  public StageConfiguration(
      @JsonProperty("instanceName") String instanceName,
      @JsonProperty("moduleName") String moduleName,
      @JsonProperty("moduleVersion") String moduleVersion,
      @JsonProperty("moduleDescription") String moduleDescription,
      @JsonProperty("configOptions") List<ConfigDefinition> configDefinitions,
      @JsonProperty("xPos") int xPos,
      @JsonProperty("yPos") int yPos,
      @JsonProperty("inputLanes") List<String> inputLanes,
      @JsonProperty("outputLanes") List<String> outputLanes) {
    this.instanceName = instanceName;
    this.moduleName = moduleName;
    this.moduleVersion = moduleVersion;
    this.moduleDescription = moduleDescription;
    this.configDefinitions = configDefinitions;
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

  public List<ConfigDefinition> getConfigDefinitions() {
    return configDefinitions;
  }

  public void setConfigDefinitions(List<ConfigDefinition> configDefinitions) {
    this.configDefinitions = configDefinitions;
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
