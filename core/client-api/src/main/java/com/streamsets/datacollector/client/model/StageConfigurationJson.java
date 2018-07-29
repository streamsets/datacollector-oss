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
package com.streamsets.datacollector.client.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.datacollector.client.StringUtil;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@ApiModel(description = "")
public class StageConfigurationJson   {

  private String instanceName = null;
  private String library = null;
  private String stageName = null;
  private String stageVersion = null;
  private List<ConfigConfigurationJson> configuration = new ArrayList<>();
  private List<ServiceConfigurationJson> services = new ArrayList<>();
  private Map<String, Object> uiInfo = new HashMap<>();
  private List<String> inputLanes = new ArrayList<>();
  private List<String> outputLanes = new ArrayList<>();
  private List<String> eventLanes = new ArrayList<>();


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("instanceName")
  public String getInstanceName() {
    return instanceName;
  }
  public void setInstanceName(String instanceName) {
    this.instanceName = instanceName;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("library")
  public String getLibrary() {
    return library;
  }
  public void setLibrary(String library) {
    this.library = library;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("stageName")
  public String getStageName() {
    return stageName;
  }
  public void setStageName(String stageName) {
    this.stageName = stageName;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("stageVersion")
  public String getStageVersion() {
    return stageVersion;
  }
  public void setStageVersion(String stageVersion) {
    this.stageVersion = stageVersion;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("configuration")
  public List<ConfigConfigurationJson> getConfiguration() {
    return configuration;
  }
  public void setConfiguration(List<ConfigConfigurationJson> configuration) {
    this.configuration = configuration;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("services")
  public List<ServiceConfigurationJson> getServices() {
    return services;
  }
  public void setServices(List<ServiceConfigurationJson> services) {
    this.services = services;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("uiInfo")
  public Map<String, Object> getUiInfo() {
    return uiInfo;
  }
  public void setUiInfo(Map<String, Object> uiInfo) {
    this.uiInfo = uiInfo;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("inputLanes")
  public List<String> getInputLanes() {
    return inputLanes;
  }
  public void setInputLanes(List<String> inputLanes) {
    this.inputLanes = inputLanes;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("outputLanes")
  public List<String> getOutputLanes() {
    return outputLanes;
  }
  public void setOutputLanes(List<String> outputLanes) {
    this.outputLanes = outputLanes;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("eventLanes")
  public List<String> getEventLanes() {
    return eventLanes;
  }
  public void setEventLanes(List<String> eventLanes) {
    this.eventLanes = eventLanes;
  }



  @Override
  public String toString()  {
    StringBuilder sb = new StringBuilder();
    sb.append("class StageConfigurationJson {\n");

    sb.append("    instanceName: ").append(StringUtil.toIndentedString(instanceName)).append("\n");
    sb.append("    library: ").append(StringUtil.toIndentedString(library)).append("\n");
    sb.append("    stageName: ").append(StringUtil.toIndentedString(stageName)).append("\n");
    sb.append("    stageVersion: ").append(StringUtil.toIndentedString(stageVersion)).append("\n");
    sb.append("    configuration: ").append(StringUtil.toIndentedString(configuration)).append("\n");
    sb.append("    services: ").append(StringUtil.toIndentedString(services)).append("\n");
    sb.append("    uiInfo: ").append(StringUtil.toIndentedString(uiInfo)).append("\n");
    sb.append("    inputLanes: ").append(StringUtil.toIndentedString(inputLanes)).append("\n");
    sb.append("    outputLanes: ").append(StringUtil.toIndentedString(outputLanes)).append("\n");
    sb.append("    eventLanes: ").append(StringUtil.toIndentedString(eventLanes)).append("\n");
    sb.append("}");
    return sb.toString();
  }
}
