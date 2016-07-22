/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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
package com.streamsets.datacollector.client.model;

import com.streamsets.datacollector.client.StringUtil;
import com.streamsets.datacollector.client.model.ConfigConfigurationJson;
import java.util.*;


import io.swagger.annotations.*;
import com.fasterxml.jackson.annotation.JsonProperty;


@ApiModel(description = "")
@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaClientCodegen", date = "2015-09-11T14:51:29.367-07:00")
public class StageConfigurationJson   {

  private String instanceName = null;
  private String library = null;
  private String stageName = null;
  private String stageVersion = null;
  private List<ConfigConfigurationJson> configuration = new ArrayList<ConfigConfigurationJson>();
  private Map<String, Object> uiInfo = new HashMap<String, Object>();
  private List<String> inputLanes = new ArrayList<String>();
  private List<String> outputLanes = new ArrayList<String>();
  private List<String> eventLanes = new ArrayList<String>();


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
    sb.append("    uiInfo: ").append(StringUtil.toIndentedString(uiInfo)).append("\n");
    sb.append("    inputLanes: ").append(StringUtil.toIndentedString(inputLanes)).append("\n");
    sb.append("    outputLanes: ").append(StringUtil.toIndentedString(outputLanes)).append("\n");
    sb.append("    eventLanes: ").append(StringUtil.toIndentedString(eventLanes)).append("\n");
    sb.append("}");
    return sb.toString();
  }
}
