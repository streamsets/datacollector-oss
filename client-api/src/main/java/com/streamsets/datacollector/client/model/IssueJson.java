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
public class IssueJson   {

  private String message = null;
  private String level = null;
  private String instanceName = null;
  private String serviceName = null;
  private String configGroup = null;
  private String configName = null;
  private Map<String, Object> additionalInfo = new HashMap<>();
  private long count;
  private List<AntennaDoctorMessageJson> antennaDoctorMessages = new ArrayList<>();

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("message")
  public String getMessage() {
    return message;
  }
  public void setMessage(String message) {
    this.message = message;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("level")
  public String getLevel() {
    return level;
  }
  public void setLevel(String level) {
    this.level = level;
  }


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
  @JsonProperty("serviceName")
  public String getServiceName() {
    return serviceName;
  }
  public void setServiceName(String serviceName) {
    this.serviceName = serviceName;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("configGroup")
  public String getConfigGroup() {
    return configGroup;
  }
  public void setConfigGroup(String configGroup) {
    this.configGroup = configGroup;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("configName")
  public String getConfigName() {
    return configName;
  }
  public void setConfigName(String configName) {
    this.configName = configName;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("additionalInfo")
  public Map<String, Object> getAdditionalInfo() {
    return additionalInfo;
  }
  public void setAdditionalInfo(Map<String, Object> additionalInfo) {
    this.additionalInfo = additionalInfo;
  }

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("count")
  public long getCount() {
    return count;
  }
  public void setCount(long count) {
    this.count = count;
  }

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("antennaDoctorMessages")
  public List<AntennaDoctorMessageJson> getAntennaDoctorMessages() {
    return antennaDoctorMessages;
  }
  public void setAntennaDoctorMessages(List<AntennaDoctorMessageJson> antennaDoctorMessages) {
    this.antennaDoctorMessages = antennaDoctorMessages;
  }

  @Override
  public String toString()  {
    StringBuilder sb = new StringBuilder();
    sb.append("class IssueJson {\n");

    sb.append("    message: ").append(StringUtil.toIndentedString(message)).append("\n");
    sb.append("    level: ").append(StringUtil.toIndentedString(level)).append("\n");
    sb.append("    instanceName: ").append(StringUtil.toIndentedString(instanceName)).append("\n");
    sb.append("    serviceName: ").append(StringUtil.toIndentedString(serviceName)).append("\n");
    sb.append("    configGroup: ").append(StringUtil.toIndentedString(configGroup)).append("\n");
    sb.append("    configName: ").append(StringUtil.toIndentedString(configName)).append("\n");
    sb.append("    additionalInfo: ").append(StringUtil.toIndentedString(additionalInfo)).append("\n");
    sb.append("}");
    return sb.toString();
  }
}
