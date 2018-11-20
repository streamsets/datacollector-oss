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
import io.swagger.annotations.ApiModelProperty;

import java.util.ArrayList;
import java.util.List;

public class ServiceConfigurationJson {

  private String service = null;
  private int serviceVersion;
  private List<ConfigConfigurationJson> configuration = new ArrayList<>();

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("service")
  public String getService() {
    return service;
  }
  public void setServiceName(String serviceName) {
    this.service = serviceName;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("serviceVersion")
  public int getServiceVersion() {
    return serviceVersion;
  }
  public void setServiceVersion(int serviceVersion) {
    this.serviceVersion = serviceVersion;
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

  @Override
  public String toString()  {
    StringBuilder sb = new StringBuilder();
    sb.append("class ServiceConfigurationJson {\n");
    sb.append("    service: ").append(StringUtil.toIndentedString(service)).append("\n");
    sb.append("    serviceVersion: ").append(StringUtil.toIndentedString(serviceVersion)).append("\n");
    sb.append("    configuration: ").append(StringUtil.toIndentedString(configuration)).append("\n");
    sb.append("}");
    return sb.toString();
  }
}
