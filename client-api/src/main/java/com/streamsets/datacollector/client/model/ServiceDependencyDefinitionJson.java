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
import io.swagger.annotations.ApiModelProperty;

import java.util.Map;

public class ServiceDependencyDefinitionJson {

  private String service;
  private Map<String, String> configuration;

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("service")
  public String getService() {
    return service;
  }
  public void setService(String service) {
    this.service = service;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("configuration")
  public Map<String, String> getConfiguration() {
    return configuration;
  }
  public void setConfiguration(Map<String, String> configuration) {
    this.configuration = configuration;
  }

}
