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
package com.streamsets.datacollector.restapi.bean;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.datacollector.config.ServiceConfiguration;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.List;

public class ServiceConfigurationJson {

  private final com.streamsets.datacollector.config.ServiceConfiguration serviceConfiguration;

  @JsonCreator
  public ServiceConfigurationJson(
    @JsonProperty("service") String service,
    @JsonProperty("serviceVersion") int serviceVersion,
    @JsonProperty("configuration") List<ConfigConfigurationJson> configuration
  ) {
    Class serviceKlass = null;
    try {
      serviceKlass = Class.forName(service);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Unknown service class", e);
    }

    this.serviceConfiguration = new com.streamsets.datacollector.config.ServiceConfiguration(
      serviceKlass,
      serviceVersion,
      BeanHelper.unwrapConfigConfiguration(configuration)
    );
  }

  public ServiceConfigurationJson(com.streamsets.datacollector.config.ServiceConfiguration serviceConfiguration) {
    Utils.checkNotNull(serviceConfiguration, "serviceConfiguration");
    this.serviceConfiguration = serviceConfiguration;
  }

  public String getService() {
    return serviceConfiguration.getService().getName();
  }

  public int getServiceVersion() {
    return serviceConfiguration.getServiceVersion();
  }

  public List<ConfigConfigurationJson> getConfiguration() {
    return BeanHelper.wrapConfigConfiguration(serviceConfiguration.getConfiguration());
  }

  @JsonIgnore
  public ServiceConfiguration getServiceConfiguration() {
    return serviceConfiguration;
  }
}
