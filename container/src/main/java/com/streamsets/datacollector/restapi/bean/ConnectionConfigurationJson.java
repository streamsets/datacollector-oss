/*
 * Copyright 2020 StreamSets Inc.
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
import com.streamsets.datacollector.config.ConnectionConfiguration;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.List;

public class ConnectionConfigurationJson {

  private final ConnectionConfiguration connectionConfiguration;

  @JsonCreator
  public ConnectionConfigurationJson(
    @JsonProperty("type") String type,
    @JsonProperty("version") String version,
    @JsonProperty("configuration") List<ConfigConfigurationJson> configuration
  ) {
    this.connectionConfiguration = new ConnectionConfiguration(
        type,
        Integer.parseInt(version),
        BeanHelper.unwrapConfigConfiguration(configuration)
    );
  }

  public ConnectionConfigurationJson(ConnectionConfiguration connectionConfiguration) {
    Utils.checkNotNull(connectionConfiguration, "connectionConfiguration");
    this.connectionConfiguration = connectionConfiguration;
  }

  public String getType() {
    return connectionConfiguration.getType();
  }

  public String getVersion() {
    return Integer.toString(connectionConfiguration.getVersion());
  }

  public List<ConfigConfigurationJson> getConfiguration() {
    return BeanHelper.wrapConfigConfiguration(connectionConfiguration.getConfiguration());
  }

  public ConfigConfigurationJson getConfig(String name) {
    return BeanHelper.wrapConfigConfiguration(connectionConfiguration.getConfig(name));
  }

  @JsonIgnore
  public ConnectionConfiguration getConnectionConfiguration() {
    return connectionConfiguration;
  }
}
