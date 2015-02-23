/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi.bean;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.pipeline.api.impl.Utils;

public class ConfigConfigurationJson {

  private final com.streamsets.pipeline.config.ConfigConfiguration configConfiguration;

  public ConfigConfigurationJson(
    @JsonProperty("name") String name,
    @JsonProperty("value") Object value) {
    this.configConfiguration = new com.streamsets.pipeline.config.ConfigConfiguration(name, value);
  }

  public ConfigConfigurationJson(com.streamsets.pipeline.config.ConfigConfiguration configConfiguration) {
    Utils.checkNotNull(configConfiguration, "configConfiguration");
    this.configConfiguration = configConfiguration;
  }

  public String getName() {
    return configConfiguration.getName();
  }

  public Object getValue() {
    return configConfiguration.getValue();
  }

  @JsonIgnore
  public com.streamsets.pipeline.config.ConfigConfiguration getConfigConfiguration() {
    return configConfiguration;
  }
}