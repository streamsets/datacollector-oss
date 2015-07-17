/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi.bean;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.impl.Utils;

public class ConfigConfigurationJson {

  private final Config config;

  public ConfigConfigurationJson(
    @JsonProperty("name") String name,
    @JsonProperty("value") Object value) {
    this.config = new Config(name, value);
  }

  public ConfigConfigurationJson(Config config) {
    Utils.checkNotNull(config, "configConfiguration");
    this.config = config;
  }

  public String getName() {
    return config.getName();
  }

  public Object getValue() {
    return config.getValue();
  }

  @JsonIgnore
  public Config getConfigConfiguration() {
    return config;
  }
}