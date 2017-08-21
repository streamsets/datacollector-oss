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
