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
package com.streamsets.datacollector.config;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.impl.Utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConnectionConfiguration implements Serializable, UserConfigurable {

  private String type;
  private int version;
  private final List<Config> configuration;
  private final Map<String, Config> configurationMap;

  public ConnectionConfiguration(String type, int version, List<Config> configuration) {
    this.type = type;
    this.version = version;
    this.configuration = new ArrayList<>();
    this.configurationMap = new HashMap<>();
    setConfig(configuration);
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  @Override
  public List<Config> getConfiguration() {
    return new ArrayList<>(configuration);
  }

  @Override
  public Config getConfig(String name) {
    return configurationMap.get(name);
  }

  public void setConfig(List<Config> configList) {
    configuration.clear();
    configuration.addAll(configList);
    configurationMap.clear();
    for (Config conf : configuration) {
      configurationMap.put(conf.getName(), conf);
    }
  }

  public void addConfig(Config config) {
    Config prevConfig = configurationMap.put(config.getName(), config);
    if (prevConfig != null) {
      configuration.remove(prevConfig);
    }
    configuration.add(config);
  }

  @Override
  public String toString() {
    return Utils.format(
        "ConnectionConfiguration[type='{}' version='{}']",
        getType(), getVersion());
  }
}
