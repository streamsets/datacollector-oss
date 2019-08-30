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
package com.streamsets.datacollector.config;

import com.streamsets.pipeline.api.Config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ServiceConfiguration implements UserConfigurable {

  private final Class service;
  private int serviceVersion;
  private List<Config> configuration;

  // Calculated, not serialized
  private Map<String, Config> configMap;

  public ServiceConfiguration(
      Class service,
      int serviceVersion,
      List<Config> configuration
  ) {
    this.service = service;
    this.serviceVersion = serviceVersion;
    this.configuration = configuration;
    setConfig(configuration);
  }

  public Class getService() {
    return service;
  }

  public int getServiceVersion() {
    return serviceVersion;
  }

  public void setServiceVersion(int version) {
    this.serviceVersion = version;
  }

  @Override
  public Config getConfig(String name) {
    return configMap.get(name);
  }

  public void addConfig(Config config) {
    Config prevConfig = configMap.put(config.getName(), config);
    if (prevConfig != null) {
      configuration.remove(prevConfig);
    }
    configuration.add(config);
  }

  @Override
  public List<Config> getConfiguration() {
    return new ArrayList<>(configuration);
  }

  public void setConfig(List<Config> configList) {
    this.configuration = configList;
    this.configMap = configuration.stream().collect(Collectors.toMap(Config::getName, c -> c));
  }
}
