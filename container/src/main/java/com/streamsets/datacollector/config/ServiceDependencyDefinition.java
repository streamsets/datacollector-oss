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

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Collections;
import java.util.Map;

public class ServiceDependencyDefinition {

  private final Class service;
  private final Map<String, String> configuration;

  public ServiceDependencyDefinition(Class service, Map<String, String> configuration) {
    this.service = service;
    this.configuration = Collections.unmodifiableMap(configuration);
  }

  @JsonIgnore
  public Class getServiceClass() {
    return service;
  }

  public String getService() {
    return getServiceClass().getName();
  }

  public Map<String, String> getConfiguration() {
    return configuration;
  }
}
