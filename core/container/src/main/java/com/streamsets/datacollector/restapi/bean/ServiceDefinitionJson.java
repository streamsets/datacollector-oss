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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.streamsets.datacollector.config.ServiceDefinition;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ServiceDefinitionJson {
  private final ServiceDefinition serviceDefinition;

  ServiceDefinitionJson(ServiceDefinition serviceDefinition) {
    this.serviceDefinition = serviceDefinition;
  }

  public String getProvides() {
    return serviceDefinition.getProvides().getName();
  }

  public ConfigGroupDefinitionJson getConfigGroupDefinition() {
    return BeanHelper.wrapConfigGroupDefinition(serviceDefinition.getGroupDefinition());
  }

  public boolean isPrivateClassLoader() {
    return serviceDefinition.isPrivateClassLoader();
  }

  public String getClassName() {
    return serviceDefinition.getClassName();
  }

  public String getVersion() {
    return Integer.toString(serviceDefinition.getVersion());
  }

  public String getLabel() {
    return serviceDefinition.getLabel();
  }

  public String getDescription() {
    return serviceDefinition.getDescription();
  }

  public List<ConfigDefinitionJson> getConfigDefinitions() {
    return BeanHelper.wrapConfigDefinitions(serviceDefinition.getConfigDefinitions());
  }

  public String getLibrary() {
    return serviceDefinition.getLibraryDefinition().getName();
  }

  public String getLibraryLabel() {
    return serviceDefinition.getLibraryDefinition().getLabel();
  }
}
