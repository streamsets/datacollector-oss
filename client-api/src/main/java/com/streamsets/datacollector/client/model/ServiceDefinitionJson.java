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

public class ServiceDefinitionJson {
  private String className = null;
  private String version = null;
  private String description = null;
  private String label = null;
  private String library = null;
  private String libraryLabel = null;
  private ConfigGroupDefinitionJson configGroupDefinition = null;
  private Boolean privateClassLoader = null;

  public String getClassName() {
    return className;
  }

  public void setClassName(String className) {
    this.className = className;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getLabel() {
    return label;
  }

  public void setLabel(String label) {
    this.label = label;
  }

  public String getLibrary() {
    return library;
  }

  public void setLibrary(String library) {
    this.library = library;
  }

  public String getLibraryLabel() {
    return libraryLabel;
  }

  public void setLibraryLabel(String libraryLabel) {
    this.libraryLabel = libraryLabel;
  }

  public ConfigGroupDefinitionJson getConfigGroupDefinition() {
    return configGroupDefinition;
  }

  public void setConfigGroupDefinition(ConfigGroupDefinitionJson configGroupDefinition) {
    this.configGroupDefinition = configGroupDefinition;
  }

  public Boolean getPrivateClassLoader() {
    return privateClassLoader;
  }

  public void setPrivateClassLoader(Boolean privateClassLoader) {
    this.privateClassLoader = privateClassLoader;
  }
}
