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

import com.streamsets.datacollector.config.ConfigDefinition;
import com.streamsets.datacollector.config.ConfigGroupDefinition;
import com.streamsets.datacollector.config.ConnectionDefinition;
import com.streamsets.datacollector.config.StageLibraryDefinition;
import com.streamsets.datacollector.definition.ConnectionVerifierDefinition;
import com.streamsets.pipeline.api.ConnectionDef;
import com.streamsets.pipeline.api.ConnectionEngine;

import java.util.List;
import java.util.Map;

public class ConnectionDefinitionJson {

  private final String libraryDefinition;
  private final int version;
  private final String label;
  private final String description;
  private final String type;
  private final List<ConfigDefinition> configDefinitions;
  private final ConfigGroupDefinition configGroupDefinition;
  private final String yamlUpgrader;
  private final ConnectionEngine[] supportedEngines;
  private final ConnectionVerifierDefinition verifierDefinition;

  public ConnectionDefinitionJson(ConnectionDefinition connection, ConnectionVerifierDefinition verifier) {
    this.libraryDefinition = connection.getLibrary();
    this.version = connection.getVersion();
    this.label = connection.getLabel();
    this.description = connection.getDescription();
    this.type = connection.getType();
    this.configDefinitions = connection.getConfigDefinitions();
    this.configGroupDefinition = connection.getConfigGroupDefinition();
    this.yamlUpgrader = connection.getUpgrader();
    this.supportedEngines = connection.getSupportedEngines();
    this.verifierDefinition = verifier;
  }

  public String getLibrary() {
    return libraryDefinition;
  }

  public int getVersion() {
    return version;
  }

  public String getLabel() {
    return label;
  }

  public String getDescription() {
    return description;
  }

  public String getType() {
    return type;
  }

  public List<ConfigDefinition> getConfigDefinitions() {
    return configDefinitions;
  }

  public ConfigGroupDefinition getConfigGroupDefinition() {
    return configGroupDefinition;
  }

  public String getYamlUpgrader() {
    return yamlUpgrader;
  }

  public ConnectionEngine[] getSupportedEngines() {
    return supportedEngines;
  }

  public ConnectionVerifierDefinition getVerifierDefinition() {
    return verifierDefinition;
  }
}
