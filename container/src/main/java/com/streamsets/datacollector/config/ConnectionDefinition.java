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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.streamsets.pipeline.api.ConnectionDef;
import com.streamsets.pipeline.api.ConnectionEngine;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Captures the configuration options for a {@link ConnectionDef}.
 *
 */
public class ConnectionDefinition implements PrivateClassLoaderDefinition {

  private final ConnectionDef connectionDef;
  private final StageLibraryDefinition libraryDefinition;
  private final ClassLoader classLoader;
  private final int version;
  private final String label;
  private final String description;
  private final String type;
  private final List<ConfigDefinition> configDefinitions;
  private final Map<String, ConfigDefinition> configDefinitionsMap;
  private final ConfigGroupDefinition configGroupDefinition;
  private final String yamlUpgrader;
  private final ConnectionEngine[] supportedEngines;

  @SuppressWarnings("unchecked")
  public ConnectionDefinition(ConnectionDefinition def, ClassLoader classLoader) {
    connectionDef = def.connectionDef;
    libraryDefinition = def.libraryDefinition;
    this.classLoader = classLoader;
    version = def.version;
    label = def.label;
    description = def.description;
    type = def.type;
    configDefinitions = def.configDefinitions;
    configDefinitionsMap = def.configDefinitionsMap;
    configGroupDefinition = def.configGroupDefinition;
    yamlUpgrader = (def.yamlUpgrader.isEmpty()) ? null : def.yamlUpgrader;
    supportedEngines = def.supportedEngines;
  }

  public ConnectionDefinition(
      ConnectionDef connectionDef,
      StageLibraryDefinition libraryDefinition,
      int version,
      String label,
      String description,
      String type,
      List<ConfigDefinition> configDefinitions,
      ConfigGroupDefinition configGroupDefinition,
      String yamlUpgrader,
      ConnectionEngine[] supportedEngines
  ) {
    this.connectionDef = connectionDef;
    this.libraryDefinition = libraryDefinition;
    this.classLoader = libraryDefinition.getClassLoader();
    this.version = version;
    this.label = label;
    this.description = description;
    this.type = type;
    this.configDefinitions = configDefinitions;
    this.configDefinitionsMap = new HashMap<>();
    this.configGroupDefinition = configGroupDefinition;
    for (ConfigDefinition conf : configDefinitions) {
      configDefinitionsMap.put(conf.getName(), conf);
      ModelDefinition modelDefinition = conf.getModel();
      if (modelDefinition != null && modelDefinition.getConfigDefinitions() != null) {
        //Multi level complex is not allowed. So we stop at this level
        //Assumption is that the config property names are unique in the class hierarchy
        //and across complex types
        for (ConfigDefinition configDefinition : modelDefinition.getConfigDefinitions()) {
          configDefinitionsMap.put(configDefinition.getName(), configDefinition);
        }
      }
    }
    this.yamlUpgrader = yamlUpgrader;
    this.supportedEngines = supportedEngines;
  }

  @JsonIgnore
  @Override
  public ClassLoader getStageClassLoader() {
    return classLoader;
  }

  @Override
  public boolean isPrivateClassLoader() {
    return false;
  }

  @Override
  public String getName() {
    return label;
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

  public String getLibrary() {
    return libraryDefinition.getName();
  }

  public String getUpgrader() {
    return yamlUpgrader;
  }

  public void addConfiguration(ConfigDefinition confDef) {
    if (configDefinitionsMap.containsKey(confDef.getName())) {
      throw new IllegalArgumentException(Utils.format("Connection '{}:{}:{}', configuration definition '{}' already exists",
              getLibrary(), getName(), getVersion(), confDef.getName()));
    }
    configDefinitionsMap.put(confDef.getName(), confDef);
    configDefinitions.add(confDef);
  }

  public List<ConfigDefinition> getConfigDefinitions() {
    return configDefinitions;
  }

  public ConfigDefinition getConfigDefinition(String configName) {
    return configDefinitionsMap.get(configName);
  }

  public ConfigGroupDefinition getConfigGroupDefinition() {
    return configGroupDefinition;
  }

  @JsonIgnore
  // This method returns not only main configs, but also all complex ones!
  public Map<String, ConfigDefinition> getConfigDefinitionsMap() {
    return configDefinitionsMap;
  }

  @Override
  public String toString() {
    return Utils.format(
        "ConnectionDefinition[library='{}' name='{}' version='{}' type='{}' supported engines='{}']",
        getLibrary(), getName(), getVersion(), getType(), supportedEngines
    );
  }

  public ConnectionEngine[] getSupportedEngines() {
    return supportedEngines;
  }
}
