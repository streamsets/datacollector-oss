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
import com.google.common.collect.ImmutableSet;
import com.streamsets.datacollector.creation.StageConfigBean;
import com.streamsets.pipeline.SDCClassLoader;
import com.streamsets.pipeline.api.ConnectionDef;
import com.streamsets.pipeline.api.ConnectionVerifier;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.HideStage;
import com.streamsets.pipeline.api.Label;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageType;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.LocalizableMessage;
import com.streamsets.pipeline.api.impl.Utils;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
  private final String verifierClass;

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
    verifierClass = def.verifierClass;
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
      String verifierClass
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
    this.verifierClass = verifierClass;
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

  public String getVerifierClass() {
    return verifierClass;
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
    return Utils.format("ConnectionDefinition[library='{}' name='{}' version='{}' type='{}' " +
                    "verifier class='{}']", getLibrary(), getName(), getVersion(), getType(), verifierClass);
  }
}


