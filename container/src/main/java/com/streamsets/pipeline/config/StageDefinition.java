/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.pipeline.container.ApiUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

/**
 * Captures the configuration options for a {@link com.streamsets.pipeline.api.Stage}.
 *
 */
public class StageDefinition {
  private static final Logger LOG = LoggerFactory.getLogger(StageDefinition.class);

  private String library;
  private ClassLoader classLoader;
  private Class klass;

  private final String className;
  private final String name;
  private final String version;
  private final String label;
  private final String description;
  private final StageType type;
  private List<ConfigDefinition> configDefinitions;
  private Map<String, ConfigDefinition> configDefinitionsMap;

  @JsonCreator
  public StageDefinition(
    @JsonProperty("className") String className,
    @JsonProperty("name") String name,
    @JsonProperty("version") String version,
    @JsonProperty("label") String label,
    @JsonProperty("description") String description,
    @JsonProperty("type") StageType type,
    @JsonProperty("configDefinitions") List<ConfigDefinition> configDefinitions) {
    this.className = className;
    this.name = name;
    this.version = version;
    this.label = label;
    this.description = description;
    this.type = type;
    this.configDefinitions = configDefinitions;
    configDefinitionsMap = new HashMap<String, ConfigDefinition>();
    for (ConfigDefinition conf : configDefinitions) {
      configDefinitionsMap.put(conf.getName(), conf);
    }
  }

  public void setLibrary(String library, ClassLoader classLoader) {
    this.library = library;
    this.classLoader = classLoader;
    try {
      klass = classLoader.loadClass(getClassName());
    } catch (ClassNotFoundException ex) {
      throw new RuntimeException(ex);
    }
  }

  public String getLibrary() {
    return library;
  }

  @JsonIgnore
  public ClassLoader getClassLoader() {
    return classLoader;
  }

  public String getClassName() {
    return className;
  }

  @JsonIgnore
  public Class getStageClass() {
    return klass;
  }

  public String getName() {
    return name;
  }

  public String getVersion() {
    return version;
  }

  public String getLabel() {
    return label;
  }

  public String getDescription() {
    return description;
  }

  public StageType getType() {
    return type;
  }

  public List<ConfigDefinition> getConfigDefinitions() {
    return configDefinitions;
  }

  public ConfigDefinition getConfigDefinition(String configName) {
    return configDefinitionsMap.get(configName);
  }

  public String toString() {
    return ApiUtils.format("{}:{}:{}", getLibrary(), getName(), getVersion());
  }

  public StageDefinition localize(Locale locale) {
    String rbName = getClassName() + "-bundle";
    try {
      ResourceBundle rb = ResourceBundle.getBundle(rbName, locale, getClassLoader());
      return localize(rb);
    } catch (MissingResourceException ex) {
      LOG.warn("Could not find resource bundle '{}' in library '{}'", rbName, getLibrary());
      return this;
    }
  }

  private final static String STAGE_LABEL = "stage.label";
  private final static String STAGE_DESCRIPTION = "stage.description";

  public StageDefinition localize(ResourceBundle rb) {
    List<ConfigDefinition> configDefs = new ArrayList<ConfigDefinition>();
    for (ConfigDefinition configDef : getConfigDefinitions()) {
      configDefs.add(configDef.localize(rb));
    }
    String label = (rb.containsKey(STAGE_LABEL)) ? rb.getString(STAGE_LABEL) : getLabel();
    String description = (rb.containsKey(STAGE_DESCRIPTION)) ? rb.getString(STAGE_DESCRIPTION) : getDescription();
    StageDefinition def = new StageDefinition(getClassName(), getName(), getVersion(), label, description,
                                              getType(), configDefs);
    def.setLibrary(getLibrary(), getClassLoader());
    return def;
  }

}
