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
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.container.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * Captures the configuration options for a {@link com.streamsets.pipeline.api.Stage}.
 *
 */
public class StageDefinition {
  private static final Logger LOG = LoggerFactory.getLogger(StageDefinition.class);

  private static final String SEPERATOR = ".";
  private static final String CONFIG = "config";
  private static final String FIELD_MODIFIER = "FieldModifier";

  private String library;
  private ClassLoader classLoader;
  private Class klass;

  private final String className;
  private final String name;
  private final String version;
  private final String label;
  private final String description;
  private final StageType type;
  private final StageDef.OnError onError;
  private List<ConfigDefinition> configDefinitions;
  private Map<String, ConfigDefinition> configDefinitionsMap;
  private final String icon;

  @JsonCreator
  public StageDefinition(
    @JsonProperty("className") String className,
    @JsonProperty("name") String name,
    @JsonProperty("version") String version,
    @JsonProperty("label") String label,
    @JsonProperty("description") String description,
    @JsonProperty("type") StageType type,
    @JsonProperty("configDefinitions") List<ConfigDefinition> configDefinitions,
    @JsonProperty("onError") StageDef.OnError onError,
    @JsonProperty("icon") String icon) {
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
    this.onError = onError;
    this.icon = icon;
  }

  public void setLibrary(String library, ClassLoader classLoader) {
    this.library = library;
    this.classLoader = classLoader;
    try {
      klass = classLoader.loadClass(getClassName());
    } catch (ClassNotFoundException ex) {
      throw new RuntimeException(ex);
    }
    updateValuesAndLabelsFromValuesProvider();
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

  public void addConfiguration(ConfigDefinition confDef) {
    if (configDefinitionsMap.containsKey(confDef.getName())) {
      throw new IllegalArgumentException(Utils.format("Stage '{}:{}:{}', configuration definition '{}' already exists",
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

  public String toString() {
    return Utils.format("{}:{}:{}", getLibrary(), getName(), getVersion());
  }

  public StageDef.OnError getOnError() {
    return onError;
  }

  public String getIcon() {
    return icon;
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
    StageDefinition def = new StageDefinition(
      getClassName(), getName(), getVersion(), label, description,
      getType(), configDefs, getOnError(), getIcon());
    def.setLibrary(getLibrary(), getClassLoader());

    for(ConfigDefinition configDef : def.getConfigDefinitions()) {
      if(configDef.getModel() != null &&
        configDef.getModel().getValues() != null &&
        !configDef.getModel().getValues().isEmpty()) {

        if(!(configDef.getModel().getLabels() != null &&
        !configDef.getModel().getLabels().isEmpty() &&
          configDef.getModel().getLabels().size() == configDef.getModel().getValues().size())) {
          //TODO: throw the correct exception
          //As of now we cannot validate the implementation of values provider during the compile time
          LOG.error(
            "The ValuesProvider implementation for configuration {} in stage {} does not have the same number of values and labels.",
            configDef.getName(), def.getName());
          throw new RuntimeException(Utils.format(
            "The ValuesProvider implementation for configuration {} in stage {} does not have the same number of values and labels.",
            configDef.getName(), def.getName()));
        }

        List<String> values = configDef.getModel().getValues();
        List<String> labels = configDef.getModel().getLabels();
        for(int i = 0; i < values.size(); i++) {
          StringBuilder sb = new StringBuilder();
          sb.append(CONFIG)
            .append(SEPERATOR)
            .append(configDef.getName())
            .append(SEPERATOR)
            .append(FIELD_MODIFIER)
            .append(SEPERATOR)
            .append(values.get(i));
          String key = sb.toString();
          String l = rb.containsKey(key) ? rb.getString(key) : labels.get(i);
          labels.set(i, l);
        }
      }
    }
    return def;
  }

  private void updateValuesAndLabelsFromValuesProvider() {
    for(ConfigDefinition configDef : getConfigDefinitions()) {
      if(configDef.getModel() != null &&
        configDef.getModel().getValuesProviderClass() != null &&
        !configDef.getModel().getValuesProviderClass().isEmpty()) {
        try {
          Class valueProviderClass = classLoader.loadClass(configDef.getModel().getValuesProviderClass());
          Object valueProvider = valueProviderClass.newInstance();
          List<String> values = (List<String>)
            valueProviderClass.getMethod("getValues").invoke(valueProvider);
          List<String> labels = (List<String>)
            valueProviderClass.getMethod("getLabels").invoke(valueProvider);

          configDef.getModel().setValues(values);
          configDef.getModel().setLabels(labels);

        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
          throw new RuntimeException(e);
        } catch (NoSuchMethodException e) {
          throw new RuntimeException(e);
        } catch (InstantiationException e) {
          throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
}
