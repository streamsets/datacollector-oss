/*
 * Copyright 2019 StreamSets Inc.
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
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.streamsets.datacollector.el.ElConstantDefinition;
import com.streamsets.datacollector.el.ElFunctionDefinition;
import com.streamsets.pipeline.api.ChooserValues;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.impl.LocalizableMessage;
import com.streamsets.pipeline.api.impl.Utils;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Captures attributes related to individual configuration options
 */
public class ConfigDefinition {
  private final Field configField;
  private final String name;
  private final ConfigDef.Type type;
  private final ConfigDef.Upload upload;
  private final String label;
  private final String description;
  private Object defaultValue;
  private final boolean required;
  private final String group;
  private final String fieldName;
  private String dependsOn;
  private List<Object> triggeredByValues;
  private final ModelDefinition model;
  private final int displayPosition;
  private List<ElFunctionDefinition> elFunctionDefinitions;
  private List<ElConstantDefinition> elConstantDefinitions;
  private List<String> elFunctionDefinitionsIdx;
  private List<String> elConstantDefinitionsIdx;
  private final long min;
  private final long max;
  private final String mode;
  private final int lines;
  private List<Class> elDefs;
  private final ConfigDef.Evaluation evaluation;
  private Map<String, List<Object>> dependsOnMap;
  private String prefix;
  private ConfigDef.DisplayMode displayMode;
  private String connectionType;

  public ConfigDefinition(String name, ConfigDef.Type type, ConfigDef.Upload upload, String label, String description,
                          Object defaultValue,
                          boolean required, String group, String fieldName, ModelDefinition model, String dependsOn,
                          List<Object> triggeredByValues, int displayPosition, List<ElFunctionDefinition> elFunctionDefinitions,
                          List<ElConstantDefinition> elConstantDefinitions, long min, long max, String mode, int lines,
                          List<Class> elDefs, ConfigDef.Evaluation evaluation, Map<String, List<Object>> dependsOnMap,
                          ConfigDef.DisplayMode displayMode, String connectionType) {
    this(null, name, type, upload, label, description, defaultValue, "",
        required, group, fieldName,
        model, dependsOn, triggeredByValues, displayPosition, elFunctionDefinitions,
        elConstantDefinitions, min, max, mode, lines, elDefs, evaluation, dependsOnMap, displayMode, connectionType);
  }

  public ConfigDefinition(Field configField, String name, ConfigDef.Type type, ConfigDef.Upload upload,
                          String label, String description,
                          Object defaultValue, String defaultValueFromResource,
                          boolean required, String group, String fieldName, ModelDefinition model, String dependsOn,
                          List<Object> triggeredByValues, int displayPosition, List<ElFunctionDefinition> elFunctionDefinitions,
                          List<ElConstantDefinition> elConstantDefinitions, long min, long max, String mode, int lines,
                          List<Class> elDefs, ConfigDef.Evaluation evaluation, Map<String, List<Object>> dependsOnMap,
                          ConfigDef.DisplayMode displayMode, String connectionType) {
    this.configField = configField;
    this.name = name;
    this.type = type;
    this.upload = upload;
    this.label = label;
    this.description = description;
    this.defaultValue = defaultValue;

    this.required = required;
    this.group = group;
    this.fieldName = fieldName;
    this.model = model;
    this.dependsOn = dependsOn;
    this.triggeredByValues = triggeredByValues;
    this.displayPosition = displayPosition;

    this.elDefs = new ArrayList<>();
    this.elDefs.addAll(elDefs);
    this.elFunctionDefinitions = new ArrayList<>();
    elFunctionDefinitionsIdx = new ArrayList<>();
    for (ElFunctionDefinition f : elFunctionDefinitions) {
      addElFunction(f);
    }
    this.elConstantDefinitions = new ArrayList<>();
    elConstantDefinitionsIdx = new ArrayList<>();
    for (ElConstantDefinition c : elConstantDefinitions) {
      addElConstant(c);
    }
    this.min = min;
    this.max = max;
    this.mode = mode;
    this.lines = lines;
    this.dependsOnMap = dependsOnMap;
    this.evaluation = evaluation;

    this.displayMode = displayMode;

    this.connectionType = connectionType;

    // if getDefaultValueFromResources is a valid resource path we'll read that into String and use it as defaultValue
    if (defaultValueFromResource != null && !defaultValueFromResource.equals("")) {
      if (defaultValue != null && !defaultValue.equals("")) {
        throw new IllegalArgumentException(
            String.format(
                "Can't use both defaultValue and defaultValueFromResources in the same ConfigDef:%n" +
                    "defaultValue=%s, defaultValueFromResources=%s",
                defaultValue,
                defaultValueFromResource
            ));
      }
      if (type != ConfigDef.Type.STRING && type != ConfigDef.Type.TEXT) {
        throw new IllegalArgumentException(
            String.format(
                "Only STRING or TEXT types can have default value read in from resources:%n" +
                    "type=%s, defaultValueFromResources=%s",
                type,
                defaultValueFromResource
                ));
      }
      if (configField == null) {
        throw new IllegalArgumentException(
            String.format(
                "Cannot read default value from resources when configField is null (don't know which resources):%n" +
                    "configField=%s, defaultValueFromResources=%s",
                configField,
                defaultValueFromResource
            ));
      }
      String resourcePath = defaultValueFromResource;
      try {
        Class declaringClass = configField.getDeclaringClass();
        URL url = Resources.getResource(declaringClass, resourcePath);
        this.defaultValue = Resources.toString(url, Charsets.UTF_8);
      } catch (IOException | NullPointerException e) {
        throw new IllegalArgumentException(
            String.format(
                "Can't load resource from %s. Make sure its a relative path in \\resources of the same package.%n%s",
                resourcePath,
                e.getMessage()
            )
        );
      }
    }
  }

  public void addAutoELDefinitions(StageLibraryDefinition libraryDef) {
    if (this.getType() != ConfigDef.Type.MODEL || this.getModel().getModelType() != ModelType.LIST_BEAN) {

      for (Class klass : libraryDef.getElDefs()) {
        addElDef(klass);
      }

      for (ElFunctionDefinition f : libraryDef.getElFunctionDefinitions()) {
        addElFunction(f);
      }

      for (ElConstantDefinition c : libraryDef.getElConstantDefinitions()) {
        addElConstant(c);
      }

    } else {
      for (ConfigDefinition configDef : this.getModel().getConfigDefinitions()) {
        configDef.addAutoELDefinitions(libraryDef);
      }
    }
  }

  private void addElDef(Class klass) {
    if (!elDefs.contains(klass)) {
      elDefs.add(klass);
    }
  }

  private void addElFunction(ElFunctionDefinition elF) {
    if (!elFunctionDefinitionsIdx.contains(elF.getIndex())) {
      elFunctionDefinitions.add(elF);
      elFunctionDefinitionsIdx.add(elF.getIndex());
    }
  }

  private void addElConstant(ElConstantDefinition elC) {
    if (!elConstantDefinitionsIdx.contains(elC.getIndex())) {
      elConstantDefinitions.add(elC);
      elConstantDefinitionsIdx.add(elC.getIndex());
    }
  }

  public Field getConfigField() {
    return configField;
  }

  public String getName() {
    return name;
  }

  public ConfigDef.Type getType() {
    return type;
  }

  public ConfigDef.Upload getUpload() {
    return upload;
  }

  public String getLabel() {
    return label;
  }

  public String getDescription() {
    return description;
  }

  public Object getDefaultValue() {
    return defaultValue;
  }

  public boolean isRequired() {
    return required;
  }

  public String getGroup() { return group; }

  public ModelDefinition getModel() {
    return model;
  }

  public String getFieldName() {
    return fieldName;
  }

  public void setDependsOn(String dependsOn) {
    this.dependsOn = dependsOn;
  }
  public String getDependsOn() {
    return dependsOn;
  }

  public long getMin() {
    return min;
  }

  public long getMax() {
    return max;
  }

  public String getMode() {
    return mode;
  }

  public int getLines() {
    return lines;
  }

  @JsonIgnore
  public List<Class> getElDefs() {
    return elDefs;
  }

  public List<Object> getTriggeredByValues() {
    return triggeredByValues;
  }

  public void setTriggeredByValues(List<Object> triggeredByValues) {
    this.triggeredByValues = triggeredByValues;
  }

  public List<ElFunctionDefinition> getElFunctionDefinitions() {
    return elFunctionDefinitions;
  }

  public List<ElConstantDefinition> getElConstantDefinitions() {
    return elConstantDefinitions;
  }

  public List<String> getElFunctionDefinitionsIdx() {
    return elFunctionDefinitionsIdx;
  }

  public List<String> getElConstantDefinitionsIdx() {
    return elConstantDefinitionsIdx;
  }

  public int getDisplayPosition() {
    return displayPosition;
  }

  public Map<String, List<Object>> getDependsOnMap() {
    return dependsOnMap;
  }

  public void setDependsOnMap(Map<String, List<Object>> dependsOnMap) {
    this.dependsOnMap = dependsOnMap;
  }

  public ConfigDef.Evaluation getEvaluation() {
    return evaluation;
  }

  public String getConnectionType() {
    return connectionType;
  }

  public ConfigDefinition localize(ClassLoader classLoader, String bundle) {
    String labelKey = "configLabel." + getName();
    String descriptionKey = "configDescription." + getName();

    // config label & description
    String label = new LocalizableMessage(classLoader, bundle, labelKey, getLabel(), null).getLocalized();
    String description = new LocalizableMessage(classLoader, bundle, descriptionKey, getDescription(), null)
        .getLocalized();

    // config model
    ModelDefinition model = getModel();
    if(getType() == ConfigDef.Type.MODEL) {
      switch (model.getModelType()) {
        case VALUE_CHOOSER:
        case MULTI_VALUE_CHOOSER:
          try {
            Class klass = classLoader.loadClass(model.getValuesProviderClass());
            ChooserValues chooserValues = (ChooserValues) klass.newInstance();
            List<String> values = chooserValues.getValues();
            if (values != null) {
              List<String> localizedValueChooserLabels = new ArrayList<>(chooserValues.getLabels());
              String rbName = chooserValues.getResourceBundle();
              if (rbName != null) {
                for (int i = 0; i < values.size(); i++) {
                  String l = new LocalizableMessage(classLoader, rbName, values.get(i),
                                                    localizedValueChooserLabels.get(i), null).getLocalized();
                  localizedValueChooserLabels.set(i, l);
                }
              }
              model = ModelDefinition.localizedValueChooser(model, values, localizedValueChooserLabels);
            }
          } catch (Exception ex) {
            throw new RuntimeException(Utils.format("Could not extract localization info from '{}': {}",
                                                    model.getValuesProviderClass(), ex.toString()), ex);
          }
          break;
        case LIST_BEAN:
          List<ConfigDefinition> listBean = model.getConfigDefinitions();
          List<ConfigDefinition> listBeanLocalize = new ArrayList<>(listBean.size());
          for (ConfigDefinition def : listBean) {
            listBeanLocalize.add(def.localize(classLoader, bundle));
          }
          model = ModelDefinition.localizedComplexField(model, listBeanLocalize);
          break;
        default:
          break;
      }
    }

    return new ConfigDefinition(getConfigField(), getName(), getType(), getUpload(), label, description, getDefaultValue(),
                                "", isRequired(), getGroup(), getFieldName(), model,
                                getDependsOn(), getTriggeredByValues(), getDisplayPosition(),
                                getElFunctionDefinitions(), getElConstantDefinitions(), getMin(),
                                getMax(), getMode(), getLines(), getElDefs(), getEvaluation(), getDependsOnMap(),
                                getDisplayMode(), getConnectionType()
    );
  }

  public String getPrefix() {
    return prefix;
  }

  public void setPrefix(String prefix) {
    this.prefix = prefix;
  }

  public ConfigDef.DisplayMode getDisplayMode() {
    return displayMode;
  }

  @Override
  public String toString() {
    return Utils.format("ConfigDefinition[name='{}' type='{}' required='{}' default='{}']", getName(), getType(),
                        isRequired(), getDefaultValue());
  }

  /**
   * Only checks if the fields have the same name. Do not compare against various stages!!
   * @param o
   * @return
   */
  @Override
  public boolean equals(Object o) {
    if (o == null || !(o instanceof ConfigDefinition)) {
      return false;
    }
    return this.fieldName.equals(((ConfigDefinition) o).fieldName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fieldName);
  }
}
