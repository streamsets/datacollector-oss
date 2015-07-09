/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.streamsets.pipeline.api.impl.Utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ModelDefinition {
  private final ModelType modelType;
  private final String valuesProviderClass;
  private final List<ConfigDefinition> configDefinitions;
  private final Map<String, ConfigDefinition> configDefinitionsAsMap;
  private List<String> values;
  private List<String> labels;
  private final Class complexFieldClass;

  public static ModelDefinition localizedValueChooser(ModelDefinition model, List<String> values, List<String> labels) {
    return new ModelDefinition(model.getModelType(), model.getValuesProviderClass(), values, labels,
                        model.getComplexFieldClass(), model.getConfigDefinitions());
  }

  public static ModelDefinition localizedComplexField(ModelDefinition model, List<ConfigDefinition> configDefs) {
    return new ModelDefinition(model.getModelType(), model.getValuesProviderClass(), model.getValues(),
                               model.getLabels(), model.getComplexFieldClass(), configDefs);
  }

  public ModelDefinition(ModelType modelType, String valuesProviderClass, List<String> values,
      List<String> labels, Class complexFieldClass, List<ConfigDefinition> configDefinitions) {
    this.modelType = modelType;
    this.valuesProviderClass = valuesProviderClass;
    this.configDefinitions = configDefinitions;
    configDefinitionsAsMap = new HashMap<>();
    if (configDefinitions != null) {
      for (ConfigDefinition def : configDefinitions) {
        configDefinitionsAsMap.put(def.getName(), def);
      }
    }
    this.values = values;
    this.labels = labels;
    this.complexFieldClass = complexFieldClass;
  }

  public ModelType getModelType() {
    return modelType;
  }

  public List<String> getValues() {
    return values;
  }

  public List<String> getLabels() {
    return labels;
  }

  public String getValuesProviderClass() {
    return valuesProviderClass;
  }

  public void setValues(List<String> values) {
    this.values = values;
  }

  public void setLabels(List<String> labels) {
    this.labels = labels;
  }

  public Class getComplexFieldClass() {
    return complexFieldClass;
  }

  public List<ConfigDefinition> getConfigDefinitions() {
    return configDefinitions;
  }

  public Map<String, ConfigDefinition> getConfigDefinitionsAsMap() {
    return configDefinitionsAsMap;
  }

  @Override
  public String toString() {
    return Utils.format("ModelDefinition[type='{}' valuesProviderClass='{}' values='{}']", getModelType(), getValues(),
                        getValuesProviderClass());
  }

}
