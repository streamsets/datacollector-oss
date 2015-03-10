/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.streamsets.pipeline.api.impl.Utils;

import java.util.List;

public class ModelDefinition {
  private final ModelType modelType;
  private final String valuesProviderClass;
  private final List<ConfigDefinition> configDefinitions;
  private List<String> values;
  private List<String> labels;

  public ModelDefinition(ModelType modelType, String valuesProviderClass, List<String> values,
      List<String> labels, List<ConfigDefinition> configDefinitions) {
    this.modelType = modelType;
    this.valuesProviderClass = valuesProviderClass;
    this.configDefinitions = configDefinitions;
    this.values = values;
    this.labels = labels;
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

  public List<ConfigDefinition> getConfigDefinitions() {
    return configDefinitions;
  }

  @Override
  public String toString() {
    return Utils.format("ModelDefinition[type='{}' valuesProviderClass='{}' values='{}']", getModelType(), getValues(),
                        getValuesProviderClass());
  }

}
