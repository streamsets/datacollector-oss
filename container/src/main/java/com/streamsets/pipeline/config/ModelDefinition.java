/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.List;

public class ModelDefinition {
  private final ModelType modelType;
  private final ChooserMode chooserMode;
  private final String valuesProviderClass;
  private List<String> values;
  private List<String> labels;

  @JsonCreator
  public ModelDefinition(
    @JsonProperty("modelType") ModelType modelType,
    @JsonProperty("chooserMode") ChooserMode chooserMode,
    @JsonProperty("valuesProviderClass")String valuesProviderClass,
    @JsonProperty("values") List<String> values,
    @JsonProperty("labels") List<String> labels) {
    this.modelType = modelType;
    this.chooserMode = chooserMode;
    this.valuesProviderClass = valuesProviderClass;
    this.values = values;
    this.labels = labels;
  }

  public ModelType getModelType() {
    return modelType;
  }

  public ChooserMode getChooserMode() {
    return chooserMode;
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

  @Override
  public String toString() {
    return Utils.format("ModelDefinition[type='{}' valuesProviderClass='{}' values='{}']", getModelType(), getValues(),
                        getValuesProviderClass());
  }

}
