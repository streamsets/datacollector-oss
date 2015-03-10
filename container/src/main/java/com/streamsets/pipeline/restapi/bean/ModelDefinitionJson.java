/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi.bean;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.List;

public class ModelDefinitionJson {

  private final com.streamsets.pipeline.config.ModelDefinition modelDefinition;

  @JsonCreator
  public ModelDefinitionJson(
    @JsonProperty("modelType") ModelTypeJson modelTypeJson,
    @JsonProperty("valuesProviderClass") String valuesProviderClass,
    @JsonProperty("values") List<String> values,
    @JsonProperty("labels") List<String> labels,
    @JsonProperty("configDefinitions") List<ConfigDefinitionJson> configDefinitionJsons) {
    this.modelDefinition = new com.streamsets.pipeline.config.ModelDefinition(BeanHelper.unwrapModelType(modelTypeJson),
                                                                              valuesProviderClass, values, labels,
      BeanHelper.unwrapConfigDefinitions(configDefinitionJsons));
  }

  public ModelDefinitionJson(com.streamsets.pipeline.config.ModelDefinition modelDefinition) {
    Utils.checkNotNull(modelDefinition, "modelDefinition");
    this.modelDefinition = modelDefinition;
  }

  public ModelTypeJson getModelType() {
    return BeanHelper.wrapModelType(modelDefinition.getModelType());
  }

  public List<String> getValues() {
    return modelDefinition.getValues();
  }

  public List<String> getLabels() {
    return modelDefinition.getLabels();
  }

  public String getValuesProviderClass() {
    return modelDefinition.getValuesProviderClass();
  }

  public List<ConfigDefinitionJson> getConfigDefinitions() {
    return BeanHelper.wrapConfigDefinitions(modelDefinition.getConfigDefinitions());
  }

  @JsonIgnore
  public com.streamsets.pipeline.config.ModelDefinition getModelDefinition() {
    return modelDefinition;
  }
}