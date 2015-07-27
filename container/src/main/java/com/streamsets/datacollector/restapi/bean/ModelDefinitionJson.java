/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.restapi.bean;

import com.streamsets.pipeline.api.impl.Utils;

import java.util.List;

public class ModelDefinitionJson {

  private final com.streamsets.datacollector.config.ModelDefinition modelDefinition;

  public ModelDefinitionJson(com.streamsets.datacollector.config.ModelDefinition modelDefinition) {
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

}