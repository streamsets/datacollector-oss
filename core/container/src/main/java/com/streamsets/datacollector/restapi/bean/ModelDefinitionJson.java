/*
 * Copyright 2017 StreamSets Inc.
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

  public String getFilteringConfig() {
    return modelDefinition.getFilteringConfig();
  }

}
