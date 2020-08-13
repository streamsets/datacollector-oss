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

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.List;
import java.util.Map;

/**
 * Captures attributes related to individual configuration options
 */
public class ConfigDefinitionJson {

  private final com.streamsets.datacollector.config.ConfigDefinition configDefinition;

  public ConfigDefinitionJson(com.streamsets.datacollector.config.ConfigDefinition configDefinition) {
    Utils.checkNotNull(configDefinition, "configDefinition");
    this.configDefinition = configDefinition;
  }

  public String getName() {
    return configDefinition.getName();
  }

  public ConfigDef.Type getType() {
    return configDefinition.getType();
  }

  public ConfigDef.Upload getUpload() {
    return configDefinition.getUpload();
  }

  public String getLabel() {
    return configDefinition.getLabel();
  }

  public String getDescription() {
    return configDefinition.getDescription();
  }

  public Object getDefaultValue() {
    return configDefinition.getDefaultValue();
  }

  public boolean isRequired() {
    return configDefinition.isRequired();
  }

  public String getGroup() { return configDefinition.getGroup(); }

  public ModelDefinitionJson getModel() {
    return BeanHelper.wrapModelDefinition(configDefinition.getModel());
  }

  public String getFieldName() {
    return configDefinition.getFieldName();
  }

  public String getDependsOn() {
    return configDefinition.getDependsOn();
  }

  public List<Object> getTriggeredByValues() {
    return configDefinition.getTriggeredByValues();
  }

  public int getDisplayPosition() {
    return configDefinition.getDisplayPosition();
  }

  public List<String> getElFunctionDefinitionsIdx() {
    return configDefinition.getElFunctionDefinitionsIdx();
  }

  public List<String> getElConstantDefinitionsIdx() {
    return configDefinition.getElConstantDefinitionsIdx();
  }

  public long getMin() {
    return configDefinition.getMin();
  }

  public long getMax() {
    return configDefinition.getMax();
  }

  public String getMode() {
    return configDefinition.getMode();
  }

  public int getLines() {
    return configDefinition.getLines();
  }

  public List<String> getElDefs() {
    return null;
  }

  public ConfigDef.Evaluation getEvaluation() {
    return this.configDefinition.getEvaluation();
  }

  public Map<String, List<Object>> getDependsOnMap() {
    return configDefinition.getDependsOnMap();
  }

  public ConfigDef.DisplayMode getDisplayMode() {
    return configDefinition.getDisplayMode();
  }

  public String getConnectionType() {
    return configDefinition.getConnectionType();
  }

}
