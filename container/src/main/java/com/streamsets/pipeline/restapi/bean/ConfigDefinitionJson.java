/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi.bean;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.impl.Utils;

/**
 * Captures attributes related to individual configuration options
 */
public class ConfigDefinitionJson {

  private final com.streamsets.pipeline.config.ConfigDefinition configDefinition;

  @JsonCreator
  public ConfigDefinitionJson(
    @JsonProperty("name") String name,
    @JsonProperty("type") ConfigDef.Type type,
    @JsonProperty("label") String label,
    @JsonProperty("description") String description,
    @JsonProperty("defaultValue") Object defaultValue,
    @JsonProperty("required") boolean required,
    @JsonProperty("group") String group,
    @JsonProperty("fieldName") String fieldName,
    @JsonProperty("model") ModelDefinitionJson model,
    @JsonProperty("dependsOn") String dependsOn,
    @JsonProperty("triggeredByValues") String[] triggeredByValues,
    @JsonProperty("displayPosition") int displayPosition) {
    this.configDefinition = new com.streamsets.pipeline.config.ConfigDefinition(name, type, label, description,
      defaultValue, required, group, fieldName, BeanHelper.unwrapModelDefinition(model), dependsOn, triggeredByValues,
      displayPosition);
  }

  public ConfigDefinitionJson(com.streamsets.pipeline.config.ConfigDefinition configDefinition) {
    Utils.checkNotNull(configDefinition, "configDefinition");
    this.configDefinition = configDefinition;
  }

  public String getName() {
    return configDefinition.getName();
  }

  public ConfigDef.Type getType() {
    return configDefinition.getType();
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

  public String[] getTriggeredByValues() {
    return configDefinition.getTriggeredByValues();
  }

  public int getDisplayPosition() {
    return configDefinition.getDisplayPosition();
  }

  @JsonIgnore
  public com.streamsets.pipeline.config.ConfigDefinition getConfigDefinition() {
    return configDefinition;
  }
}