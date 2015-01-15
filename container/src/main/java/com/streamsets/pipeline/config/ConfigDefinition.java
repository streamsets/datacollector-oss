/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.impl.LocalizableMessage;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Captures attributes related to individual configuration options
 */
public class ConfigDefinition {

  public static final String REQUIRED_FIELDS = "stageRequiredFields";

  // we are not using Guava ImmutableSet.of() because this breaks the annotation processor
  public static final Set<String> SYSTEM_CONFIGS = new HashSet<>(Arrays.asList(REQUIRED_FIELDS));
  private final String name;
  private final ConfigDef.Type type;
  private final String label;
  private final String description;
  private final Object defaultValue;
  private final boolean required;
  private final String group;
  private final String fieldName;
  private final String dependsOn;
  private final String[] triggeredByValues;
  private final ModelDefinition model;
  private final int displayPosition;

  @JsonCreator
  public ConfigDefinition(
      @JsonProperty("name") String name,
      @JsonProperty("type") ConfigDef.Type type,
      @JsonProperty("label") String label,
      @JsonProperty("description") String description,
      @JsonProperty("default") Object defaultValue,
      @JsonProperty("required") boolean required,
      @JsonProperty("group") String group,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("model") ModelDefinition model,
      @JsonProperty("dependsOn") String dependsOn,
      @JsonProperty("triggeredByValues") String[] triggeredByValues,
      @JsonProperty("displayPosition") int displayPosition) {
    this.name = name;
    this.type = type;
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
  }

  public String getName() {
    return name;
  }

  public ConfigDef.Type getType() {
    return type;
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

  public String getDependsOn() {
    return dependsOn;
  }

  public String[] getTriggeredByValues() {
    return triggeredByValues;
  }

  public int getDisplayPosition() {
    return displayPosition;
  }

  private final static String CONFIG_LABEL = "{}.label";
  private final static String CONFIG_DESCRIPTION = "{}.description";

  public ConfigDefinition localize(ClassLoader classLoader, String bundle) {
    String labelKey = Utils.format(CONFIG_LABEL, getName());
    String descriptionKey = Utils.format(CONFIG_DESCRIPTION, getName());

    String label = new LocalizableMessage(classLoader, bundle, labelKey, getLabel(), null).
        getLocalized();
    String description = new LocalizableMessage(classLoader, bundle, descriptionKey, getDescription(), null)
        .getLocalized();

    return new ConfigDefinition(getName(), getType(), label, description, getDefaultValue(),
      isRequired(), getGroup(), getFieldName(), getModel(), getDependsOn(), getTriggeredByValues(),
      getDisplayPosition());
  }

  @Override
  public String toString() {
    return Utils.format("ConfigDefinition[name='{}' type='{}' required='{}' default='{}']", getName(), getType(),
                        isRequired(), getDefaultValue());
  }

}
