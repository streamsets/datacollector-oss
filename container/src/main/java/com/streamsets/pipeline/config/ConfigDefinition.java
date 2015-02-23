/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.impl.LocalizableMessage;
import com.streamsets.pipeline.api.impl.Utils;

/**
 * Captures attributes related to individual configuration options
 */
public class ConfigDefinition {

  public static final String REQUIRED_FIELDS = "stageRequiredFields";
  public static final String ON_RECORD_ERROR = "stageOnRecordError";

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

  public ConfigDefinition(String name, ConfigDef.Type type, String label, String description, Object defaultValue,
      boolean required, String group, String fieldName, ModelDefinition model, String dependsOn,
      String[] triggeredByValues, int displayPosition) {
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
