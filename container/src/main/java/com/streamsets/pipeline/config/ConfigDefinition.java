/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.ChooserValues;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.impl.LocalizableMessage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.el.ElConstantDefinition;
import com.streamsets.pipeline.el.ElFunctionDefinition;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Captures attributes related to individual configuration options
 */
public class ConfigDefinition {

  public static final String REQUIRED_FIELDS = "stageRequiredFields";
  public static final String ON_RECORD_ERROR = "stageOnRecordError";
  public static final String PRECONDITIONS = "stageRecordPreconditions";
  public static final Set<String> SYSTEM_CONFIGS = ImmutableSet.of(REQUIRED_FIELDS, PRECONDITIONS, ON_RECORD_ERROR);

  private final Field configField;
  private final String name;
  private final ConfigDef.Type type;
  private final String label;
  private final String description;
  private Object defaultValue;
  private final boolean required;
  private final String group;
  private final String fieldName;
  private final String dependsOn;
  private List<Object> triggeredByValues;
  private final ModelDefinition model;
  private final int displayPosition;
  private final List<ElFunctionDefinition> elFunctionDefinitions;
  private final List<ElConstantDefinition> elConstantDefinitions;
  private final long min;
  private final long max;
  private final String mode;
  private final int lines;
  private final List<Class> elDefs;
  private final ConfigDef.Evaluation evaluation;
  private Map<String, List<Object>> dependsOnMap;

  public ConfigDefinition(String name, ConfigDef.Type type, String label, String description,
      Object defaultValue,
      boolean required, String group, String fieldName, ModelDefinition model, String dependsOn,
      List<Object> triggeredByValues, int displayPosition, List<ElFunctionDefinition> elFunctionDefinitions,
      List<ElConstantDefinition> elConstantDefinitions, long min, long max, String mode, int lines,
      List<Class> elDefs, ConfigDef.Evaluation evaluation, Map<String, List<Object>> dependsOnMap) {
    this(null, name, type, label, description, defaultValue, required, group, fieldName, model,
         dependsOn, triggeredByValues, displayPosition, elFunctionDefinitions,
         elConstantDefinitions, min, max, mode, lines, elDefs, evaluation, dependsOnMap);
  }

  public ConfigDefinition(Field configField, String name, ConfigDef.Type type, String label, String description,
      Object defaultValue,
      boolean required, String group, String fieldName, ModelDefinition model, String dependsOn,
      List<Object> triggeredByValues, int displayPosition, List<ElFunctionDefinition> elFunctionDefinitions,
      List<ElConstantDefinition> elConstantDefinitions, long min, long max, String mode, int lines,
      List<Class> elDefs, ConfigDef.Evaluation evaluation, Map<String, List<Object>> dependsOnMap) {
    this.configField = configField;
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
    this.elFunctionDefinitions = elFunctionDefinitions;
    this.elConstantDefinitions = elConstantDefinitions;
    this.min = min;
    this.max = max;
    this.mode = mode;
    this.lines = lines;
    this.elDefs = elDefs;
    this.dependsOnMap = dependsOnMap;
    this.evaluation = evaluation;
  }

  public Field getConfigField() {
    return configField;
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

  public long getMin() {
    return min;
  }

  public long getMax() {
    return max;
  }

  public String getMode() {
    return mode;
  }

  public int getLines() {
    return lines;
  }

  public List<Class> getElDefs() {
    return elDefs;
  }

  public List<Object> getTriggeredByValues() {
    return triggeredByValues;
  }

  public void setTriggeredByValues(List<Object> triggeredByValues) {
    this.triggeredByValues = triggeredByValues;
  }

  public List<ElFunctionDefinition> getElFunctionDefinitions() {
    return elFunctionDefinitions;
  }

  public List<ElConstantDefinition> getElConstantDefinitions() {
    return elConstantDefinitions;
  }

  public int getDisplayPosition() {
    return displayPosition;
  }

  public void setDefaultValue(Object defaultValue) {
    this.defaultValue = defaultValue;
  }

  public Map<String, List<Object>> getDependsOnMap() {
    return dependsOnMap;
  }

  public void setDependsOnMap(Map<String, List<Object>> dependsOnMap) {
    this.dependsOnMap = dependsOnMap;
  }

  public ConfigDef.Evaluation getEvaluation() {
    return evaluation;
  }

  public ConfigDefinition localize(ClassLoader classLoader, String bundle) {
    String labelKey = "configLabel." + getName();
    String descriptionKey = "configDescription." + getName();

    // config label & description
    String label = new LocalizableMessage(classLoader, bundle, labelKey, getLabel(), null).getLocalized();
    String description = new LocalizableMessage(classLoader, bundle, descriptionKey, getDescription(), null)
        .getLocalized();

    // config model
    ModelDefinition model = getModel();
    if(getType() == ConfigDef.Type.MODEL) {
      switch (model.getModelType()) {
        case VALUE_CHOOSER:
          try {
            Class klass = classLoader.loadClass(model.getValuesProviderClass());
            ChooserValues chooserValues = (ChooserValues) klass.newInstance();
            List<String> values = chooserValues.getValues();
            if (values != null) {
              List<String> localizedValueChooserLabels = new ArrayList<>(chooserValues.getLabels());
              String rbName = chooserValues.getResourceBundle();
              if (rbName != null) {
                for (int i = 0; i < values.size(); i++) {
                  String l = new LocalizableMessage(classLoader, rbName, values.get(i),
                                                    localizedValueChooserLabels.get(i), null).getLocalized();
                  localizedValueChooserLabels.set(i, l);
                }
              }
              model = ModelDefinition.localizedValueChooser(model, values, localizedValueChooserLabels);
            }
          } catch (Exception ex) {
            throw new RuntimeException(Utils.format("Could not extract localization info from '{}': {}",
                                                    model.getValuesProviderClass(), ex.getMessage(), ex));
          }
          break;
        case COMPLEX_FIELD:
          List<ConfigDefinition> complexField = model.getConfigDefinitions();
          List<ConfigDefinition> complexFieldLocalized = new ArrayList<>(complexField.size());
          for (ConfigDefinition def : complexField) {
            complexFieldLocalized.add(def.localize(classLoader, bundle));
          }
          model = ModelDefinition.localizedComplexField(model, complexFieldLocalized);
          break;
      }
    }

    return new ConfigDefinition(getConfigField(), getName(), getType(), label, description, getDefaultValue(),
                                isRequired(), getGroup(), getFieldName(), model, getDependsOn(), getTriggeredByValues(),
                                getDisplayPosition(), getElFunctionDefinitions(), getElConstantDefinitions(), getMin(),
                                getMax(), getMode(), getLines(), getElDefs(), getEvaluation(), getDependsOnMap());
  }

  @Override
  public String toString() {
    return Utils.format("ConfigDefinition[name='{}' type='{}' required='{}' default='{}']", getName(), getType(),
                        isRequired(), getDefaultValue());
  }

}
