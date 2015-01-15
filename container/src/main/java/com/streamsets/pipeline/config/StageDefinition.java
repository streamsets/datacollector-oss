/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.pipeline.api.ChooserValues;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.impl.LocalizableMessage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stagelibrary.StageLibraryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Captures the configuration options for a {@link com.streamsets.pipeline.api.Stage}.
 *
 */
public class StageDefinition {
  private static final Logger LOG = LoggerFactory.getLogger(StageDefinition.class);

  private static final String SEPARATOR = ".";
  private static final String FIELD_VALUE_CHOOSER = "FieldValueChooser";

  private String library;
  private String libraryLabel;
  private ClassLoader classLoader;
  private Class klass;

  private final String className;
  private final String name;
  private final String version;
  private final String label;
  private final String description;
  private final StageType type;
  private final RawSourceDefinition rawSourceDefinition;
  private List<ConfigDefinition> configDefinitions;
  private Map<String, ConfigDefinition> configDefinitionsMap;
  private final String icon;
  private final ConfigGroupDefinition configGroupDefinition;


  @JsonCreator
  public StageDefinition(
    @JsonProperty("className") String className,
    @JsonProperty("name") String name,
    @JsonProperty("version") String version,
    @JsonProperty("label") String label,
    @JsonProperty("description") String description,
    @JsonProperty("type") StageType type,
    @JsonProperty("configDefinitions") List<ConfigDefinition> configDefinitions,
    @JsonProperty("rawSourceDefinition") RawSourceDefinition rawSourceDefinition,
    @JsonProperty("icon") String icon,
    @JsonProperty("configGroupDefinition") ConfigGroupDefinition configGroupDefinition) {
    this.className = className;
    this.name = name;
    this.version = version;
    this.label = label;
    this.description = description;
    this.type = type;
    this.configDefinitions = configDefinitions;
    this.rawSourceDefinition = rawSourceDefinition;
    configDefinitionsMap = new HashMap<>();
    for (ConfigDefinition conf : configDefinitions) {
      configDefinitionsMap.put(conf.getName(), conf);
    }
    this.icon = icon;
    this.configGroupDefinition = configGroupDefinition;
  }

  public void setLibrary(String library, String label, ClassLoader classLoader) {
    this.library = library;
    this.libraryLabel = label;
    this.classLoader = classLoader;
    try {
      klass = classLoader.loadClass(getClassName());
    } catch (ClassNotFoundException ex) {
      throw new RuntimeException(ex);
    }
    updateValuesAndLabelsFromValuesProvider();
    updateGroupLabels();
  }

  public ConfigGroupDefinition getConfigGroupDefinition() {
    return configGroupDefinition;
  }

  public String getLibrary() {
    return library;
  }

  public String getLibraryLabel() {
    return libraryLabel;
  }

  @JsonIgnore
  public ClassLoader getStageClassLoader() {
    return classLoader;
  }

  public String getClassName() {
    return className;
  }

  @JsonIgnore
  public String getBundle() {
    return className;
  }

  @JsonIgnore
  public Class getStageClass() {
    return klass;
  }

  public String getName() {
    return name;
  }

  public String getVersion() {
    return version;
  }

  public String getLabel() {
    return label;
  }

  public RawSourceDefinition getRawSourceDefinition() {
    return rawSourceDefinition;
  }

  public String getDescription() {
    return description;
  }

  public StageType getType() {
    return type;
  }

  public void addConfiguration(ConfigDefinition confDef) {
    if (configDefinitionsMap.containsKey(confDef.getName())) {
      throw new IllegalArgumentException(Utils.format("Stage '{}:{}:{}', configuration definition '{}' already exists",
                                                       getLibrary(), getName(), getVersion(), confDef.getName()));
    }
    configDefinitionsMap.put(confDef.getName(), confDef);
    configDefinitions.add(confDef);
  }

  public List<ConfigDefinition> getConfigDefinitions() {
    return configDefinitions;
  }

  public ConfigDefinition getConfigDefinition(String configName) {
    return configDefinitionsMap.get(configName);
  }

  @Override
  public String toString() {
    return Utils.format("StageDefinition[library='{}' name='{}' version='{}' type='{}' class='{}']", getLibrary(),
                        getName(), getVersion(), getType(), getStageClass());
  }

  public String getIcon() {
    return icon;
  }

  private final static String STAGE_LABEL = "label";
  private final static String STAGE_DESCRIPTION = "description";

  public StageDefinition localize() {
    String rbName = getClassName();
    List<ConfigDefinition> configDefs = new ArrayList<>();
    for (ConfigDefinition configDef : getConfigDefinitions()) {
      configDefs.add(configDef.localize(classLoader, rbName));
    }

    //Localize RawSourceDefinition instance which contains ConfigDefinitions
    List<ConfigDefinition> rawSourcePreviewConfigDefs = new ArrayList<>();
    RawSourceDefinition rawSourceDef = getRawSourceDefinition();
    RawSourceDefinition rsd = null;
    if(rawSourceDef != null) {
      for (ConfigDefinition configDef : rawSourceDef.getConfigDefinitions()) {
        rawSourcePreviewConfigDefs.add(configDef.localize(classLoader, rbName));
      }
      rsd = new RawSourceDefinition(rawSourceDef.getRawSourcePreviewerClass(), rawSourceDef.getMimeType(),
          rawSourcePreviewConfigDefs);
    }

    String label = new LocalizableMessage(classLoader, rbName, STAGE_LABEL, getLabel(), null).getLocalized();
    String description = new LocalizableMessage(classLoader, rbName, STAGE_DESCRIPTION, getDescription(), null).
        getLocalized();

    String libraryLabel = StageLibraryUtils.getLibraryLabel(classLoader);

    ConfigGroupDefinition configGroupDefinition = getConfigGroupDefinition();
    ConfigGroupDefinition localizedConfGroupDef = null;
    //localize group names
    if(configGroupDefinition != null) {
      Map<String, String> localizedGroups = new HashMap<>();
      for (Map.Entry<String, String> group : getConfigGroupDefinition().getGroupNameToLabelMap().entrySet()) {
        localizedGroups.put(group.getKey(),
          new LocalizableMessage(classLoader, rbName, group.getKey(), group.getValue(), null).getLocalized());
      }
      localizedConfGroupDef = new ConfigGroupDefinition(configGroupDefinition.getClassNameToGroupsMap(),
        localizedGroups);
    }

    StageDefinition def = new StageDefinition(
      getClassName(), getName(), getVersion(), label, description,
      getType(), configDefs, rsd, getIcon(), localizedConfGroupDef);
    def.setLibrary(getLibrary(), libraryLabel, classLoader);

    for(ConfigDefinition configDef : def.getConfigDefinitions()) {
      if(configDef.getModel() != null &&
        configDef.getModel().getValues() != null &&
        !configDef.getModel().getValues().isEmpty()) {
        List<String> values = configDef.getModel().getValues();
        List<String> labels = configDef.getModel().getLabels();
        List<String> localizedLabels = new ArrayList<>(values.size());
        for(int i = 0; i < values.size(); i++) {
          String key = configDef.getName() + SEPARATOR + configDef.getModel().getModelType().name() + SEPARATOR +
            values.get(i);
          String l = new LocalizableMessage(classLoader, rbName, key, labels.get(i), null).getLocalized();
          localizedLabels.add(l);
        }
        configDef.getModel().setLabels(localizedLabels);
      }
    }

    return def;
  }

  private void updateValuesAndLabelsFromValuesProvider() {
    for(ConfigDefinition configDef : getConfigDefinitions()) {
      updateValuesAndLabels(configDef);
    }
  }

  private void updateValuesAndLabels(ConfigDefinition configDef) {
    if(configDef.getModel() != null) {
      if(configDef.getModel().getValuesProviderClass() != null &&
        !configDef.getModel().getValuesProviderClass().isEmpty()) {
        setValuesAndLabels(configDef);
      }
      if (configDef.getModel().getConfigDefinitions() != null) {
        for (ConfigDefinition complexFeldConfDef : configDef.getModel().getConfigDefinitions()) {
          //complex fields cannot have more complex fields as of now
          updateValuesAndLabels(complexFeldConfDef);
        }
      }
    }
  }

  private void setValuesAndLabels(ConfigDefinition configDef) {
    try {
      Class valueProviderClass = classLoader.loadClass(configDef.getModel().getValuesProviderClass());
      ChooserValues valueProvider = (ChooserValues) valueProviderClass.newInstance();
      List<String> values = valueProvider.getValues();
      List<String> labels = valueProvider.getLabels();

      if(values != null && labels != null && values.size() != labels.size()) {
        LOG.error(
          "The ChooserValues implementation for configuration '{}' in stage '{}' does not have the same number of "
            + "values and labels. Values '{}], labels '{}'.",
          configDef.getFieldName(), getStageClass().getName(), values, labels);
        throw new RuntimeException(Utils.format("The ChooserValues implementation for configuration '{}' in stage "
            + "'{}' does not have the same number of values and labels. Values '{}], labels '{}'.",
          configDef.getFieldName(), getStageClass().getName(), values, labels));
      }
      configDef.getModel().setValues(values);
      configDef.getModel().setLabels(labels);
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  private void updateGroupLabels() {
    ConfigGroupDefinition configGroupDefinition = getConfigGroupDefinition();
    if(configGroupDefinition != null) {
      Map<String, List<String>> classNameToGroupsMap = configGroupDefinition.getClassNameToGroupsMap();
      Map<String, String> groupNameToLabelMap = configGroupDefinition.getGroupNameToLabelMap();

      for (Map.Entry<String, List<String>> entry : classNameToGroupsMap.entrySet()) {
        try {
          Class configGroupsClass = classLoader.loadClass(entry.getKey());
          for (String groupName : entry.getValue()) {
            ConfigGroups.Groups group = (ConfigGroups.Groups) Enum.valueOf(configGroupsClass, groupName);
            groupNameToLabelMap.put(groupName, group.getLabel());
          }
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
}
