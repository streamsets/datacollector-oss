/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Label;
import com.streamsets.pipeline.api.impl.LocalizableMessage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stagelibrary.StageLibraryUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Captures the configuration options for a {@link com.streamsets.pipeline.api.Stage}.
 *
 */
public class StageDefinition {
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
  private final boolean errorStage;
  private final boolean requiredFields;
  private final boolean onRecordError;
  private final RawSourceDefinition rawSourceDefinition;
  private List<ConfigDefinition> configDefinitions;
  private Map<String, ConfigDefinition> configDefinitionsMap;
  private final String icon;
  private final ConfigGroupDefinition configGroupDefinition;
  private final boolean variableOutputStreams;
  private final int outputStreams;
  private final String outputStreamLabelProviderClass;
  private List<String> outputStreamLabels;
  private final List<ExecutionMode> executionModes;

  // localized version
  private StageDefinition(ClassLoader classLoader, String library, String libraryLabel, String className, String name,
      String version, String label, String description, StageType type, boolean errorStage, boolean requiredFields,
      boolean onRecordError, List<ConfigDefinition> configDefinitions, RawSourceDefinition rawSourceDefinition,
      String icon, ConfigGroupDefinition configGroupDefinition, boolean variableOutputStreams, int outputStreams,
      List<String> outputStreamLabels, List<ExecutionMode> executionModes) {
    this.classLoader = classLoader;
    this.library = library;
    this.libraryLabel = libraryLabel;
    this.className = className;
    this.name = name;
    this.version = version;
    this.label = label;
    this.description = description;
    this.type = type;
    this.errorStage = errorStage;
    this.requiredFields = requiredFields;
    this.onRecordError = onRecordError;
    this.configDefinitions = configDefinitions;
    this.rawSourceDefinition = rawSourceDefinition;
    configDefinitionsMap = new HashMap<>();
    for (ConfigDefinition conf : configDefinitions) {
      configDefinitionsMap.put(conf.getName(), conf);
      ModelDefinition modelDefinition = conf.getModel();
      if(modelDefinition != null && modelDefinition.getConfigDefinitions() != null) {
        //Multi level complex is not allowed. So we stop at this level
        //Assumption is that the config property names are unique in the class hierarchy
        //and across complex types
        for (ConfigDefinition configDefinition : modelDefinition.getConfigDefinitions()) {
          configDefinitionsMap.put(configDefinition.getName(), configDefinition);
        }
      }
    }
    this.icon = icon;
    this.configGroupDefinition = configGroupDefinition;
    this.variableOutputStreams = variableOutputStreams;
    this.outputStreams = outputStreams;
    this.outputStreamLabels = outputStreamLabels;
    outputStreamLabelProviderClass = null;
    this.executionModes = executionModes;
  }

  public StageDefinition(String className, String name, String version, String label, String description,
      StageType type, boolean errorStage,  boolean requiredFields, boolean onRecordError,
      List<ConfigDefinition> configDefinitions, RawSourceDefinition rawSourceDefinition, String icon,
      ConfigGroupDefinition configGroupDefinition, boolean variableOutputStreams, int outputStreams,
      String outputStreamLabelProviderClass, List<ExecutionMode> executionModes) {
    this.className = className;
    this.name = name;
    this.version = version;
    this.label = label;
    this.description = description;
    this.type = type;
    this.errorStage = errorStage;
    this.requiredFields = requiredFields;
    this.onRecordError = onRecordError;
    this.configDefinitions = configDefinitions;
    this.rawSourceDefinition = rawSourceDefinition;
    configDefinitionsMap = new HashMap<>();
    for (ConfigDefinition conf : configDefinitions) {
      configDefinitionsMap.put(conf.getName(), conf);
      ModelDefinition modelDefinition = conf.getModel();
      if(modelDefinition != null && modelDefinition.getConfigDefinitions() != null) {
        //Multi level complex is not allowed. So we stop at this level
        //Assumption is that the config property names are unique in the class hierarchy
        //and across complex types
        for (ConfigDefinition configDefinition : modelDefinition.getConfigDefinitions()) {
          configDefinitionsMap.put(configDefinition.getName(), configDefinition);
        }
      }
    }
    this.icon = icon;
    this.configGroupDefinition = configGroupDefinition;
    this.variableOutputStreams = variableOutputStreams;
    this.outputStreams = outputStreams;
    this.outputStreamLabelProviderClass = outputStreamLabelProviderClass;
    this.executionModes = executionModes;
  }

  public void setLibrary(String library, String label, ClassLoader classLoader) {
    this.library = library;
    this.libraryLabel = label;
    this.classLoader = classLoader;
    this.outputStreamLabels = getOutputStreamLabels(classLoader);
    try {
      klass = classLoader.loadClass(getClassName());
    } catch (ClassNotFoundException ex) {
      throw new RuntimeException(ex);
    }
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

  public ClassLoader getStageClassLoader() {
    return classLoader;
  }

  public String getClassName() {
    return className;
  }

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

  public boolean isErrorStage() {
    return errorStage;
  }

  public boolean hasRequiredFields() {
    return requiredFields;
  }

  public boolean hasOnRecordError() {
    return onRecordError;
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

  public Map<String, ConfigDefinition> getConfigDefinitionsMap() {
    return configDefinitionsMap;
  }

  @Override
  public String toString() {
    return Utils.format("StageDefinition[library='{}' name='{}' version='{}' type='{}' class='{}']", getLibrary(),
                        getName(), getVersion(), getType(), getStageClass());
  }

  public String getIcon() {
    return icon;
  }

  public boolean isVariableOutputStreams() {
    return variableOutputStreams;
  }

  public int getOutputStreams() {
    return outputStreams;
  }

  public String getOutputStreamLabelProviderClass() {
    return outputStreamLabelProviderClass;
  }

  public List<String> getOutputStreamLabels() {
    return outputStreamLabels;
  }

  public List<ExecutionMode> getExecutionModes() {
    return executionModes;
  }

  private final static String STAGE_LABEL = "stageLabel";
  private final static String STAGE_DESCRIPTION = "stageDescription";

  private static Map<String, String> getGroupToResourceBundle(ConfigGroupDefinition configGroupDefinition) {
    Map<String, String> map = new HashMap<>();
    for (Map.Entry<String, List<String>> entry: configGroupDefinition.getClassNameToGroupsMap().entrySet()) {
      for (String group : entry.getValue()) {
        map.put(group, entry.getKey() + "-bundle");
      }
    }
    return map;
  }

  public static ConfigGroupDefinition localizeConfigGroupDefinition(ClassLoader classLoader,
      ConfigGroupDefinition groupDefs) {
    if (groupDefs != null) {
      Map<String, List<String>> classNameToGroupsMap = groupDefs.getClassNameToGroupsMap();
      Map<String, String> groupToDefaultLabelMap = new HashMap<>();
      for (Map.Entry<String, List<String>> entry : classNameToGroupsMap.entrySet()) {
        Class groupClass;
        try {
          groupClass = classLoader.loadClass(entry.getKey());
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        }
        boolean isLabel = Label.class.isAssignableFrom(groupClass);
        for (String group : entry.getValue()) {
          Enum e = Enum.valueOf(groupClass, group);
          String groupLabel = (isLabel) ? ((Label)e).getLabel() : e.name();
          groupToDefaultLabelMap.put(group, groupLabel);
        }
      }
      Map<String, String> groupBundles = getGroupToResourceBundle(groupDefs);
      List<Map<String, String>> localizedGroups = new ArrayList<>();
      for (Map<String, String> group : groupDefs.getGroupNameToLabelMapList()) {
        String groupName = group.get("name");
        Map<String, String> localizeGroup = new HashMap<>();
        localizeGroup.put("name", groupName);
        localizeGroup.put("label", new LocalizableMessage(classLoader, groupBundles.get(groupName), groupName,
                                                          groupToDefaultLabelMap.get(groupName), null).getLocalized());
        localizedGroups.add(localizeGroup);
      }
      groupDefs = new ConfigGroupDefinition(groupDefs.getClassNameToGroupsMap(), localizedGroups);
    }
    return groupDefs;
  }

  private static final String SYSTEM_CONFIGS_RB = SystemStageConfigs.class.getName() + "-bundle";

  public StageDefinition localize() {
    String rbName = getClassName() + "-bundle";

    // stage label & description
    String label = new LocalizableMessage(classLoader, rbName, STAGE_LABEL, getLabel(), null).getLocalized();
    String description = new LocalizableMessage(classLoader, rbName, STAGE_DESCRIPTION, getDescription(), null)
        .getLocalized();

    // Library label
    String libraryLabel = StageLibraryUtils.getLibraryLabel(classLoader);

    String errorStageLabel = null;
    String errorStageDescription = null;

    // stage configs
    List<ConfigDefinition> configDefs = new ArrayList<>();
    for (ConfigDefinition configDef : getConfigDefinitions()) {
      if (ConfigDefinition.SYSTEM_CONFIGS.contains(configDef.getName())) {
        configDefs.add(configDef.localize(getClass().getClassLoader(), SYSTEM_CONFIGS_RB));
      } else {
        configDefs.add(configDef.localize(classLoader, rbName));
      }
    }

    // stage raw-source
    RawSourceDefinition rawSourceDef = getRawSourceDefinition();
    if(rawSourceDef != null) {
      String rawSourceRbName = rawSourceDef.getRawSourcePreviewerClass() + "-bundle";
      List<ConfigDefinition> rawSourceConfigDefs = new ArrayList<>();
      for (ConfigDefinition configDef : rawSourceDef.getConfigDefinitions()) {
        rawSourceConfigDefs.add(configDef.localize(classLoader, rawSourceRbName));
      }
      rawSourceDef = new RawSourceDefinition(rawSourceDef.getRawSourcePreviewerClass(), rawSourceDef.getMimeType(),
                                    rawSourceConfigDefs);
    }

    // stage groups
    ConfigGroupDefinition groupDefs = localizeConfigGroupDefinition(classLoader, getConfigGroupDefinition());

    // output stream labels
    List<String> streamLabels = getOutputStreamLabels();
    if (!isVariableOutputStreams() && getOutputStreams() > 0) {
      streamLabels = getLocalizedOutputStreamLabels(classLoader);
    }

    return new StageDefinition(classLoader, getLibrary(), libraryLabel, getClassName(), getName(), getVersion(), label,
                               description, getType(), isErrorStage(),
                               hasRequiredFields(), hasOnRecordError(), configDefs, rawSourceDef, getIcon(), groupDefs,
                               isVariableOutputStreams(), getOutputStreams(), streamLabels, executionModes);
  }

  private List<String> _getOutputStreamLabels(ClassLoader classLoader, boolean localized) {
    List<String> list = new ArrayList<>();
    if (getOutputStreamLabelProviderClass() != null) {
      try {
        String rbName = (localized) ? getOutputStreamLabelProviderClass() + "-bundle" : null;
        Class klass = classLoader.loadClass(getOutputStreamLabelProviderClass());
        boolean isLabel = Label.class.isAssignableFrom(klass);
        for (Object e : klass.getEnumConstants()) {

          String label = (isLabel) ? ((Label) e).getLabel() : ((Enum) e).name();
          if (rbName != null) {
            label = new LocalizableMessage(classLoader, rbName, ((Enum)e).name(), label, null).getLocalized();
          }
          list.add(label);
        }
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
    return list;
  }

  private List<String> getOutputStreamLabels(ClassLoader classLoader) {
    return _getOutputStreamLabels(classLoader, false);
  }

  private List<String> getLocalizedOutputStreamLabels(ClassLoader classLoader) {
    return _getOutputStreamLabels(classLoader, true);
  }

}
