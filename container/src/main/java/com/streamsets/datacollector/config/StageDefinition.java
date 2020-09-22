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
package com.streamsets.datacollector.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableSet;
import com.streamsets.datacollector.creation.StageConfigBean;
import com.streamsets.pipeline.SDCClassLoader;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.HideStage;
import com.streamsets.pipeline.api.Label;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageType;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.LocalizableMessage;
import com.streamsets.pipeline.api.impl.Utils;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Captures the configuration options for a {@link com.streamsets.pipeline.api.Stage}.
 *
 */
public class StageDefinition implements PrivateClassLoaderDefinition {
  private final StageLibraryDefinition libraryDefinition;
  private final boolean privateClassLoader;
  private final ClassLoader classLoader;
  private final Class<? extends Stage> klass;
  private final String name;
  private final int version;
  private final String label;
  private final String description;
  private final StageType type;
  private final boolean errorStage;
  private final boolean statsAggregatorStage;
  private final boolean pipelineLifecycleStage;
  private final boolean preconditions;
  private final boolean onRecordError;
  private final boolean connectionVerifierStage;
  private final RawSourceDefinition rawSourceDefinition;
  private final List<ConfigDefinition> configDefinitions;
  private final Map<String, ConfigDefinition> configDefinitionsMap;
  private final String icon;
  private final ConfigGroupDefinition configGroupDefinition;
  private final boolean variableOutputStreams;
  private final int outputStreams;
  private final String outputStreamLabelProviderClass;
  private List<String> outputStreamLabels;
  private final List<ExecutionMode> executionModes;
  private final boolean recordsByRef;
  private final StageUpgrader upgrader;
  private final List<String> libJarsRegex;
  private final boolean resetOffset;
  private final String onlineHelpRefUrl;
  private final boolean offsetCommitTrigger;
  private final boolean producesEvents;
  private final List<ServiceDependencyDefinition> services;
  private final List<HideStage.Type> hideStage;
  private final StageDef stageDef;
  private final boolean sendsResponse;
  private final boolean beta;
  private final int inputStreams;
  private final String inputStreamLabelProviderClass;
  private List<String> inputStreamLabels;
  private List<Class> eventDefs;
  private final boolean bisectable;
  private String yamlUpgrader;
  private List<String> tags;

  // localized version
  private StageDefinition(
      StageDef stageDef,
      StageLibraryDefinition libraryDefinition,
      boolean privateClassLoader,
      ClassLoader classLoader,
      Class<? extends Stage> klass,
      String name,
      int version,
      String label,
      String description,
      StageType type,
      boolean errorStage,
      boolean preconditions,
      boolean onRecordError,
      boolean connectionVerifierStage,
      List<ConfigDefinition> configDefinitions,
      RawSourceDefinition rawSourceDefinition,
      String icon,
      ConfigGroupDefinition configGroupDefinition,
      boolean variableOutputStreams,
      int outputStreams,
      List<String> outputStreamLabels,
      List<ExecutionMode> executionModes,
      boolean recordsByRef,
      StageUpgrader upgrader,
      List<String> libJarsRegex,
      boolean resetOffset,
      String onlineHelpRefUrl,
      boolean statsAggregatorStage,
      boolean pipelineLifecycleStage,
      boolean offsetCommitTrigger,
      boolean producesEvents,
      List<ServiceDependencyDefinition> services,
      List<HideStage.Type> hideStage,
      boolean sendsResponse,
      boolean beta,
      int inputStreams,
      String inputStreamLabelProviderClass,
      List<String> inputStreamLabels,
      boolean bisectable,
      List<Class> eventDefs,
      String yamlUpgrader,
      List<String> tags
  ) {
    this.stageDef = stageDef;
    this.libraryDefinition = libraryDefinition;
    this.privateClassLoader = privateClassLoader;
    this.classLoader = classLoader;
    this.klass = klass;
    this.name = name;
    this.version = version;
    this.label = label;
    this.description = description;
    this.type = type;
    this.errorStage = errorStage;
    this.preconditions = preconditions;
    this.onRecordError = onRecordError;
    this.connectionVerifierStage = connectionVerifierStage;
    this.configDefinitions = configDefinitions;
    this.rawSourceDefinition = rawSourceDefinition;
    this.onlineHelpRefUrl = onlineHelpRefUrl;
    this.statsAggregatorStage = statsAggregatorStage;
    this.pipelineLifecycleStage = pipelineLifecycleStage;
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
    this.recordsByRef = recordsByRef;
    this.upgrader = upgrader;
    this.libJarsRegex = libJarsRegex;
    this.resetOffset = resetOffset;
    this.offsetCommitTrigger = offsetCommitTrigger;
    this.producesEvents = producesEvents;
    this.services = Collections.unmodifiableList(services);
    this.hideStage = Collections.unmodifiableList(hideStage);
    this.sendsResponse = sendsResponse;
    this.beta = beta;
    this.inputStreams = inputStreams;
    this.inputStreamLabelProviderClass = inputStreamLabelProviderClass;
    this.inputStreamLabels = inputStreamLabels;
    this.bisectable = bisectable;
    this.eventDefs = eventDefs;
    this.yamlUpgrader = yamlUpgrader;
    this.tags = tags;
  }

  @SuppressWarnings("unchecked")
  public StageDefinition(StageDefinition def, ClassLoader classLoader) {
    stageDef = def.stageDef;
    libraryDefinition = def.libraryDefinition;
    privateClassLoader = def.privateClassLoader;
    this.classLoader = classLoader;
    try {

      klass = (Class<? extends Stage>) classLoader.loadClass(def.getClassName());
    } catch (Exception ex) {
      throw new Error(ex);
    }
    name = def.name;
    version = def.version;
    label = def.label;
    description = def.description;
    type = def.type;
    errorStage = def.errorStage;
    preconditions = def.preconditions;
    onRecordError = def.onRecordError;
    connectionVerifierStage = def.connectionVerifierStage;
    configDefinitions = def.configDefinitions;
    rawSourceDefinition = def.rawSourceDefinition;
    configDefinitionsMap = def.configDefinitionsMap;
    icon = def.icon;
    configGroupDefinition = def.configGroupDefinition;
    variableOutputStreams = def.variableOutputStreams;
    outputStreams = def.outputStreams;
    outputStreamLabelProviderClass = def.outputStreamLabelProviderClass;
    executionModes = def.executionModes;
    recordsByRef = def.recordsByRef;
    upgrader = def.upgrader;
    libJarsRegex = def.libJarsRegex;
    resetOffset = def.resetOffset;
    onlineHelpRefUrl = def.onlineHelpRefUrl;
    statsAggregatorStage = def.statsAggregatorStage;
    offsetCommitTrigger = def.offsetCommitTrigger;
    producesEvents = def.producesEvents;
    pipelineLifecycleStage = def.pipelineLifecycleStage;
    services = def.services;
    hideStage = def.hideStage;
    sendsResponse = def.sendsResponse;
    beta = def.beta;
    inputStreams = def.inputStreams;
    inputStreamLabelProviderClass = def.inputStreamLabelProviderClass;
    inputStreamLabels = def.inputStreamLabels;
    bisectable = def.bisectable;
    yamlUpgrader = (def.yamlUpgrader.isEmpty()) ? null : def.yamlUpgrader;
    tags = def.tags;
  }

  public StageDefinition(
      StageDef stageDef,
      StageLibraryDefinition libraryDefinition,
      boolean privateClassLoader,
      Class<? extends Stage> klass,
      String name,
      int version,
      String label,
      String description,
      StageType type,
      boolean errorStage,
      boolean preconditions,
      boolean onRecordError,
      boolean connectionVerifierStage,
      List<ConfigDefinition> configDefinitions,
      RawSourceDefinition rawSourceDefinition,
      String icon,
      ConfigGroupDefinition configGroupDefinition,
      boolean variableOutputStreams,
      int outputStreams,
      String outputStreamLabelProviderClass,
      List<ExecutionMode> executionModes,
      boolean recordsByRef,
      StageUpgrader upgrader,
      List<String> libJarsRegex,
      boolean resetOffset,
      String onlineHelpRefUrl,
      boolean statsAggregatorStage,
      boolean pipelineLifecycleStage,
      boolean offsetCommitTrigger,
      boolean producesEvents,
      List<ServiceDependencyDefinition> services,
      List<HideStage.Type> hideStage,
      boolean sendsResponse,
      boolean beta,
      int inputStreams,
      String inputStreamLabelProviderClass,
      boolean bisectable,
      List<Class> eventDefs,
      String yamlUpgrader,
      List<String> tags
  ) {
    this.stageDef = stageDef;
    this.libraryDefinition = libraryDefinition;
    this.privateClassLoader = privateClassLoader;
    this.onlineHelpRefUrl = onlineHelpRefUrl;
    this.classLoader = libraryDefinition.getClassLoader();
    this.klass = klass;
    this.name = name;
    this.version = version;
    this.label = label;
    this.description = description;
    this.type = type;
    this.errorStage = errorStage;
    this.preconditions = preconditions;
    this.onRecordError = onRecordError;
    this.connectionVerifierStage = connectionVerifierStage;
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
    this.recordsByRef = recordsByRef;
    this.upgrader = upgrader;
    this.libJarsRegex = libJarsRegex;
    this.resetOffset = resetOffset;
    this.statsAggregatorStage = statsAggregatorStage;
    this.pipelineLifecycleStage = pipelineLifecycleStage;
    this.offsetCommitTrigger = offsetCommitTrigger;
    this.producesEvents = producesEvents;
    this.services = Collections.unmodifiableList(services);
    this.hideStage = Collections.unmodifiableList(hideStage);
    this.sendsResponse = sendsResponse;
    this.beta = beta;
    this.inputStreams = inputStreams;
    this.inputStreamLabelProviderClass = inputStreamLabelProviderClass;
    this.bisectable = bisectable;
    this.eventDefs = eventDefs;
    this.yamlUpgrader = yamlUpgrader;
    this.tags = tags;
  }

  public List<ExecutionMode> getLibraryExecutionModes() {
    return executionModes;
  }

  public ConfigGroupDefinition getConfigGroupDefinition() {
    return configGroupDefinition;
  }

  public String getLibrary() {
    return libraryDefinition.getName();
  }

  public String getLibraryLabel() {
    return libraryDefinition.getLabel();
  }

  @JsonIgnore
  @Override
  public ClassLoader getStageClassLoader() {
    return classLoader;
  }

  @Override
  public boolean isPrivateClassLoader() {
    return privateClassLoader;
  }

  public String getClassName() {
    return klass.getName();
  }

  @JsonIgnore
  public Class<? extends Stage> getStageClass() {
    return klass;
  }

  @Override
  public String getName() {
    return name;
  }

  public int getVersion() {
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

  public boolean hasPreconditions() {
    return preconditions;
  }

  public boolean hasOnRecordError() {
    return onRecordError;
  }

  public boolean isConnectionVerifierStage() { return connectionVerifierStage; }

  public boolean isStatsAggregatorStage() {
    return statsAggregatorStage;
  }

  public boolean isPipelineLifecycleStage() {
    return pipelineLifecycleStage;
  }

  public void addConfiguration(ConfigDefinition confDef) {
    if (configDefinitionsMap.containsKey(confDef.getName())) {
      throw new IllegalArgumentException(Utils.format("Stage '{}:{}:{}', configuration definition '{}' already exists",
                                                       getLibrary(), getName(), getVersion(), confDef.getName()));
    }
    configDefinitionsMap.put(confDef.getName(), confDef);
    configDefinitions.add(confDef);
  }

  public boolean isOffsetCommitTrigger() {
    return offsetCommitTrigger;
  }

  public List<ConfigDefinition> getConfigDefinitions() {
    return configDefinitions;
  }

  public ConfigDefinition getConfigDefinition(String configName) {
    return configDefinitionsMap.get(configName);
  }

  public Set<String> getHideConfigs() {
    HideConfigs hideConfigs = klass.getAnnotation(HideConfigs.class);
    Set<String> hideConfigSet = (hideConfigs != null) ?
        ImmutableSet.copyOf(hideConfigs.value()) :
        Collections.<String>emptySet();
    return hideConfigSet;
  }

  @JsonIgnore
  // This method returns not only main configs, but also all complex ones!
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

  public List<String> getLibJarsRegex() {
    return libJarsRegex;
  }

  public boolean isResetOffset() {
    return resetOffset;
  }

  public boolean getRecordsByRef() {
    return recordsByRef;
  }

  @JsonIgnore
  public StageUpgrader getUpgrader() {
    return upgrader;
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

  @SuppressWarnings("unchecked")
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
      groupDefs = new ConfigGroupDefinition(groupDefs.getGroupNames(), groupDefs.getClassNameToGroupsMap(),
                                            localizedGroups);
    }
    return groupDefs;
  }

  private static final String SYSTEM_CONFIGS_RB = StageConfigBean.class.getName() + "-bundle";

  public StageDefinition localize() {
    ClassLoader classLoader = libraryDefinition.getClassLoader();
    String rbName = getClassName() + "-bundle";

    // stage label & description
    String label = new LocalizableMessage(classLoader, rbName, STAGE_LABEL, getLabel(), null).getLocalized();
    String description = new LocalizableMessage(classLoader, rbName, STAGE_DESCRIPTION, getDescription(), null)
        .getLocalized();

    // stage configs
    List<ConfigDefinition> configDefs = new ArrayList<>();
    for (ConfigDefinition configDef : getConfigDefinitions()) {
      if (StageConfigBean.CONFIGS.contains(configDef.getName())) {
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
    List<String> outputStreamLabels = getOutputStreamLabels();
    if (!isVariableOutputStreams() && getOutputStreams() > 0) {
      outputStreamLabels = getStreamLabels(classLoader, getOutputStreamLabelProviderClass(), true);
    }

    // input stream labels
    List<String> inputStreamLabels = getInputStreamLabels();
    if (getInputStreams() > 0) {
      inputStreamLabels = getStreamLabels(classLoader, getInputStreamLabelProviderClass(), true);
    }

    return new StageDefinition(
        stageDef,
        libraryDefinition,
        privateClassLoader,
        getStageClassLoader(),
        getStageClass(),
        getName(),
        getVersion(),
        label,
        description,
        getType(),
        isErrorStage(),
        hasPreconditions(),
        hasOnRecordError(),
        isConnectionVerifierStage(),
        configDefs,
        rawSourceDef,
        getIcon(),
        groupDefs,
        isVariableOutputStreams(),
        getOutputStreams(),
        outputStreamLabels,
        executionModes,
        recordsByRef,
        upgrader,
        libJarsRegex,
        resetOffset,
        onlineHelpRefUrl,
        statsAggregatorStage,
        pipelineLifecycleStage,
        offsetCommitTrigger,
        producesEvents,
        services,
        hideStage,
        sendsResponse,
        beta,
        inputStreams,
        inputStreamLabelProviderClass,
        inputStreamLabels,
        bisectable,
        eventDefs,
        yamlUpgrader,
        tags
    );
  }

  private List<String> getStreamLabels(ClassLoader classLoader, String streamsLabelProviderClass, boolean localized) {
    List<String> list = new ArrayList<>();
    if (streamsLabelProviderClass != null) {
      try {
        String rbName = (localized) ? streamsLabelProviderClass + "-bundle" : null;
        Class klass = classLoader.loadClass(streamsLabelProviderClass);
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

  public String getOnlineHelpRefUrl() {
    return onlineHelpRefUrl;
  }

  public boolean isProducingEvents() {
    return producesEvents;
  }

  public List<ServiceDependencyDefinition> getServices() {
    return services;
  }

  public List<HideStage.Type> getHideStage() {
    return hideStage;
  }

  public String getOutputStreamsDrivenByConfig() {
    return stageDef != null ? stageDef.outputStreamsDrivenByConfig(): null;
  }

  @JsonIgnore
  public StageDef getStageDef() {
    return stageDef;
  }

  public boolean getSendsResponse() {
    return sendsResponse;
  }

  public boolean isBeta() {
    return beta;
  }

  public int getInputStreams() {
    return inputStreams;
  }

  public String getInputStreamLabelProviderClass() {
    return inputStreamLabelProviderClass;
  }

  public List<String> getInputStreamLabels() {
    return inputStreamLabels;
  }

  public boolean isBisectable() {
    return bisectable;
  }

  public List<Class> getEventDefs() {
    return eventDefs;
  }

  public List<String> getClassPath() {
    SDCClassLoader classLoader = (SDCClassLoader)getStageClassLoader();
    return Arrays.stream(classLoader.getURLs()).map(URL::getFile).collect(Collectors.toList());
  }

  public String getYamlUpgrader() {
    return yamlUpgrader;
  }

  public List<String> getTags() {
    return tags;
  }
}


