/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.ChooserValues;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.el.ElConstantDefinition;
import com.streamsets.pipeline.el.ElFunctionDefinition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class PipelineDefinition {
  /*The config definitions of the pipeline*/
  private List<ConfigDefinition> configDefinitions;
  private ConfigGroupDefinition groupDefinition;

  public static PipelineDefinition getPipelineDef() {
    return new PipelineDefinition().localize();
  }

  public PipelineDefinition localize() {
    ClassLoader classLoader = getClass().getClassLoader();

    // stage configs
    List<ConfigDefinition> configDefs = new ArrayList<>();
    for (ConfigDefinition configDef : getConfigDefinitions()) {
      configDefs.add(configDef.localize(classLoader, PipelineDefConfigs.class.getName() + "-bundle"));
    }

    // stage groups
    ConfigGroupDefinition groupDefs = StageDefinition.localizeConfigGroupDefinition(classLoader,
                                                                                    getConfigGroupDefinition());
    return new PipelineDefinition(configDefs, groupDefs);
  }

  private PipelineDefinition(List<ConfigDefinition> configDefs, ConfigGroupDefinition groupDef) {
    configDefinitions = configDefs;
    groupDefinition = groupDef;
  }

  private static List<ConfigDefinition> createPipelineConfigs() {
    List<ConfigDefinition> defs = new ArrayList<>();
    defs.add(createExecutionModeOption());
    defs.add(createDeliveryGuaranteeOption());
    defs.add(createBadRecordsHandlingConfigs());
    defs.add(createConstantsConfigs());
    defs.add(createMemoryLimitConfigs());
    defs.add(createMemoryLimitExceededBehaviorConfigs());
    defs.addAll(createClusterOptions());
    return defs;
  }
  @VisibleForTesting
  PipelineDefinition() {
    this(createPipelineConfigs(), createConfigGroupDefinition());
  }

  /*Need this API for Jackson to serialize*/
  public List<ConfigDefinition> getConfigDefinitions() {
    return configDefinitions;
  }

  public ConfigGroupDefinition getConfigGroupDefinition() {
    return groupDefinition;
  }

  @Override
  public String toString() {
    return Utils.format("PipelineDefinition[configDefinitions='{}']", configDefinitions);
  }

  /**************************************************************/
  /********************** Private methods ***********************/
  /**************************************************************/

  private static ConfigGroupDefinition createConfigGroupDefinition() {
    Map<String, List<String>> classNameToGroupsMap = new HashMap<>();
    List<String> groupsInEnum = new ArrayList<>();
    List<Map<String, String>> groups = new ArrayList<>();
    for (PipelineDefConfigs.Groups group : PipelineDefConfigs.Groups.values()) {
      groupsInEnum.add(group.name());
      groups.add(ImmutableMap.of("name", group.name(), "label", group.getLabel()));
    }
    classNameToGroupsMap.put(PipelineDefConfigs.Groups.class.getName(), groupsInEnum);
    return new ConfigGroupDefinition(classNameToGroupsMap, groups);
  }

  private static ConfigDefinition createExecutionModeOption() {

    ChooserValues valueChooser = new ExecutionModeChooserValues();
    ModelDefinition model = new ModelDefinition(ModelType.VALUE_CHOOSER, valueChooser.getClass().getName(),
                                                valueChooser.getValues(), valueChooser.getLabels(), null);

    return new ConfigDefinition(
        PipelineDefConfigs.EXECUTION_MODE_CONFIG,
        ConfigDef.Type.MODEL,
        PipelineDefConfigs.EXECUTION_MODE_LABEL,
        PipelineDefConfigs.EXECUTION_MODE_DESCRIPTION,
        ExecutionMode.STANDALONE.name(),
        true,
        "",
        PipelineDefConfigs.EXECUTION_MODE_CONFIG,
        model,
        "",
        new ArrayList<>(),
        0,
        Collections.<ElFunctionDefinition> emptyList(),
        Collections.<ElConstantDefinition> emptyList(),
        Long.MIN_VALUE,
        Long.MAX_VALUE,
        "",
        0,
        Collections.<String> emptyList(),
        ConfigDef.Evaluation.IMPLICIT,
        null);
  }

  private static List<ConfigDefinition> createClusterOptions() {
    List<ConfigDefinition> list = new ArrayList<>();
    list.add(new ConfigDefinition(
        PipelineDefConfigs.CLUSTER_SLAVE_MEMORY_CONFIG,
        ConfigDef.Type.NUMBER,
        PipelineDefConfigs.CLUSTER_SLAVE_MEMORY_LABEL,
        PipelineDefConfigs.CLUSTER_SLAVE_MEMORY_DESCRIPTION,
        Integer.parseInt(PipelineDefConfigs.CLUSTER_SLAVE_MEMORY_DEFAULT),
        true,
        "CLUSTER",
        PipelineDefConfigs.CLUSTER_SLAVE_MEMORY_CONFIG,
        null,
        PipelineDefConfigs.EXECUTION_MODE_CONFIG,
        Arrays.asList((Object)ExecutionMode.CLUSTER),
        10,
        Collections.<ElFunctionDefinition> emptyList(),
        Collections.<ElConstantDefinition> emptyList(),
        256,
        1024 * 1024,
        "",
        0,
        Collections.<String> emptyList(),
        ConfigDef.Evaluation.IMPLICIT,
        (Map) ImmutableMap.of(PipelineDefConfigs.EXECUTION_MODE_CONFIG, Arrays.asList(ExecutionMode.CLUSTER.name()))));
    list.add(new ConfigDefinition(
        PipelineDefConfigs.CLUSTER_LAUNCHER_ENV_CONFIG,
        ConfigDef.Type.MAP,
        PipelineDefConfigs.CLUSTER_LAUNCHER_ENV_LABEL,
        PipelineDefConfigs.CLUSTER_LAUNCHER_ENV_DESCRIPTION,
        "",
        false,
        "CLUSTER",
        PipelineDefConfigs.CLUSTER_LAUNCHER_ENV_CONFIG,
        null,
        PipelineDefConfigs.EXECUTION_MODE_CONFIG,
        Arrays.asList((Object)ExecutionMode.CLUSTER),
        20,
        Collections.<ElFunctionDefinition> emptyList(),
        Collections.<ElConstantDefinition> emptyList(),
        Long.MIN_VALUE,
        Long.MAX_VALUE,
        "",
        0,
        Collections.<String> emptyList(),
        ConfigDef.Evaluation.IMPLICIT,
        (Map) ImmutableMap.of(PipelineDefConfigs.EXECUTION_MODE_CONFIG, Arrays.asList(ExecutionMode.CLUSTER.name()))));
    return list;
  }

  private static ConfigDefinition createDeliveryGuaranteeOption() {

    ChooserValues valueChooser = new DeliveryGuaranteeChooserValues();
    ModelDefinition model = new ModelDefinition(ModelType.VALUE_CHOOSER, valueChooser.getClass().getName(),
                                                valueChooser.getValues(), valueChooser.getLabels(), null);

    return new ConfigDefinition(
      PipelineDefConfigs.DELIVERY_GUARANTEE_CONFIG,
      ConfigDef.Type.MODEL,
      PipelineDefConfigs.DELIVERY_GUARANTEE_LABEL,
      PipelineDefConfigs.DELIVERY_GUARANTEE_DESCRIPTION,
      DeliveryGuarantee.AT_LEAST_ONCE.name(),
      true,
      "",
      PipelineDefConfigs.DELIVERY_GUARANTEE_CONFIG,
      model,
      "",
      new ArrayList<>(),
      5,
      Collections.<ElFunctionDefinition> emptyList(),
      Collections.<ElConstantDefinition> emptyList(),
      Long.MIN_VALUE,
      Long.MAX_VALUE,
      "",
      0,
      Collections.<String> emptyList(),
      ConfigDef.Evaluation.IMPLICIT,
      null);
  }

  private static ConfigDefinition createBadRecordsHandlingConfigs() {
    ChooserValues valueChooser = new ErrorHandlingChooserValues();
    ModelDefinition model = new ModelDefinition(ModelType.VALUE_CHOOSER, valueChooser.getClass().getName(),
                                                valueChooser.getValues(), valueChooser.getLabels(), null);
    return new ConfigDefinition(
        PipelineDefConfigs.ERROR_RECORDS_CONFIG,
        ConfigDef.Type.MODEL,
        PipelineDefConfigs.ERROR_RECORDS_LABEL,
        PipelineDefConfigs.ERROR_RECORDS_DESCRIPTION,
        "",
        true,
        PipelineDefConfigs.Groups.BAD_RECORDS.name(),
        PipelineDefConfigs.ERROR_RECORDS_CONFIG,
        model,
        "",
        new ArrayList<>(),
        10,
        Collections.<ElFunctionDefinition> emptyList(),
        Collections.<ElConstantDefinition> emptyList(),
        Long.MIN_VALUE,
        Long.MAX_VALUE,
        "",
        0,
        Collections.<String> emptyList(),
        ConfigDef.Evaluation.IMPLICIT,
        null);
  }

  private static ConfigDefinition createConstantsConfigs() {
    return new ConfigDefinition(
      PipelineDefConfigs.CONSTANTS_CONFIG,
      ConfigDef.Type.MAP,
      PipelineDefConfigs.CONSTANTS_LABEL,
      PipelineDefConfigs.CONSTANTS_DESCRIPTION,
      null,
      true,
      PipelineDefConfigs.Groups.CONSTANTS.name(),
      PipelineDefConfigs.CONSTANTS_CONFIG,
      null,
      "",
      new ArrayList<>(),
      10,
      Collections.<ElFunctionDefinition> emptyList(),
      Collections.<ElConstantDefinition> emptyList(),
      Long.MIN_VALUE,
      Long.MAX_VALUE,
      "",
      0,
      Collections.<String> emptyList(),
      ConfigDef.Evaluation.IMPLICIT,
      null);
  }
  private static ConfigDefinition createMemoryLimitExceededBehaviorConfigs() {

    ChooserValues valueChooser = new MemoryLimitExceededChooserValues();
    ModelDefinition model = new ModelDefinition(ModelType.VALUE_CHOOSER, valueChooser.getClass().getName(),
      valueChooser.getValues(), valueChooser.getLabels(), null);

    return new ConfigDefinition(
      PipelineDefConfigs.MEMORY_LIMIT_EXCEEDED_CONFIG,
      ConfigDef.Type.MODEL,
      PipelineDefConfigs.MEMORY_LIMIT_EXCEEDED_LABEL,
      PipelineDefConfigs.MEMORY_LIMIT_EXCEEDED_DESCRIPTION,
      MemoryLimitExceeded.STOP_PIPELINE.name(),
      true,
      "",
      PipelineDefConfigs.MEMORY_LIMIT_EXCEEDED_CONFIG,
      model,
      "",
      new ArrayList<>(),
      10,
      Collections.<ElFunctionDefinition> emptyList(),
      Collections.<ElConstantDefinition> emptyList(),
      Long.MIN_VALUE,
      Long.MAX_VALUE,
      "",
      0,
      Collections.<String> emptyList(),
      ConfigDef.Evaluation.IMPLICIT,
      null);
  }
  private static ConfigDefinition createMemoryLimitConfigs() {

    return new ConfigDefinition(
      PipelineDefConfigs.MEMORY_LIMIT_CONFIG,
      ConfigDef.Type.NUMBER,
      PipelineDefConfigs.MEMORY_LIMIT_LABEL,
      PipelineDefConfigs.MEMORY_LIMIT_DESCRIPTION,
      PipelineDefConfigs.MEMORY_LIMIT_DEFAULT,
      true,
      "",
      PipelineDefConfigs.MEMORY_LIMIT_CONFIG,
      null,
      "",
      new ArrayList<>(),
      20,
      Collections.<ElFunctionDefinition> emptyList(),
      Collections.<ElConstantDefinition> emptyList(),
      Long.MIN_VALUE,
      Long.MAX_VALUE,
      "",
      0,
      Collections.<String> emptyList(),
      ConfigDef.Evaluation.IMPLICIT,
      null);
  }
}
