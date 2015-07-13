/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.definition.ConfigDefinitionExtractor;
import com.streamsets.pipeline.definition.ConfigGroupExtractor;
import com.streamsets.pipeline.creation.PipelineConfigBean;

import java.util.ArrayList;
import java.util.List;

public class PipelineDefinition {
  /*The config definitions of the pipeline*/
  private final List<ConfigDefinition> configDefinitions;
  private final ConfigGroupDefinition groupDefinition;
  private final List<ConfigConfiguration> defaultConfigs;

  public static PipelineDefinition getPipelineDef() {
    return new PipelineDefinition().localize();
  }

  public PipelineDefinition localize() {
    ClassLoader classLoader = getClass().getClassLoader();

    // stage configs
    List<ConfigDefinition> configDefs = new ArrayList<>();
    for (ConfigDefinition configDef : getConfigDefinitions()) {
      configDefs.add(configDef.localize(classLoader, PipelineConfigBean.class.getName() + "-bundle"));
    }

    // stage groups
    ConfigGroupDefinition groupDefs = StageDefinition.localizeConfigGroupDefinition(classLoader,
                                                                                    getConfigGroupDefinition());
    return new PipelineDefinition(configDefs, groupDefs);
  }

  private PipelineDefinition(List<ConfigDefinition> configDefs, ConfigGroupDefinition groupDef) {
    configDefinitions = configDefs;
    groupDefinition = groupDef;
    List<ConfigConfiguration> configs = new ArrayList<>();
    for (ConfigDefinition configDef : configDefs) {
      configs.add(new ConfigConfiguration(configDef.getName(), configDef.getDefaultValue()));
    }
    defaultConfigs = ImmutableList.copyOf(configs);
  }

  private static List<ConfigDefinition> createPipelineConfigs() {
    return ConfigDefinitionExtractor.get().extract(PipelineConfigBean.class, "Pipeline Definition");
  }

  @VisibleForTesting
  PipelineDefinition() {
    this(createPipelineConfigs(), ConfigGroupExtractor.get().extract(PipelineConfigBean.class, "Pipeline Definition"));
  }

  /*Need this API for Jackson to serialize*/
  public List<ConfigDefinition> getConfigDefinitions() {
    return configDefinitions;
  }

  public ConfigGroupDefinition getConfigGroupDefinition() {
    return groupDefinition;
  }

  public List<ConfigConfiguration> getPipelineDefaultConfigs() {
    return defaultConfigs;
  }

  @Override
  public String toString() {
    return Utils.format("PipelineDefinition[configDefinitions='{}']", configDefinitions);
  }

}
