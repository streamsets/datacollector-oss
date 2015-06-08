/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.definition;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.ErrorStage;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.HideConfig;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.ConfigDefinition;
import com.streamsets.pipeline.config.ConfigGroupDefinition;
import com.streamsets.pipeline.config.RawSourceDefinition;
import com.streamsets.pipeline.config.StageDefinition;
import com.streamsets.pipeline.config.StageType;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public abstract class StageDefinitionExtractor {

  private static final StageDefinitionExtractor EXTRACTOR = new StageDefinitionExtractor() {};

  public static StageDefinitionExtractor get() {
    return EXTRACTOR;
  }

  public StageDefinition extract(Class<? extends Stage> klass, Object contextMsg) {
    contextMsg = Utils.formatL("{} Stage='{}'", contextMsg, klass.getSimpleName());
    StageDef sDef = klass.getAnnotation(StageDef.class);
    Utils.checkArgument(sDef != null, Utils.formatL("{} does not have a StageDef annotation", contextMsg));

    String className = klass.getName();
    String name = className.replace(".", "_").replace("$", "_");
    String version = sDef.version();
    Utils.checkArgument(!version.isEmpty(), Utils.formatL("{} version cannot be empty", contextMsg));
    String label = sDef.label();
    String description = sDef.description();
    String icon = sDef.icon();
    StageType type = extractStageType(klass, contextMsg);
    boolean errorStage = klass.getAnnotation(ErrorStage.class) != null;
    HideConfig hideConfigs = klass.getAnnotation(HideConfig.class);
    boolean preconditions = (hideConfigs == null) || hideConfigs.preconditions();
    boolean onRecordError = (hideConfigs == null) || hideConfigs.onErrorRecord();
    List<ConfigDefinition> configDefinitions = extractConfigDefinitions(klass, hideConfigs, contextMsg);
    RawSourceDefinition rawSourceDefinition = RawSourceDefinitionExtractor.get().extract(klass, contextMsg);
    Utils.checkArgument(rawSourceDefinition == null || type == StageType.SOURCE,
                        Utils.formatL("{} only a SOURCE can have a RawSourcePreviewer", contextMsg));
    ConfigGroupDefinition configGroupDefinition = ConfigGroupExtractor.get().extract(klass, contextMsg);
    Utils.checkArgument(sDef.outputStreams().isEnum(), Utils.formatL("{} outputStreams '{}' must be an enum",
                                                                    contextMsg, sDef.outputStreams().getSimpleName()));
    String outputStreamLabelProviderClass = (type != StageType.TARGET) ? sDef.outputStreams().getName() : null;
    Utils.checkArgument(sDef.outputStreams() == StageDef.DefaultOutputStreams.class || type != StageType.TARGET,
                        Utils.formatL("{} a TARGET cannot have an OutputStreams", contextMsg));
    boolean variableOutputStreams = StageDef.VariableOutputStreams.class.isAssignableFrom(sDef.outputStreams());
    int outputStreams = (variableOutputStreams || type == StageType.TARGET)
                        ? 0 : sDef.outputStreams().getEnumConstants().length;
    List<ExecutionMode> executionModes = ImmutableList.copyOf(sDef.execution());
    Utils.checkArgument(!executionModes.isEmpty(),
                        Utils.formatL("{} the Stage must support at least one execution mode", contextMsg));

    //TODO figure out how this one is used besides the check
    String outputStreamsDrivenByConfig = sDef.outputStreamsDrivenByConfig();
    Utils.checkArgument(!variableOutputStreams || !outputStreamsDrivenByConfig.isEmpty(), Utils.formatL(
        "{} A stage with VariableOutputStreams must have  Stage define a 'outputStreamsDrivenByConfig' config",
        contextMsg));

    validateConfigGroups(configDefinitions, configGroupDefinition, contextMsg);

    StageDefinition stageDef =
        new StageDefinition(className, name, version, label, description, type, errorStage, preconditions,
                            onRecordError, configDefinitions, rawSourceDefinition, icon, configGroupDefinition,
                            variableOutputStreams, outputStreams, outputStreamLabelProviderClass, executionModes);

    Utils.checkArgument(!variableOutputStreams || stageDef.getConfigDefinition(outputStreamsDrivenByConfig) != null,
                        Utils.formatL("{} outputStreamsDrivenByConfig='{}' not defined as configuration",
                                      contextMsg, outputStreamsDrivenByConfig));
    return stageDef;
  }

  private List<ConfigDefinition> extractConfigDefinitions(Class<? extends Stage> klass, HideConfig hideConfigs,
      Object contextMsg) {
    List<ConfigDefinition> cDefs = ConfigDefinitionExtractor.get().extract(klass, contextMsg);

    Set<String> hideConfigSet = (hideConfigs != null) ? ImmutableSet.copyOf(hideConfigs.value())
                                                     : Collections.<String>emptySet();

    if (!hideConfigSet.isEmpty()) {
      Iterator<ConfigDefinition> iterator = cDefs.iterator();
      while (iterator.hasNext()) {
        if (hideConfigSet.contains(iterator.next().getName())) {
          iterator.remove();
        }
      }
    }
    return cDefs;
  }

  private StageType extractStageType(Class<? extends Stage> klass, Object contextMsg) {
    StageType type;
    if (Source.class.isAssignableFrom(klass)) {
      type = StageType.SOURCE;
    } else if (Processor.class.isAssignableFrom(klass)) {
      type = StageType.PROCESSOR;
    } else if (Target.class.isAssignableFrom(klass)) {
      type = StageType.TARGET;
    } else {
      throw new IllegalArgumentException(Utils.format("{} does not implement Source, Processor nor Target", contextMsg));
    }
    return type;
  }

  private void validateConfigGroups(List<ConfigDefinition> configs, ConfigGroupDefinition groups, Object contextMsg) {
    for (ConfigDefinition config : configs) {
      if (!config.getGroup().isEmpty()) {
        Utils.checkArgument(groups.getGroupNames().contains(config.getGroup()),
                            Utils.formatL("{} configuration '{}' has an undefined group '{}'",
                                          contextMsg, config.getName(), config.getGroup()));
      }
    }
  }

}
