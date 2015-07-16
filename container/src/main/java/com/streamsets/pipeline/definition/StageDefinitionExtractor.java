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
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.ConfigDefinition;
import com.streamsets.pipeline.config.ConfigGroupDefinition;
import com.streamsets.pipeline.config.RawSourceDefinition;
import com.streamsets.pipeline.config.StageDefinition;
import com.streamsets.pipeline.config.StageLibraryDefinition;
import com.streamsets.pipeline.config.StageType;
import com.streamsets.pipeline.creation.PipelineConfigBean;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class StageDefinitionExtractor {

  private static final StageDefinitionExtractor EXTRACTOR = new StageDefinitionExtractor() {};

  public static StageDefinitionExtractor get() {
    return EXTRACTOR;
  }

  static final ConfigDefinition REQUIRED_FIELDS;
  static final ConfigDefinition PRECONDITIONS;
  static final ConfigDefinition ON_ERROR_RECORD;

  static {
    List<ConfigDefinition> defs = ConfigDefinitionExtractor.get().extract(BuiltInStageDefConfigs.class,
                                                                          "Built-in stage configurations");
    Map<String, ConfigDefinition> map = new HashMap<>();
    for (ConfigDefinition def : defs) {
      map.put(def.getName(), def);
    }
    REQUIRED_FIELDS = map.get(BuiltInStageDefConfigs.STAGE_REQUIRED_FIELDS_CONFIG);
    PRECONDITIONS = map.get(BuiltInStageDefConfigs.STAGE_PRECONDITIONS_CONFIG);
    ON_ERROR_RECORD = map.get(BuiltInStageDefConfigs.STAGE_ON_RECORD_ERROR_CONFIG);
    Utils.checkState(REQUIRED_FIELDS != null, Utils.format("Missing built-in configuration '{}'",
                                                           BuiltInStageDefConfigs.STAGE_REQUIRED_FIELDS_CONFIG));
    Utils.checkState(PRECONDITIONS != null, Utils.format("Missing built-in configuration '{}'",
                                                         BuiltInStageDefConfigs.STAGE_PRECONDITIONS_CONFIG));
    Utils.checkState(ON_ERROR_RECORD != null, Utils.format("Missing built-in configuration '{}'",
                                                           BuiltInStageDefConfigs.STAGE_ON_RECORD_ERROR_CONFIG));
  }

  static String getStageName(Class klass) {
    return klass.getName().replace(".", "_").replace("$", "_");
  }

  public List<ErrorMessage> validate(StageLibraryDefinition libraryDef, Class<? extends Stage> klass, Object contextMsg) {
    List<ErrorMessage> errors = new ArrayList<>();
    contextMsg = Utils.formatL("{} Stage='{}'", contextMsg, klass.getSimpleName());

    StageDef sDef = klass.getAnnotation(StageDef.class);
    if (sDef == null) {
      errors.add(new ErrorMessage(DefinitionError.DEF_300, contextMsg));
    } else {
      String version = sDef.version();
      if (version.isEmpty()) {
        errors.add(new ErrorMessage(DefinitionError.DEF_301, contextMsg));
      }
      if (!sDef.icon().isEmpty()) {
        if (klass.getClassLoader().getResource(sDef.icon()) == null) {
          errors.add(new ErrorMessage(DefinitionError.DEF_311, contextMsg, sDef.icon()));
        }
      }
      StageType type = extractStageType(klass);
      if (type == null) {
        errors.add(new ErrorMessage(DefinitionError.DEF_302, contextMsg));
      }
      boolean errorStage = klass.getAnnotation(ErrorStage.class) != null;
      if (type != null && errorStage && type == StageType.SOURCE) {
        errors.add(new ErrorMessage(DefinitionError.DEF_303, contextMsg));
      }
      HideConfig hideConfigs = klass.getAnnotation(HideConfig.class);

      List<ErrorMessage> configErrors = ConfigDefinitionExtractor.get().validate(klass, contextMsg);
      errors.addAll(configErrors);

      List<ErrorMessage> rawSourceErrors = RawSourceDefinitionExtractor.get().validate(klass, contextMsg);
      errors.addAll(rawSourceErrors);
      if (type != null && rawSourceErrors.isEmpty() && type != StageType.SOURCE) {
        if (RawSourceDefinitionExtractor.get().extract(klass, contextMsg) != null) {
          errors.add(new ErrorMessage(DefinitionError.DEF_304, contextMsg));
        }

      }
      List<ErrorMessage> configGroupErrors = ConfigGroupExtractor.get().validate(klass, contextMsg);
      errors.addAll(configGroupErrors);
      errors.addAll(ConfigGroupExtractor.get().validate(klass, contextMsg));

      if (!sDef.outputStreams().isEnum()) {
        errors.add(new ErrorMessage(DefinitionError.DEF_305, contextMsg, sDef.outputStreams().getSimpleName()));
      }

      if (type != null && sDef.outputStreams() != StageDef.DefaultOutputStreams.class && type == StageType.TARGET) {
        errors.add(new ErrorMessage(DefinitionError.DEF_306, contextMsg));
      }

      boolean variableOutputStreams = StageDef.VariableOutputStreams.class.isAssignableFrom(sDef.outputStreams());

      List<ExecutionMode> executionModes = ImmutableList.copyOf(sDef.execution());
      if (executionModes.isEmpty()) {
        errors.add(new ErrorMessage(DefinitionError.DEF_307, contextMsg));
      }

      String outputStreamsDrivenByConfig = sDef.outputStreamsDrivenByConfig();

      if (variableOutputStreams && outputStreamsDrivenByConfig.isEmpty()) {
        errors.add(new ErrorMessage(DefinitionError.DEF_308, contextMsg));
      }

      if (configErrors.isEmpty() && configGroupErrors.isEmpty()) {
        List<ConfigDefinition> configDefs = extractConfigDefinitions(klass, hideConfigs, contextMsg);
        ConfigGroupDefinition configGroupDef = ConfigGroupExtractor.get().extract(klass, contextMsg);
        errors.addAll(validateConfigGroups(configDefs, configGroupDef, contextMsg));
        if (variableOutputStreams) {
          boolean found = false;
          for (ConfigDefinition configDef : configDefs) {
            if (configDef.getName().equals(outputStreamsDrivenByConfig)) {
              found = true;
              break;
            }
          }
          if (!found) {
            errors.add(new ErrorMessage(DefinitionError.DEF_309, contextMsg, outputStreamsDrivenByConfig));
          }
        }
      }
    }
    return errors;
  }

  public StageDefinition extract(StageLibraryDefinition libraryDef, Class<? extends Stage> klass, Object contextMsg) {
    List<ErrorMessage> errors = validate(libraryDef, klass, contextMsg);
    if (errors.isEmpty()) {
      contextMsg = Utils.formatL("{} Stage='{}'", contextMsg, klass.getSimpleName());

      StageDef sDef = klass.getAnnotation(StageDef.class);
      String name = getStageName(klass);
      String version = sDef.version();
      String label = sDef.label();
      String description = sDef.description();
      String icon = sDef.icon();
      StageType type = extractStageType(klass);
      boolean errorStage = klass.getAnnotation(ErrorStage.class) != null;
      HideConfig hideConfigs = klass.getAnnotation(HideConfig.class);
      boolean preconditions = !errorStage && type != StageType.SOURCE &&
                              ((hideConfigs == null) || !hideConfigs.preconditions());
      boolean onRecordError = !errorStage && ((hideConfigs == null) || !hideConfigs.onErrorRecord());
      List<ConfigDefinition> configDefinitions = extractConfigDefinitions(klass, hideConfigs, contextMsg);
      RawSourceDefinition rawSourceDefinition = RawSourceDefinitionExtractor.get().extract(klass, contextMsg);
      ConfigGroupDefinition configGroupDefinition = ConfigGroupExtractor.get().extract(klass, contextMsg);
      String outputStreamLabelProviderClass = (type != StageType.TARGET) ? sDef.outputStreams().getName() : null;
      boolean variableOutputStreams = StageDef.VariableOutputStreams.class.isAssignableFrom(sDef.outputStreams());
      int outputStreams = (variableOutputStreams || type == StageType.TARGET)
                          ? 0 : sDef.outputStreams().getEnumConstants().length;
      List<ExecutionMode> executionModes = ImmutableList.copyOf(sDef.execution());

      boolean recordsByRef = sDef.recordsByRef();

      if (preconditions) {
        configDefinitions.add(REQUIRED_FIELDS);
        configDefinitions.add(PRECONDITIONS);
      }
      if (onRecordError) {
        configDefinitions.add(ON_ERROR_RECORD);
      }

      boolean privateClassLoader = sDef.privateClassLoader();

      return new StageDefinition(libraryDef, privateClassLoader, klass, name, version, label, description, type,
                                 errorStage, preconditions, onRecordError, configDefinitions, rawSourceDefinition, icon,
                                 configGroupDefinition, variableOutputStreams, outputStreams,
                                 outputStreamLabelProviderClass, executionModes, recordsByRef);
    } else {
      throw new IllegalArgumentException(Utils.format("Invalid StageDefinition: {}", errors));
    }
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

  private StageType extractStageType(Class<? extends Stage> klass) {
    StageType type;
    if (Source.class.isAssignableFrom(klass)) {
      type = StageType.SOURCE;
    } else if (Processor.class.isAssignableFrom(klass)) {
      type = StageType.PROCESSOR;
    } else if (Target.class.isAssignableFrom(klass)) {
      type = StageType.TARGET;
    } else if (PipelineConfigBean.class.isAssignableFrom(klass)) {
      type = StageType.PIPELINE;
    } else {
      type = null;
    }
    return type;
  }

  private List<ErrorMessage> validateConfigGroups(List<ConfigDefinition> configs, ConfigGroupDefinition
      groups, Object contextMsg) {
    List<ErrorMessage> errors = new ArrayList<>();
    for (ConfigDefinition config : configs) {
      if (!config.getGroup().isEmpty()) {
        if (!groups.getGroupNames().contains(config.getGroup())) {
          errors.add(new ErrorMessage(DefinitionError.DEF_310, contextMsg, config.getName(), config.getGroup()));
        }
      }
    }
    return errors;
  }

}
