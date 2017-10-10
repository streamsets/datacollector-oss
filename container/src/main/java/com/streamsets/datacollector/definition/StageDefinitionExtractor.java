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
package com.streamsets.datacollector.definition;

import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.config.ConfigDefinition;
import com.streamsets.datacollector.config.ConfigGroupDefinition;
import com.streamsets.datacollector.config.RawSourceDefinition;
import com.streamsets.datacollector.config.ServiceDependencyDefinition;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.config.StageLibraryDefinition;
import com.streamsets.datacollector.config.StageType;
import com.streamsets.datacollector.creation.PipelineBeanCreator;
import com.streamsets.datacollector.creation.PipelineConfigBean;
import com.streamsets.datacollector.creation.RuleDefinitionsConfigBean;
import com.streamsets.datacollector.creation.StageConfigBean;
import com.streamsets.pipeline.api.OffsetCommitTrigger;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.PipelineLifecycleStage;
import com.streamsets.pipeline.api.ProtoSource;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StatsAggregatorStage;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ErrorStage;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Executor;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.service.ServiceConfiguration;
import com.streamsets.pipeline.api.service.ServiceDependency;
import org.apache.commons.lang3.ClassUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class StageDefinitionExtractor {

  private static final StageDefinitionExtractor EXTRACTOR = new StageDefinitionExtractor() {};

  public static StageDefinitionExtractor get() {
    return EXTRACTOR;
  }

  static String getStageName(Class klass) {
    return klass.getName().replace(".", "_").replace("$", "_");
  }

  public static List<String> getGroups(Class klass) {
    Set<String> set = new LinkedHashSet<>();
    addGroupsToList(klass, set);
    List<Class<?>> allSuperclasses = ClassUtils.getAllSuperclasses(klass);
    for(Class<?> superClass : allSuperclasses) {
      if(!superClass.isInterface() && superClass.isAnnotationPresent(ConfigGroups.class)) {
        addGroupsToList(superClass, set);
      }
    }
    if(set.isEmpty()) {
      set.add(""); // the default empty group
    }

    return new ArrayList<>(set);
  }

  @SuppressWarnings("unchecked")
  private static void addGroupsToList(Class<?> klass, Set<String> set) {
    ConfigGroups groups = klass.getAnnotation(ConfigGroups.class);
    if (groups != null) {
      Class<? extends Enum> groupKlass = (Class<? extends Enum>) groups.value();
      for (Enum e : groupKlass.getEnumConstants()) {
        set.add(e.name());
      }
    }
  }

  public List<ErrorMessage> validate(StageLibraryDefinition libraryDef, Class<? extends Stage> klass, Object contextMsg) {
    List<ErrorMessage> errors = new ArrayList<>();
    contextMsg = Utils.formatL("{} Stage='{}'", contextMsg, klass.getSimpleName());

    StageDef sDef = klass.getAnnotation(StageDef.class);
    if (sDef == null) {
      errors.add(new ErrorMessage(DefinitionError.DEF_300, contextMsg));
    } else {
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
      if (OffsetCommitter.class.isAssignableFrom(klass) && !Source.class.isAssignableFrom(klass)) {
        errors.add(new ErrorMessage(DefinitionError.DEF_314, contextMsg));
      }
      if (OffsetCommitTrigger.class.isAssignableFrom(klass) && type != StageType.TARGET) {
        errors.add(new ErrorMessage(DefinitionError.DEF_312, contextMsg));
      }

      HideConfigs hideConfigs = klass.getAnnotation(HideConfigs.class);

      List<String> stageGroups = getGroups(klass);

      List<ErrorMessage> configGroupErrors = ConfigGroupExtractor.get().validate(klass, contextMsg);
      errors.addAll(configGroupErrors);
      errors.addAll(ConfigGroupExtractor.get().validate(klass, contextMsg));

      List<ErrorMessage> configErrors = ConfigDefinitionExtractor.get().validate(klass, stageGroups, contextMsg);
      errors.addAll(configErrors);

      List<ErrorMessage> rawSourceErrors = RawSourceDefinitionExtractor.get().validate(klass, contextMsg);
      errors.addAll(rawSourceErrors);
      if (type != null && rawSourceErrors.isEmpty() && type != StageType.SOURCE) {
        if (RawSourceDefinitionExtractor.get().extract(klass, contextMsg) != null) {
          errors.add(new ErrorMessage(DefinitionError.DEF_304, contextMsg));
        }
      }

      if (!sDef.outputStreams().isEnum()) {
        errors.add(new ErrorMessage(DefinitionError.DEF_305, contextMsg, sDef.outputStreams().getSimpleName()));
      }

      if (type != null && sDef.outputStreams() != StageDef.DefaultOutputStreams.class && type.isOneOf(StageType.TARGET, StageType.EXECUTOR)) {
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
        List<ConfigDefinition> configDefs = extractConfigDefinitions(libraryDef, klass, hideConfigs, errors, contextMsg);
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
      try {
        contextMsg = Utils.formatL("{} Stage='{}'", contextMsg, klass.getSimpleName());

        StageDef sDef = klass.getAnnotation(StageDef.class);
        String name = getStageName(klass);
        int version = sDef.version();
        String label = sDef.label();
        String description = sDef.description();
        String icon = sDef.icon();
        StageType type = extractStageType(klass);
        boolean errorStage = klass.getAnnotation(ErrorStage.class) != null;
        boolean statsAggregatorStage = klass.getAnnotation(StatsAggregatorStage.class) != null;
        boolean pipelineLifecycleStage = klass.getAnnotation(PipelineLifecycleStage.class) != null;
        HideConfigs hideConfigs = klass.getAnnotation(HideConfigs.class);
        boolean preconditions = !errorStage && type != StageType.SOURCE &&
            ((hideConfigs == null) || !hideConfigs.preconditions());
        boolean onRecordError = !errorStage && ((hideConfigs == null) || !hideConfigs.onErrorRecord());
        List<ConfigDefinition> configDefinitions = extractConfigDefinitions(libraryDef, klass, hideConfigs, new ArrayList<ErrorMessage>(), contextMsg);
        RawSourceDefinition rawSourceDefinition = RawSourceDefinitionExtractor.get().extract(klass, contextMsg);
        ConfigGroupDefinition configGroupDefinition = ConfigGroupExtractor.get().extract(klass, contextMsg);
        String outputStreamLabelProviderClass = (!type.isOneOf(StageType.TARGET, StageType.EXECUTOR)) ? sDef.outputStreams().getName() : null;
        boolean variableOutputStreams = StageDef.VariableOutputStreams.class.isAssignableFrom(sDef.outputStreams());
        int outputStreams = (variableOutputStreams || type.isOneOf(StageType.TARGET, StageType.EXECUTOR) )
            ? 0 : sDef.outputStreams().getEnumConstants().length;
        List<ExecutionMode> executionModes = ImmutableList.copyOf(sDef.execution());
        List<ExecutionMode> executionModesLibraryOverride = libraryDef.getStageExecutionModesOverride(klass);
        if (executionModesLibraryOverride != null) {
          executionModes = executionModesLibraryOverride;
        }
        List<String> libJarsRegex = ImmutableList.copyOf(sDef.libJarsRegex());
        boolean recordsByRef = sDef.recordsByRef();
        List<ServiceDependencyDefinition> services = extractServiceDependencies(sDef);

        // If not a stage library, then dont add stage system configs
        if (!PipelineBeanCreator.PIPELINE_LIB_DEFINITION.equals(libraryDef.getName())) {
          List<ConfigDefinition> systemConfigs =
            ConfigDefinitionExtractor.get().extract(StageConfigBean.class, Collections.<String> emptyList(),
              "systemConfigs");

          for (ConfigDefinition def : systemConfigs) {
            switch (def.getName()) {
              case StageConfigBean.STAGE_PRECONDITIONS_CONFIG:
              case StageConfigBean.STAGE_REQUIRED_FIELDS_CONFIG:
                if (preconditions) {
                  configDefinitions.add(def);
                }
                break;
              case StageConfigBean.STAGE_ON_RECORD_ERROR_CONFIG:
                if (onRecordError) {
                  configDefinitions.add(def);
                }
                break;
              default:
                configDefinitions.add(def);
            }
          }
        }

        for (ConfigDefinition cDef : configDefinitions) {
          cDef.addAutoELDefinitions(libraryDef);
        }

        // This is on purpose and made difficult. The property will be:
        // -Dcom.streamsets.pipeline.stage.destination.hdfs.HdfsDTarget.no.private.classloader
        boolean privateClassLoader =
            sDef.privateClassLoader() &&
                System.getProperty(klass.getCanonicalName() + ".no.private.classloader") == null;

        StageUpgrader upgrader;
        try {
          upgrader = sDef.upgrader().newInstance();
        } catch (Exception ex) {
          throw new IllegalArgumentException(Utils.format(
              "Could not instantiate StageUpgrader for StageDefinition '{}': {}", name, ex.toString(), ex));
        }

        boolean resetOffset = sDef.resetOffset();


        String onlineHelpRefUrl = sDef.onlineHelpRefUrl();

        boolean offsetCommitController = (type == StageType.TARGET) &&
          OffsetCommitTrigger.class.isAssignableFrom(klass);
        boolean producesEvents = sDef.producesEvents();

        return new StageDefinition(
            libraryDef,
            privateClassLoader,
            klass,
            name,
            version,
            label,
            description,
            type,
            errorStage,
            preconditions,
            onRecordError,
            configDefinitions,
            rawSourceDefinition,
            icon,
            configGroupDefinition,
            variableOutputStreams,
            outputStreams,
            outputStreamLabelProviderClass,
            executionModes,
            recordsByRef,
            upgrader,
            libJarsRegex,
            resetOffset,
            onlineHelpRefUrl,
            statsAggregatorStage,
            pipelineLifecycleStage,
            offsetCommitController,
            producesEvents,
            services
        );
      } catch (Exception e) {
        throw new IllegalStateException("Exception while extracting stage definition for " + getStageName(klass), e);
      }

    } else {
      throw new IllegalArgumentException(Utils.format("Invalid StageDefinition: {}", errors));
    }
  }

  private List<ConfigDefinition> extractConfigDefinitions(StageLibraryDefinition libraryDef,
      Class<? extends Stage> klass, HideConfigs hideConfigs, List<ErrorMessage> errors, Object contextMsg) {

    List<String> stageGroups = getGroups(klass);

    List<ConfigDefinition> cDefs = ConfigDefinitionExtractor.get().extract(klass, stageGroups, contextMsg);

    Set<String> hideConfigSet = (hideConfigs != null) ?
      new HashSet<>(Arrays.asList(hideConfigs.value())) :
      Collections.<String>emptySet();

    if (!hideConfigSet.isEmpty()) {
      Iterator<ConfigDefinition> iterator = cDefs.iterator();
      while (iterator.hasNext()) {
        ConfigDefinition current = iterator.next();
        if(hideConfigSet.contains(current.getName())) {
          iterator.remove();
          hideConfigSet.remove(current.getName());
        }
      }

      if(!hideConfigSet.isEmpty()) {
        for(String toHide : hideConfigSet) {
          errors.add(new ErrorMessage(DefinitionError.DEF_313, contextMsg, toHide));
        }
      }
    }
    return cDefs;
  }

  private StageType extractStageType(Class<? extends Stage> klass) {
    StageType type;
    if (ProtoSource.class.isAssignableFrom(klass)) {
      type = StageType.SOURCE;
    } else if (Processor.class.isAssignableFrom(klass)) {
      type = StageType.PROCESSOR;
    } else if (Executor.class.isAssignableFrom(klass)) {
      type = StageType.EXECUTOR;
    } else if (Target.class.isAssignableFrom(klass)) {
      type = StageType.TARGET;
    } else if (PipelineConfigBean.class.isAssignableFrom(klass) ||
        RuleDefinitionsConfigBean.class.isAssignableFrom(klass)) {
      type = StageType.PIPELINE;
    } else {
      type = null;
    }
    return type;
  }

  private List<ServiceDependencyDefinition> extractServiceDependencies(StageDef stageDef) {
    List<ServiceDependencyDefinition> services = new LinkedList<>();
    for (ServiceDependency dependency : stageDef.services()) {
      Map<String, String> configuration = new HashMap<>();
      for (ServiceConfiguration conf : dependency.configuration()) {
        configuration.put(conf.name(), conf.value());
      }

      services.add(new ServiceDependencyDefinition(dependency.service(), configuration));
    }
    return services;
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
