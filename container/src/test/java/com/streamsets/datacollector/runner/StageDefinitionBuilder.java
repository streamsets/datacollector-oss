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
package com.streamsets.datacollector.runner;

import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.config.ConfigDefinition;
import com.streamsets.datacollector.config.ConfigGroupDefinition;
import com.streamsets.datacollector.config.RawSourceDefinition;
import com.streamsets.datacollector.config.ServiceDependencyDefinition;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.config.StageLibraryDefinition;
import com.streamsets.datacollector.config.StageType;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.ProtoSource;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.Executor;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 */
public class StageDefinitionBuilder {
  StageLibraryDefinition libraryDefinition;
  boolean privateClassLoader = false;
  Class<? extends Stage> klass;
  String name;
  int version = 1;
  String label;
  String description;
  StageType type;
  boolean errorStage = false;
  boolean preconditions = true;
  boolean onRecordError = true;
  List<ConfigDefinition> configDefinitions = Collections.emptyList();
  RawSourceDefinition rawSourceDefinition = null;
  String icon = "";
  ConfigGroupDefinition configGroupDefinition = null;
  boolean variableOutputStreams = false;
  int outputStreams;
  String outputStreamLabelProviderClass = null;
  List<ExecutionMode> executionModes = Arrays.asList(ExecutionMode.CLUSTER_YARN_STREAMING, ExecutionMode.STANDALONE, ExecutionMode.CLUSTER_BATCH);
  boolean recordsByRef = false;
  StageUpgrader upgrader = new StageUpgrader.Default();
  List<String> libJarsRegex = Collections.emptyList();
  boolean resetOffset = false;
  String onlineHelpRefUrl = "";
  boolean statsAggregatorStage = false;
  boolean pipelineLifecycleStage = false;
  boolean offsetCommitTrigger = false;
  boolean producesEvents = false;
  List<ServiceDependencyDefinition> services = Collections.emptyList();

  public StageDefinitionBuilder(ClassLoader cl, Class<? extends Stage> klass, String name) {
    this.libraryDefinition = createLibraryDef(cl);
    this.klass = klass;
    this.name = name;
    this.label = name + "Label";
    this.description = name + "Description";
    this.type = autoDetectStageType(klass);
    this.outputStreams = type.isOneOf(StageType.TARGET, StageType.EXECUTOR) ? 0 : 1;
  }

  public StageDefinitionBuilder withLabel(String label) {
    this.label = label;
    return this;
  }

  public StageDefinitionBuilder withDescription(String description) {
    this.description = description;
    return this;
  }

  public StageDefinitionBuilder withErrorStage(boolean errorStage) {
    this.errorStage = errorStage;
    return this;
  }

  public StageDefinitionBuilder withPreconditions(boolean preconditions) {
    this.preconditions = preconditions;
    return  this;
  }

  public StageDefinitionBuilder withConfig(ConfigDefinition ... config) {
    this.configDefinitions = Arrays.asList(config);
    return this;
  }

  public StageDefinitionBuilder withConfig(List<ConfigDefinition> config) {
    this.configDefinitions = config;
    return this;
  }

  public StageDefinitionBuilder withRawSourceDefintion(RawSourceDefinition def) {
    this.rawSourceDefinition = def;
    return this;
  }

  public StageDefinitionBuilder withConfigGroupDefintion(ConfigGroupDefinition groupDefintion) {
    this.configGroupDefinition = groupDefintion;
    return this;
  }

  public StageDefinitionBuilder withOutputStreams(int outputStreams) {
    this.outputStreams = outputStreams;
    return this;
  }

  public StageDefinitionBuilder withOutputStreamLabelProviderClass(String outputStreamLabelProviderClass) {
    this.outputStreamLabelProviderClass = outputStreamLabelProviderClass;
    return this;
  }

  public StageDefinitionBuilder withExecutionModes(ExecutionMode ...modes) {
    this.executionModes = Arrays.asList(modes);
    return this;
  }

  public StageDefinitionBuilder withLibJarsRegexp(String ... libJarsRegexp) {
    this.libJarsRegex = Arrays.asList(libJarsRegexp);
    return this;
  }

  public StageDefinitionBuilder withStatsAggregatorStage(boolean statsAggregatorStage) {
    this.statsAggregatorStage = statsAggregatorStage;
    return this;
  }

  public StageDefinitionBuilder withPipelineLifecycleStage(boolean pipelineLifecycleStage) {
    this.pipelineLifecycleStage = pipelineLifecycleStage;
    return this;
  }

  public StageDefinitionBuilder withOffsetCommitTrigger(boolean offsetCommitTrigger) {
    this.offsetCommitTrigger = offsetCommitTrigger;
    return this;
  }

  public StageDefinitionBuilder withProducingEvents(boolean producesEvents) {
    this.producesEvents = producesEvents;
    return this;
  }

  public StageDefinitionBuilder withServices(ServiceDependencyDefinition ...defs) {
    this.services = Arrays.asList(defs);
    return this;
  }

  public StageDefinition build() {
    return new StageDefinition(
      libraryDefinition,
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
      offsetCommitTrigger,
      producesEvents,
      services
    );
  }

  private static StageType autoDetectStageType(Class klass) {
    if(ProtoSource.class.isAssignableFrom(klass)) {
      return StageType.SOURCE;
    }
    if(Processor.class.isAssignableFrom(klass)) {
      return StageType.PROCESSOR;
    }
    if(Executor.class.isAssignableFrom(klass)) {
      return StageType.EXECUTOR;
    }
    if(Target.class.isAssignableFrom(klass)) {
      return StageType.TARGET;
    }

    throw new IllegalArgumentException("Don't know stage type for class: " + klass.getName());
  }

  public static final StageLibraryDefinition createLibraryDef(ClassLoader cl) {
    return new StageLibraryDefinition(cl, "default", "", new Properties(), null, null, null) {
      @Override
      public List<ExecutionMode> getStageExecutionModesOverride(Class klass) {
        return ImmutableList.copyOf(ExecutionMode.values());
      }
    };
  }

}
