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
package com.streamsets.datacollector.restapi.bean;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.pipeline.api.ExecutionMode;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class StageDefinitionJson {

  private final com.streamsets.datacollector.config.StageDefinition stageDefinition;

  public StageDefinitionJson(com.streamsets.datacollector.config.StageDefinition stageDefinition) {
    this.stageDefinition = stageDefinition;
  }

  public ConfigGroupDefinitionJson getConfigGroupDefinition() {
    return BeanHelper.wrapConfigGroupDefinition(stageDefinition.getConfigGroupDefinition());
  }

  public boolean isPrivateClassLoader() {
    return stageDefinition.isPrivateClassLoader();
  }

  public boolean isResetOffset() {
    return stageDefinition.isResetOffset();
  }

  public String getClassName() {
    return stageDefinition.getClassName();
  }

  public String getName() {
    return stageDefinition.getName();
  }

  public String getVersion() {
    return Integer.toString(stageDefinition.getVersion());
  }

  public String getLabel() {
    return stageDefinition.getLabel();
  }

  public RawSourceDefinitionJson getRawSourceDefinition() {
    return BeanHelper.wrapRawSourceDefinition(stageDefinition.getRawSourceDefinition());
  }

  public String getDescription() {
    return stageDefinition.getDescription();
  }

  public StageTypeJson getType() {
    return BeanHelper.wrapStageType(stageDefinition.getType());
  }

  public boolean isErrorStage() {
    return stageDefinition.isErrorStage();
  }

  public boolean isStatsAggregatorStage() {
    return stageDefinition.isStatsAggregatorStage();
  }

  public boolean isPipelineLifecycleStage() {
    return stageDefinition.isPipelineLifecycleStage();
  }

  public boolean isOffsetCommitTrigger() {
    return stageDefinition.isOffsetCommitTrigger();
  }

  @JsonProperty("preconditions")
  public boolean hasRequiredFields() {
    return stageDefinition.hasPreconditions();
  }

  @JsonProperty("onRecordError")
  public boolean hasOnRecordError() {
    return stageDefinition.hasOnRecordError();
  }

  public List<ConfigDefinitionJson> getConfigDefinitions() {
    return BeanHelper.wrapConfigDefinitions(stageDefinition.getConfigDefinitions());
  }

  public String getIcon() {
    return stageDefinition.getIcon();
  }

  public boolean isVariableOutputStreams() {
    return stageDefinition.isVariableOutputStreams();
  }

  public int getOutputStreams() {
    return stageDefinition.getOutputStreams();
  }

  public String getOutputStreamLabelProviderClass() {
    return stageDefinition.getOutputStreamLabelProviderClass();
  }

  public String getLibrary() {
    return stageDefinition.getLibrary();
  }

  public String getLibraryLabel() {
    return stageDefinition.getLibraryLabel();
  }

  public List<String> getOutputStreamLabels() {
    return stageDefinition.getOutputStreamLabels();
  }

  public List<ExecutionMode> getExecutionModes() {
    return stageDefinition.getExecutionModes();
  }

  public String getOnlineHelpRefUrl() {
    return stageDefinition.getOnlineHelpRefUrl();
  }

  @JsonProperty("producingEvents")
  public boolean isProducingEvents() {
    return stageDefinition.isProducingEvents();
  }

  public List<ServiceDependencyDefinitionJson> getServices() {
    return BeanHelper.wrapServiceDependencyDefinitions(stageDefinition.getServices());
  }
}
