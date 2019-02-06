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
import com.google.common.annotations.VisibleForTesting;
import com.streamsets.datacollector.validation.PipelineConfigurationValidator;
import com.streamsets.pipeline.api.Config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class PipelineConfiguration extends PipelineFragmentConfiguration {
  private StageConfiguration errorStage;
  private StageConfiguration statsAggregatorStage;
  private List<StageConfiguration> startEventStages;
  private List<StageConfiguration> stopEventStages;

  @SuppressWarnings("unchecked")
  public PipelineConfiguration(
      int schemaVersion,
      int version,
      String pipelineId,
      UUID uuid,
      String title,
      String description,
      List<Config> configuration,
      Map<String, Object> uiInfo,
      List<StageConfiguration> stages,
      StageConfiguration errorStage,
      StageConfiguration statsAggregatorStage,
      List<StageConfiguration> startEventStages,
      List<StageConfiguration> stopEventStages
  ) {
    this(
        schemaVersion,
        version,
        pipelineId,
        uuid,
        title,
        description,
        configuration,
        uiInfo,
        null,
        stages,
        errorStage,
        statsAggregatorStage,
        startEventStages,
        stopEventStages,
        null
    );
  }

  @SuppressWarnings("unchecked")
  public PipelineConfiguration(
      int schemaVersion,
      int version,
      String pipelineId,
      UUID uuid,
      String title,
      String description,
      List<Config> configuration,
      Map<String, Object> uiInfo,
      List<PipelineFragmentConfiguration> fragments,
      List<StageConfiguration> stages,
      StageConfiguration errorStage,
      StageConfiguration statsAggregatorStage,
      List<StageConfiguration> startEventStages,
      List<StageConfiguration> stopEventStages,
      StageConfiguration testOriginStage
  ) {
    super(
        uuid,
        version,
        schemaVersion,
        title,
        pipelineId,
        null,
        description,
        fragments,
        stages,
        uiInfo,
        configuration,
        testOriginStage
    );
    this.errorStage = errorStage;
    this.statsAggregatorStage = statsAggregatorStage;
    this.startEventStages = startEventStages != null ? startEventStages : Collections.emptyList();
    this.stopEventStages = stopEventStages != null ? stopEventStages : Collections.emptyList();
  }

  public void setErrorStage(StageConfiguration errorStage) {
    this.errorStage = errorStage;
  }

  public void setStatsAggregatorStage(StageConfiguration statsAggregatorStage) {
    this.statsAggregatorStage = statsAggregatorStage;
  }

  public StageConfiguration getErrorStage() {
    return this.errorStage;
  }

  public StageConfiguration getStatsAggregatorStage() {
    return statsAggregatorStage;
  }

  public List<StageConfiguration> getStartEventStages() {
    return startEventStages;
  }

  public void setStartEventStages(List<StageConfiguration> startEventStages) {
    this.startEventStages = startEventStages;
  }

  public List<StageConfiguration> getStopEventStages() {
    return stopEventStages;
  }

  public void setStopEventStages(List<StageConfiguration> stopEventStages) {
    this.stopEventStages = stopEventStages;
  }

  public void setValidation(PipelineConfigurationValidator validation) {
    issues = validation.getIssues();
    previewable = validation.canPreview();
  }

  @VisibleForTesting
  @JsonIgnore
  public PipelineConfiguration createWithNewConfig(Config replacement) {
    List<Config> newConfigurations = new ArrayList<>();
    for (Config candidate : getConfiguration()) {
      if (replacement.getName().equals(candidate.getName())) {
        newConfigurations.add(replacement);
      } else {
        newConfigurations.add(candidate);
      }
    }
    return new PipelineConfiguration(
        schemaVersion,
        version,
        pipelineId,
        uuid,
        title,
        description,
        newConfigurations,
        uiInfo,
        fragments,
        stages,
        errorStage,
        statsAggregatorStage,
        startEventStages,
        stopEventStages,
        testOriginStage
    );
  }

}
