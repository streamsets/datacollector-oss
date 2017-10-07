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
import com.google.common.base.Preconditions;
import com.streamsets.datacollector.store.PipelineInfo;
import com.streamsets.datacollector.validation.Issues;
import com.streamsets.datacollector.validation.PipelineConfigurationValidator;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.impl.Utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class PipelineConfiguration implements Serializable {
  private int schemaVersion;
  private int version;
  private String pipelineId;
  private UUID uuid = null;
  private PipelineInfo info;
  private String title;
  private String description;
  private List<Config> configuration;
  private final Map<String, Object> uiInfo;
  private List<StageConfiguration> stages;
  private StageConfiguration errorStage;
  private StageConfiguration statsAggregatorStage;
  private Issues issues;
  private boolean previewable;
  private MemoryLimitConfiguration memoryLimitConfiguration;
  private Map<String, Object> metadata;
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
    this.schemaVersion = schemaVersion;
    this.version = version;
    this.pipelineId = pipelineId;
    this.uuid = Preconditions.checkNotNull(uuid, "uuid cannot be null");
    this.title = title;
    this.description = description;
    this.configuration = new ArrayList<>(configuration);
    this.uiInfo = (uiInfo != null) ? new HashMap<>(uiInfo) : new HashMap<>();
    this.stages = (stages != null) ? stages : Collections.emptyList();
    this.errorStage = errorStage;
    this.statsAggregatorStage = statsAggregatorStage;
    issues = new Issues();
    memoryLimitConfiguration = new MemoryLimitConfiguration();
    this.startEventStages = startEventStages;
    this.stopEventStages = stopEventStages;
  }

  public void setInfo(PipelineInfo info) {
    //NOP, just for jackson
    // TODO - why is this a NOP? We really need this to be set correctly for embedded mode
  }

  @JsonIgnore
  public void setPipelineInfo(PipelineInfo info) {
    this.info = info;
  }

  public int getSchemaVersion() {
    return schemaVersion;
  }

  public void setSchemaVersion(int schemaVersion) {
    this.schemaVersion = schemaVersion;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  public int getVersion() {
    return version;
  }

  public String getPipelineId() {
    return pipelineId;
  }

  public void setPipelineId(String pipelineId) {
    this.pipelineId = pipelineId;
  }

  public String getTitle() {
    return title;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getDescription() {
    return description;
  }

  public PipelineInfo getInfo() {
    return info;
  }

  public List<StageConfiguration> getStages() {
    return stages;
  }

  public void setStages(List<StageConfiguration> stages) {
    this.stages = stages;
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

  public void setUuid(UUID uuid) {
    this.uuid = uuid;
  }

  public UUID getUuid() {
    return uuid;
  }

  public void setIssues(Issues issues) {
    //NOP, just for jackson
  }

  public void setValidation(PipelineConfigurationValidator validation) {
    issues = validation.getIssues();
    previewable = validation.canPreview();
  }

  public Issues getIssues() {
    return issues;
  }

  public void setValid(boolean dummy) {
    //NOP, just for jackson
  }
  public void setPreviewable(boolean dummy) {
    //NOP, just for jackson
  }

  public boolean isValid() {
    return (issues != null) && !issues.hasIssues();
  }

  public boolean isPreviewable() {
    return (issues !=null) && previewable;
  }

  public void setConfiguration(List<Config> configuration) {
    this.configuration = configuration;
  }

  public List<Config> getConfiguration() {
    return configuration;
  }

  public Config getConfiguration(String name) {
    for (Config config : configuration) {
      if (config.getName().equals(name)) {
        return config;
      }
    }
    return null;
  }

  public StageConfiguration getStatsAggregatorStage() {
    return statsAggregatorStage;
  }

  public void addConfiguration(Config config) {
    boolean found = false;
    for (int i = 0; !found && i < configuration.size(); i++) {
      if (configuration.get(i).getName().equals(config.getName())) {
        configuration.set(i, config);
        found = true;
      }
    }
    if (!found) {
      configuration.add(config);
    }
  }

  public Map<String, Object> getUiInfo() {
    return uiInfo;
  }

  public MemoryLimitConfiguration getMemoryLimitConfiguration() {
    return memoryLimitConfiguration;
  }

  @JsonIgnore
  public void setMemoryLimitConfiguration(MemoryLimitConfiguration memoryLimitConfiguration) {
    this.memoryLimitConfiguration = memoryLimitConfiguration;
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

  @Override
  public String toString() {
    return Utils.format("PipelineConfiguration[version='{}' uuid='{}' valid='{}' previewable='{}' configuration='{}']",
                        getVersion(), getUuid(), isValid(), isPreviewable(), getConfiguration());
  }


  @VisibleForTesting
  @JsonIgnore
  public PipelineConfiguration createWithNewConfig(Config replacement) {
    List<Config> newConfigurations = new ArrayList<>();
    for (Config candidate : this.configuration) {
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
        stages,
        errorStage,
        statsAggregatorStage,
        startEventStages,
        stopEventStages
    );
  }

  public Map<String, Object> getMetadata() {
    return metadata;
  }

  public void setMetadata(Map<String, Object> metadata) {
    this.metadata = metadata;
  }

}
