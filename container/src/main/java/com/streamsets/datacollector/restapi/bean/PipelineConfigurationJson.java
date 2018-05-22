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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.pipeline.api.impl.Utils;
import org.apache.commons.collections.CollectionUtils;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PipelineConfigurationJson implements Serializable {

  private final PipelineConfiguration pipelineConfiguration;

  @SuppressWarnings("unchecked")
  public PipelineConfigurationJson(
    @JsonProperty("schemaVersion") int schemaVersion,
    @JsonProperty("version") int version,
    @JsonProperty("pipelineId") String pipelineId,
    @JsonProperty("title") String title,
    @JsonProperty("description") String description,
    @JsonProperty("uuid") UUID uuid,
    @JsonProperty("configuration") List<ConfigConfigurationJson> configuration,
    @JsonProperty("uiInfo") Map<String, Object> uiInfo,
    @JsonProperty("fragments") List<PipelineFragmentConfigurationJson> fragments,
    @JsonProperty("stages") List<StageConfigurationJson> stages,
    @JsonProperty("errorStage") StageConfigurationJson errorStage,
    @JsonProperty("info") PipelineInfoJson pipelineInfo,
    @JsonProperty("metadata") Map<String, Object> metadata,
    @JsonProperty("statsAggregatorStage") StageConfigurationJson statsAggregatorStage,
    @JsonProperty("startEventStages") List<StageConfigurationJson> startEventStages,
    @JsonProperty("stopEventStages") List<StageConfigurationJson> stopEventStages,
    @JsonProperty("testOriginStage") StageConfigurationJson testOriginStage
  ) {
    version = (version == 0) ? 1 : version;
    this.pipelineConfiguration = new PipelineConfiguration(
        schemaVersion,
        version,
        pipelineId,
        uuid,
        title,
        description,
        BeanHelper.unwrapConfigConfiguration(configuration),
        uiInfo,
        BeanHelper.unwrapPipelineFragementConfigurations(fragments),
        BeanHelper.unwrapStageConfigurations(stages),
        BeanHelper.unwrapStageConfiguration(errorStage),
        BeanHelper.unwrapStageConfiguration(statsAggregatorStage),
        BeanHelper.unwrapStageConfigurations(startEventStages),
        BeanHelper.unwrapStageConfigurations(stopEventStages),
        BeanHelper.unwrapStageConfiguration(testOriginStage)
    );
    this.pipelineConfiguration.setPipelineInfo(BeanHelper.unwrapPipelineInfo(pipelineInfo));
    this.pipelineConfiguration.setMetadata(metadata);
  }

  public PipelineConfigurationJson(PipelineConfiguration pipelineConfiguration) {
    Utils.checkNotNull(pipelineConfiguration, "pipelineConfiguration");
    this.pipelineConfiguration = pipelineConfiguration;
  }

  public int getSchemaVersion() {
    return pipelineConfiguration.getSchemaVersion();
  }

  public String getPipelineId() {
    return pipelineConfiguration.getPipelineId();
  }

  public int getVersion() {
    return pipelineConfiguration.getVersion();
  }

  public String getTitle() {
    if (pipelineConfiguration.getTitle() == null) {
      return pipelineConfiguration.getInfo().getPipelineId();
    }
    return pipelineConfiguration.getTitle();
  }

  public String getDescription() {
    return pipelineConfiguration.getDescription();
  }

  public PipelineInfoJson getInfo() {
    return BeanHelper.wrapPipelineInfo(pipelineConfiguration.getInfo());
  }

  public List<PipelineFragmentConfigurationJson> getFragments() {
    return BeanHelper.wrapPipelineFragmentConfigurations(pipelineConfiguration.getFragments());
  }

  public List<StageConfigurationJson> getStages() {
    if (CollectionUtils.isEmpty(pipelineConfiguration.getFragments())) {
      return BeanHelper.wrapStageConfigurations(pipelineConfiguration.getStages());
    } else {
      // update original stages
      List<StageConfiguration> originalStages = pipelineConfiguration.getOriginalStages()
          .stream()
          .map(stageConfiguration -> pipelineConfiguration.getStages()
              .stream()
              .filter(upgraded -> upgraded.getInstanceName().equals(stageConfiguration.getInstanceName()))
              .findFirst()
              .orElse(stageConfiguration)
          )
          .collect(Collectors.toList());
      return BeanHelper.wrapStageConfigurations(originalStages);
    }
  }

  public StageConfigurationJson getErrorStage() {
    return BeanHelper.wrapStageConfiguration(pipelineConfiguration.getErrorStage());
  }

  public StageConfigurationJson getStatsAggregatorStage() {
    return BeanHelper.wrapStageConfiguration(pipelineConfiguration.getStatsAggregatorStage());
  }

  public UUID getUuid() {
    return pipelineConfiguration.getUuid();
  }

  public IssuesJson getIssues() {
    return BeanHelper.wrapIssues(pipelineConfiguration.getIssues());
  }

  public boolean isValid() {
    return pipelineConfiguration.isValid();
  }

  public boolean isPreviewable() {
    return pipelineConfiguration.isPreviewable();
  }

  public List<ConfigConfigurationJson> getConfiguration() {
    return BeanHelper.wrapConfigConfiguration(pipelineConfiguration.getConfiguration());
  }

  public Map<String, Object> getUiInfo() {
    return pipelineConfiguration.getUiInfo();
  }

  public Map<String, Object> getMetadata() {
    return pipelineConfiguration.getMetadata();
  }

  public List<StageConfigurationJson> getStartEventStages() {
    return BeanHelper.wrapStageConfigurations(pipelineConfiguration.getStartEventStages());
  }

  public List<StageConfigurationJson> getStopEventStages() {
    return BeanHelper.wrapStageConfigurations(pipelineConfiguration.getStopEventStages());
  }

  public StageConfigurationJson getTestOriginStage() {
    return BeanHelper.wrapStageConfiguration(pipelineConfiguration.getTestOriginStage());
  }

  @JsonIgnore
  public PipelineConfiguration getPipelineConfiguration() {
    return pipelineConfiguration;
  }
}
