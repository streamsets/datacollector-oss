/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import com.streamsets.pipeline.api.impl.Utils;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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
    @JsonProperty("stages") List<StageConfigurationJson> stages,
    @JsonProperty("errorStage") StageConfigurationJson errorStage,
    @JsonProperty("info") PipelineInfoJson pipelineInfo,
    @JsonProperty("metadata") Map<String, Object> metadata,
    @JsonProperty("statsAggregatorStage") StageConfigurationJson statsAggregatorStage
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
        BeanHelper.unwrapStageConfigurations(stages),
        BeanHelper.unwrapStageConfiguration(errorStage),
        BeanHelper.unwrapStageConfiguration(statsAggregatorStage)
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

  public List<StageConfigurationJson> getStages() {
    return BeanHelper.wrapStageConfigurations(pipelineConfiguration.getStages());
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

  @JsonIgnore
  public PipelineConfiguration getPipelineConfiguration() {
    return pipelineConfiguration;
  }
}
