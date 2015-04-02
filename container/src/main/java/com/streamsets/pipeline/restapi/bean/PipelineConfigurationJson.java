/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi.bean;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.validation.Issues;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PipelineConfigurationJson {

  private final com.streamsets.pipeline.config.PipelineConfiguration pipelineConfiguration;

  @SuppressWarnings("unchecked")
  public PipelineConfigurationJson(
    @JsonProperty("schemaVersion") int schemaVersion,
    @JsonProperty("uuid") UUID uuid,
    @JsonProperty("configuration") List<ConfigConfigurationJson> configuration,
    @JsonProperty("uiInfo") Map<String, Object> uiInfo,
    @JsonProperty("stages") List<StageConfigurationJson> stages,
    @JsonProperty("errorStage") StageConfigurationJson errorStage) {
    this.pipelineConfiguration = new com.streamsets.pipeline.config.PipelineConfiguration(schemaVersion, uuid,
      BeanHelper.unwrapConfigConfiguration(configuration), uiInfo, BeanHelper.unwrapStageConfigurations(stages),
      BeanHelper.unwrapStageConfiguration(errorStage));
  }

  public PipelineConfigurationJson(com.streamsets.pipeline.config.PipelineConfiguration pipelineConfiguration) {
    Utils.checkNotNull(pipelineConfiguration, "pipelineConfiguration");
    this.pipelineConfiguration = pipelineConfiguration;
  }

  public int getSchemaVersion() {
    return pipelineConfiguration.getSchemaVersion();
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

  public UUID getUuid() {
    return pipelineConfiguration.getUuid();
  }

  public Issues getIssues() {
    return pipelineConfiguration.getIssues();
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

  @JsonIgnore
  public com.streamsets.pipeline.config.PipelineConfiguration getPipelineConfiguration() {
    return pipelineConfiguration;
  }
}