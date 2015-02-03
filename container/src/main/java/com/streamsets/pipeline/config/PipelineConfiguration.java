/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.store.PipelineInfo;
import com.streamsets.pipeline.validation.Issues;
import com.streamsets.pipeline.validation.PipelineConfigurationValidator;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PipelineConfiguration {
  private int schemaVersion;
  private UUID uuid = null;
  private PipelineInfo info;
  private String description;
  private List<ConfigConfiguration> configuration;
  private final Map<String, Object> uiInfo;
  private List<StageConfiguration> stages;
  private Issues issues;
  private boolean previewable;

  @SuppressWarnings("unchecked")
  public PipelineConfiguration(
      @JsonProperty("schemaVersion") int schemaVersion,
      @JsonProperty("uuid") UUID uuid,
      @JsonProperty("configuration") List<ConfigConfiguration> configuration,
      @JsonProperty("uiInfo") Map<String, Object> uiInfo,
      @JsonProperty("stages") List<StageConfiguration> stages) {
    this.schemaVersion = schemaVersion;
    this.uuid = Preconditions.checkNotNull(uuid, "uuid cannot be null");
    this.configuration = configuration;
    this.uiInfo = uiInfo;
    this.stages = (stages != null) ? stages : Collections.EMPTY_LIST;
    issues = new Issues();
  }

  public void setInfo(PipelineInfo info) {
    //NOP, just for jackson
  }

  @JsonIgnore
  public void setPipelineInfo(PipelineInfo info) {
    this.info = info;
  }

  public int getSchemaVersion() {
    return schemaVersion;
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

  public List<ConfigConfiguration> getConfiguration() {
    return configuration;
  }

  public Map<String, Object> getUiInfo() {
    return uiInfo;
  }

  @Override
  public String toString() {
    return Utils.format("PipelineConfiguration[uuid='{}' valid='{}' previewable='{}' configuration='{}']", getUuid(),
                        isValid(), isPreviewable(), getConfiguration());
  }

}
