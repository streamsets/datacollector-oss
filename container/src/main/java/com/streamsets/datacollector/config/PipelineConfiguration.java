/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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

public class PipelineConfiguration implements Serializable{
  private int schemaVersion;
  private int version;
  private UUID uuid = null;
  private PipelineInfo info;
  private String description;
  private List<Config> configuration;
  private final Map<String, Object> uiInfo;
  private List<StageConfiguration> stages;
  private StageConfiguration errorStage;
  private Issues issues;
  private boolean previewable;
  private MemoryLimitConfiguration memoryLimitConfiguration;

  @SuppressWarnings("unchecked")
  public PipelineConfiguration(int schemaVersion, int version, UUID uuid, String description, List<Config> configuration,
      Map<String, Object> uiInfo, List<StageConfiguration> stages, StageConfiguration errorStage) {
    this.schemaVersion = schemaVersion;
    this.version = version;
    this.uuid = Preconditions.checkNotNull(uuid, "uuid cannot be null");
    this.description = description;
    this.configuration = configuration;
    this.uiInfo = (uiInfo != null) ? new HashMap<>(uiInfo) : new HashMap<String, Object>();
    this.stages = (stages != null) ? stages : Collections.<StageConfiguration>emptyList();
    this.errorStage = errorStage;
    issues = new Issues();
    memoryLimitConfiguration = new MemoryLimitConfiguration();
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

  public void setVersion(int version) {
    this.version = version;
  }

  public int getVersion() {
    return version;
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
    return new PipelineConfiguration(schemaVersion, version, uuid, description, newConfigurations, uiInfo, stages,
      errorStage);
  }
}
