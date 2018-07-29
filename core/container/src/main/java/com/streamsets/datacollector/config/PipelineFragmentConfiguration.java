/*
 * Copyright 2018 StreamSets Inc.
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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.store.PipelineInfo;
import com.streamsets.datacollector.validation.Issues;
import com.streamsets.datacollector.validation.PipelineFragmentConfigurationValidator;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.impl.Utils;
import org.apache.commons.collections.CollectionUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class PipelineFragmentConfiguration implements Serializable {
  protected final Map<String, Object> uiInfo;
  protected int schemaVersion;
  protected int version;
  protected String pipelineId;
  protected String fragmentInstanceId;
  protected UUID uuid;
  protected String title;
  protected String description;
  protected List<PipelineFragmentConfiguration> fragments;
  protected List<StageConfiguration> stages;
  protected List<StageConfiguration> resolvedStages; // resolved stages from fragments
  protected Issues issues;
  protected List<Config> configuration;
  protected Map<String, Object> metadata;
  private PipelineInfo info;
  protected boolean previewable;
  protected StageConfiguration testOriginStage;

  public static final String FRAGMENT_SOURCE_STAGE_NAME =
      "com_streamsets_pipeline_stage_origin_fragment_FragmentSource";
  public static final String FRAGMENT_PROCESSOR_STAGE_NAME =
      "com_streamsets_pipeline_stage_processor_fragment_FragmentProcessor";
  public static final String FRAGMENT_TARGET_STAGE_NAME =
      "com_streamsets_pipeline_stage_destination_fragment_FragmentTarget";
  public static final String CONF_FRAGMENT_ID= "conf.fragmentId";
  public static final String CONF_FRAGMENT_INSTANCE_ID= "conf.fragmentInstanceId";

  private static final List<String> FRAGMENT_STAGE_NAMES = ImmutableList.of(
      FRAGMENT_SOURCE_STAGE_NAME,
      FRAGMENT_PROCESSOR_STAGE_NAME,
      FRAGMENT_TARGET_STAGE_NAME
  );

  public PipelineFragmentConfiguration(
      UUID uuid,
      int version,
      int schemaVersion,
      String title,
      String pipelineId,
      String fragmentInstanceId,
      String description,
      List<PipelineFragmentConfiguration> fragments,
      List<StageConfiguration> stages,
      Map<String, Object> uiInfo,
      List<Config> configuration,
      StageConfiguration testOriginStage
  ) {
    this.uuid = Preconditions.checkNotNull(uuid, "uuid cannot be null");
    this.version = version;
    issues = new Issues();
    this.schemaVersion = schemaVersion;
    this.title = title;
    this.pipelineId = pipelineId;
    this.fragmentInstanceId = fragmentInstanceId;
    this.description = description;
    this.fragments = Optional.ofNullable(fragments).orElse(Collections.emptyList());
    this.stages = (stages != null) ? stages : Collections.emptyList();
    this.uiInfo = (uiInfo != null) ? new HashMap<>(uiInfo) : new HashMap<>();
    this.configuration = new ArrayList<>(configuration);
    this.testOriginStage = testOriginStage;
    this.processStages();
  }

  public void setInfo(PipelineInfo info) {
    this.info = info;
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

  public String getFragmentInstanceId() {
    return fragmentInstanceId;
  }

  public PipelineFragmentConfiguration setFragmentInstanceId(String fragmentInstanceId) {
    this.fragmentInstanceId = fragmentInstanceId;
    return this;
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

  public List<PipelineFragmentConfiguration> getFragments() {
    return fragments;
  }

  public void setFragments(List<PipelineFragmentConfiguration> fragments) {
    this.fragments = fragments;
  }

  public List<StageConfiguration> getStages() {
    return resolvedStages;
  }

  public List<StageConfiguration> getOriginalStages() {
    return stages;
  }

  public void setStages(List<StageConfiguration> stages) {
    this.resolvedStages = stages;
  }

  public void setOriginalStages(List<StageConfiguration> stages) {
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

  public void setValidation(PipelineFragmentConfigurationValidator validation) {
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

  public Map<String, Object> getUiInfo() {
    return uiInfo;
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

  public StageConfiguration getTestOriginStage() {
    return testOriginStage;
  }

  public void setTestOriginStage(StageConfiguration testOriginStage) {
    this.testOriginStage = testOriginStage;
  }

  @Override
  public String toString() {
    return Utils.format("PipelineConfiguration[version='{}' uuid='{}' valid='{}' previewable='{}' configuration='{}']",
                        getVersion(), getUuid(), isValid(), isPreviewable(), getConfiguration());
  }

  public Map<String, Object> getMetadata() {
    return metadata;
  }

  public void setMetadata(Map<String, Object> metadata) {
    this.metadata = metadata;
  }

  private void processStages() {
    if (CollectionUtils.isEmpty(fragments)) {
      this.resolvedStages = stages;
    } else {
      this.resolvedStages = new ArrayList<>();
      this.stages.forEach(stageInstance -> {
        if (isFragmentStage(stageInstance)) {
          resolvedStages.addAll(this.getFragmentStages(stageInstance));
        } else {
          resolvedStages.add(stageInstance);
        }
      });
    }
  }

  private boolean isFragmentStage(StageConfiguration stageInstance) {
    return FRAGMENT_STAGE_NAMES.contains(stageInstance.getStageName());
  }

  private List<StageConfiguration> getFragmentStages(StageConfiguration stageInstance) {
    Config fragmentIdConfig = stageInstance.getConfig(CONF_FRAGMENT_ID);
    Config fragmentInstanceIdConfig = stageInstance.getConfig(CONF_FRAGMENT_INSTANCE_ID);

    if (fragmentIdConfig != null && fragmentInstanceIdConfig != null) {
      String fragmentId = (String) fragmentIdConfig.getValue();
      String fragmentInstanceId = (String) fragmentInstanceIdConfig.getValue();

      PipelineFragmentConfiguration fragment = getFragments()
          .stream()
          .filter(f -> f.getFragmentInstanceId().equals(fragmentInstanceId) && f.getPipelineId().equals(fragmentId))
          .findFirst()
          .orElse(null);

      if (fragment != null) {
        return fragment.getStages();
      }
    }
    return Collections.emptyList();
  }

}
