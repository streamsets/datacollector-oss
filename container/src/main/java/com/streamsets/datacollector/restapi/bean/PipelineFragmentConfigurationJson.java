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
package com.streamsets.datacollector.restapi.bean;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.datacollector.config.PipelineFragmentConfiguration;
import com.streamsets.datacollector.config.StageConfiguration;
import org.apache.commons.collections.CollectionUtils;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PipelineFragmentConfigurationJson implements Serializable {

  private final PipelineFragmentConfiguration fragmentConfiguration;

  @JsonProperty("fragments")
  private List<PipelineFragmentConfigurationJson> fragments;

  @SuppressWarnings("unchecked")
  public PipelineFragmentConfigurationJson(
      @JsonProperty("schemaVersion") int schemaVersion,
      @JsonProperty("version") int version,
      @JsonProperty("fragmentId") String fragmentId,
      @JsonProperty("fragmentInstanceId") String fragmentInstanceId,
      @JsonProperty("title") String title,
      @JsonProperty("description") String description,
      @JsonProperty("uuid") UUID uuid,
      @JsonProperty("configuration") List<ConfigConfigurationJson> configuration,
      @JsonProperty("uiInfo") Map<String, Object> uiInfo,
      @JsonProperty("stages") List<StageConfigurationJson> stages,
      @JsonProperty("info") PipelineInfoJson pipelineInfo,
      @JsonProperty("metadata") Map<String, Object> metadata,
      @JsonProperty("testOriginStage") StageConfigurationJson testOriginStage
  ) {
    fragmentConfiguration = new PipelineFragmentConfiguration(
        uuid,
        version,
        schemaVersion,
        title,
        fragmentId,
        fragmentInstanceId,
        description,
        BeanHelper.unwrapPipelineFragementConfigurations(fragments),
        BeanHelper.unwrapStageConfigurations(stages),
        uiInfo,
        BeanHelper.unwrapConfigConfiguration(configuration),
        BeanHelper.unwrapStageConfiguration(testOriginStage)
    );
    this.fragmentConfiguration.setPipelineInfo(BeanHelper.unwrapPipelineInfo(pipelineInfo));
    this.fragmentConfiguration.setMetadata(metadata);
  }

  public PipelineFragmentConfigurationJson(PipelineFragmentConfiguration pipelineFragmentConfiguration) {
    this.fragmentConfiguration = pipelineFragmentConfiguration;
  }

  public int getSchemaVersion() {
    return fragmentConfiguration.getSchemaVersion();
  }

  public String getFragmentId() {
    return fragmentConfiguration.getPipelineId();
  }

  public String getFragmentInstanceId() {
    return fragmentConfiguration.getFragmentInstanceId();
  }

  public int getVersion() {
    return fragmentConfiguration.getVersion();
  }

  public String getTitle() {
    if (fragmentConfiguration.getTitle() == null) {
      return fragmentConfiguration.getInfo().getPipelineId();
    }
    return fragmentConfiguration.getTitle();
  }

  public String getDescription() {
    return fragmentConfiguration.getDescription();
  }

  public PipelineInfoJson getInfo() {
    return BeanHelper.wrapPipelineInfo(fragmentConfiguration.getInfo());
  }

  public List<PipelineFragmentConfigurationJson> getFragments() {
    return BeanHelper.wrapPipelineFragmentConfigurations(fragmentConfiguration.getFragments());
  }

  public void setFragments(List<PipelineFragmentConfigurationJson> fragments) {
    this.fragments = fragments;
  }

  public List<StageConfigurationJson> getStages() {
    if (CollectionUtils.isEmpty(fragments)) {
      return BeanHelper.wrapStageConfigurations(fragmentConfiguration.getStages());
    } else {
      // update original stages
      List<StageConfiguration> originalStages = fragmentConfiguration.getOriginalStages()
          .stream()
          .map(stageConfiguration -> fragmentConfiguration.getStages()
              .stream()
              .filter(upgraded -> upgraded.getInstanceName().equals(stageConfiguration.getInstanceName()))
              .findFirst()
              .orElse(stageConfiguration)
          )
          .collect(Collectors.toList());
      return BeanHelper.wrapStageConfigurations(originalStages);
    }
  }

  public UUID getUuid() {
    return fragmentConfiguration.getUuid();
  }

  public IssuesJson getIssues() {
    return BeanHelper.wrapIssues(fragmentConfiguration.getIssues());
  }

  public boolean isValid() {
    return fragmentConfiguration.isValid();
  }

  public boolean isPreviewable() {
    return fragmentConfiguration.isPreviewable();
  }

  public List<ConfigConfigurationJson> getConfiguration() {
    return BeanHelper.wrapConfigConfiguration(fragmentConfiguration.getConfiguration());
  }

  public Map<String, Object> getUiInfo() {
    return fragmentConfiguration.getUiInfo();
  }

  public Map<String, Object> getMetadata() {
    return fragmentConfiguration.getMetadata();
  }


  public StageConfigurationJson getTestOriginStage() {
    return BeanHelper.wrapStageConfiguration(fragmentConfiguration.getTestOriginStage());
  }

  @JsonIgnore
  public PipelineFragmentConfiguration getFragmentConfiguration() {
    return fragmentConfiguration;
  }

}
