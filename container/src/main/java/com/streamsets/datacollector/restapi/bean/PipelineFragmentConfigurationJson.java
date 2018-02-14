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

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PipelineFragmentConfigurationJson implements Serializable {

  private final PipelineFragmentConfiguration fragmentConfiguration;

  @SuppressWarnings("unchecked")
  public PipelineFragmentConfigurationJson(
      @JsonProperty("schemaVersion") int schemaVersion,
      @JsonProperty("version") int version,
      @JsonProperty("pipelineId") String pipelineId,
      @JsonProperty("title") String title,
      @JsonProperty("description") String description,
      @JsonProperty("uuid") UUID uuid,
      @JsonProperty("configuration") List<ConfigConfigurationJson> configuration,
      @JsonProperty("uiInfo") Map<String, Object> uiInfo,
      @JsonProperty("fragments") List<PipelineFragmentConfigurationJson> fragments,
      @JsonProperty("stages") List<StageConfigurationJson> stages
  ) {
    fragmentConfiguration = new PipelineFragmentConfiguration(
        uuid,
        version,
        schemaVersion,
        title,
        pipelineId,
        description,
        BeanHelper.unwrapPipelineFragementConfigurations(fragments),
        BeanHelper.unwrapStageConfigurations(stages),
        uiInfo,
        BeanHelper.unwrapConfigConfiguration(configuration)
    );
  }

  public PipelineFragmentConfigurationJson(PipelineFragmentConfiguration pipelineFragmentConfiguration) {
    this.fragmentConfiguration = pipelineFragmentConfiguration;
  }

  public int getSchemaVersion() {
    return fragmentConfiguration.getSchemaVersion();
  }

  public String getPipelineId() {
    return fragmentConfiguration.getPipelineId();
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

  public List<StageConfigurationJson> getStages() {
    return BeanHelper.wrapStageConfigurations(fragmentConfiguration.getStages());
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

  @JsonIgnore
  public PipelineFragmentConfiguration getFragmentConfiguration() {
    return fragmentConfiguration;
  }

}
