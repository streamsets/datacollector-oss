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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.datacollector.config.DetachedStageConfiguration;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DetachedStageConfigurationJson {
  private final com.streamsets.datacollector.config.DetachedStageConfiguration detachedStageConfiguration;

  @JsonCreator
  public DetachedStageConfigurationJson(
    @JsonProperty("schemaVersion") int schemaVersion,
    @JsonProperty("stageConfiguration") StageConfigurationJson stageConfigurationJson
  ) {
    this.detachedStageConfiguration = new DetachedStageConfiguration(
      schemaVersion,
      BeanHelper.unwrapStageConfiguration(stageConfigurationJson)
    );
  }

  public DetachedStageConfigurationJson(DetachedStageConfiguration detachedStageConfiguration) {
    this.detachedStageConfiguration = detachedStageConfiguration;
  }

  public int getSchemaVersion() {
    return detachedStageConfiguration.getSchemaVersion();
  }

  public StageConfigurationJson getStageConfiguration() {
    return BeanHelper.wrapStageConfiguration(detachedStageConfiguration.getStageConfiguration());
  }

  public IssuesJson getIssues() {
    return BeanHelper.wrapIssues(detachedStageConfiguration.getIssues());
  }

  @JsonIgnore
  public DetachedStageConfiguration getDetachedStageConfiguration() {
    return detachedStageConfiguration;
  }
}
