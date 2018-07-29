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
package com.streamsets.datacollector.client.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DetachedStageConfigurationJson {
  private Integer schemaVersion = null;
  private StageConfigurationJson stageConfiguration = null;

  public Integer getSchemaVersion() {
    return schemaVersion;
  }

  public void setSchemaVersion(Integer schemaVersion) {
    this.schemaVersion = schemaVersion;
  }

  public StageConfigurationJson getStageConfiguration() {
    return stageConfiguration;
  }

  public void setStageConfiguration(StageConfigurationJson stageConfiguration) {
    this.stageConfiguration = stageConfiguration;
  }

  public IssuesJson getIssues() {
    return issues;
  }

  public void setIssues(IssuesJson issues) {
    this.issues = issues;
  }

  private IssuesJson issues = null;
}
