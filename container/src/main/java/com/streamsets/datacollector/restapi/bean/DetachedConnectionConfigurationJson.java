/*
 * Copyright 2020 StreamSets Inc.
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
import com.streamsets.datacollector.config.DetachedConnectionConfiguration;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DetachedConnectionConfigurationJson {
  private final DetachedConnectionConfiguration detachedConnectionConfigurationJson;

  @JsonCreator
  public DetachedConnectionConfigurationJson(
    @JsonProperty("schemaVersion") int schemaVersion,
    @JsonProperty("connectionConfiguration") ConnectionConfigurationJson connectionConfigurationJson
  ) {
    this.detachedConnectionConfigurationJson = new DetachedConnectionConfiguration(
      schemaVersion,
      BeanHelper.unwrapConnectionConfiguration(connectionConfigurationJson)
    );
  }

  public DetachedConnectionConfigurationJson(DetachedConnectionConfiguration detachedConnectionConfigurationJson) {
    this.detachedConnectionConfigurationJson = detachedConnectionConfigurationJson;
  }

  public int getSchemaVersion() {
    return detachedConnectionConfigurationJson.getSchemaVersion();
  }

  public ConnectionConfigurationJson getConnectionConfiguration() {
    return BeanHelper.wrapConnectionConfiguration(detachedConnectionConfigurationJson.getConnectionConfiguration());
  }

  public IssuesJson getIssues() {
    return BeanHelper.wrapIssues(detachedConnectionConfigurationJson.getIssues());
  }

  public int getLatestAvailableVersion() {
    return detachedConnectionConfigurationJson.getLatestAvailableVersion();
  }

  @JsonIgnore
  public DetachedConnectionConfiguration getDetachedConnectionConfiguration() {
    return detachedConnectionConfigurationJson;
  }
}
