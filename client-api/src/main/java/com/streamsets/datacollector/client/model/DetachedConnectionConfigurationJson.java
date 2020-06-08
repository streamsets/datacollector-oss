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
package com.streamsets.datacollector.client.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DetachedConnectionConfigurationJson {
  private Integer schemaVersion = null;
  private ConnectionConfigurationJson connectionConfiguration = null;

  public Integer getSchemaVersion() {
    return schemaVersion;
  }

  public void setSchemaVersion(Integer schemaVersion) {
    this.schemaVersion = schemaVersion;
  }

  public ConnectionConfigurationJson getConnectionConfiguration() {
    return connectionConfiguration;
  }

  public void setConnectionConfiguration(ConnectionConfigurationJson connectionConfiguration) {
    this.connectionConfiguration = connectionConfiguration;
  }

  public IssuesJson getIssues() {
    return issues;
  }

  public void setIssues(IssuesJson issues) {
    this.issues = issues;
  }

  private IssuesJson issues = null;
}
