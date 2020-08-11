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
package com.streamsets.datacollector.config;

import com.streamsets.datacollector.validation.DetachedConnectionValidator;
import com.streamsets.datacollector.validation.Issues;

/**
 * Describes detached connection when exchanging data in REST interface.
 */
public class DetachedConnectionConfiguration {
  public static final int CURRENT_VERSION = 1;

  // Current version of the schema
  int schemaVersion;

  // Stage configuration itself
  ConnectionConfiguration connectionConfiguration;

  // latest available version for upgrade
  int latestAvailableVersion;

  // Issues associated with this
  Issues issues;

  public DetachedConnectionConfiguration() {
    this(CURRENT_VERSION, null);
  }

  public DetachedConnectionConfiguration(ConnectionConfiguration connectionConfiguration) {
    this(CURRENT_VERSION, connectionConfiguration);
  }

  public DetachedConnectionConfiguration(int schemaVersion, ConnectionConfiguration connectionConfiguration) {
    this.schemaVersion = schemaVersion;
    this.connectionConfiguration = connectionConfiguration;
  }

  public int getSchemaVersion() {
    return schemaVersion;
  }

  public ConnectionConfiguration getConnectionConfiguration() {
    return connectionConfiguration;
  }

  public Issues getIssues() {
    return issues;
  }

  public void setValidation(DetachedConnectionValidator validation) {
    issues = validation.getIssues();
  }

  public int getLatestAvailableVersion() {
    return latestAvailableVersion;
  }

  public void setLatestAvailableVersion(int latestAvailableVersion) {
    this.latestAvailableVersion = latestAvailableVersion;
  }
}
