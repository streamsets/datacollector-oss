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

import com.streamsets.datacollector.validation.DetachedStageValidator;
import com.streamsets.datacollector.validation.Issues;

/**
 * Describes detached stage when exchanging data in REST interface.
 */
public class DetachedStageConfiguration {
  public static final int CURRENT_VERSION = 1;

  // Current version of the schema
  int schemaVersion;

  // Stage configuration itself
  StageConfiguration stageConfiguration;

  // Issues associated with this
  Issues issues;

  public DetachedStageConfiguration() {
    this(CURRENT_VERSION, null);
  }

  public DetachedStageConfiguration(StageConfiguration stageConfiguration) {
    this(CURRENT_VERSION, stageConfiguration);
  }

  public DetachedStageConfiguration(int schemaVersion, StageConfiguration stageConfiguration) {
    this.schemaVersion = schemaVersion;
    this.stageConfiguration = stageConfiguration;
  }

  public int getSchemaVersion() {
    return schemaVersion;
  }

  public StageConfiguration getStageConfiguration() {
    return stageConfiguration;
  }

  public Issues getIssues() {
    return issues;
  }

  public void setValidation(DetachedStageValidator validation) {
    issues = validation.getIssues();
  }
}
