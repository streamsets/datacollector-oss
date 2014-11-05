/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.streamsets.pipeline.validation.Issues;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class PipelineConfiguration {

  public enum OnError { DROP_RECORD, DROP_BATCH, STOP_PIPELINE }

  private UUID uuid = null;
  private List<ConfigConfiguration> configuration;
  private final Map<String, Object> uiInfo;
  private List<StageConfiguration> stages;
  private Issues issues;

  @SuppressWarnings("unchecked")
  public PipelineConfiguration(@JsonProperty("uuid") UUID uuid,
      @JsonProperty("configuration") List<ConfigConfiguration> configuration,
      @JsonProperty("uiInfo") Map<String, Object> uiInfo,
      @JsonProperty("stages") List<StageConfiguration> stages) {
    this.uuid = Preconditions.checkNotNull(uuid, "uuid cannot be null");
    this.configuration = configuration;
    this.uiInfo = uiInfo;
    this.stages = (stages != null) ? stages : Collections.EMPTY_LIST;
    issues = new Issues();
  }

  public List<StageConfiguration> getStages() {
    return stages;
  }

  public void setUuid(UUID uuid) {
    this.uuid = uuid;
  }

  public UUID getUuid() {
    return uuid;
  }

  public void setIssues(Issues issues) {
    this.issues = Preconditions.checkNotNull(issues, "issues cannot be null");
  }

  public Issues getIssues() {
    return issues;
  }

  public void setValid(boolean dummy) {
    //NOP, just for jackson
  }

  public boolean isValid() {
    return !issues.hasIssues();
  }

  public List<ConfigConfiguration> getConfiguration() {
    return configuration;
  }

  public Map<String, Object> getUiInfo() {
    return uiInfo;
  }
}
