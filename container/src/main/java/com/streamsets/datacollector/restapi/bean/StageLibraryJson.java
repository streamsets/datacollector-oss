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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class StageLibraryJson {
  private String id;
  private String label;
  private boolean installed;
  private List<List<StageInfoJson>> stageDefList;

  @JsonCreator
  public StageLibraryJson(
      @JsonProperty("id") String id,
      @JsonProperty("label") String label,
      @JsonProperty("installed") boolean installed,
      @JsonProperty("stageDefList") List<List<StageInfoJson>> stageDefList
  ) {
    this.id = id;
    this.label = label;
    this.installed = installed;
    this.stageDefList = stageDefList;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getLabel() {
    return label;
  }

  public void setLabel(String label) {
    this.label = label;
  }

  public boolean isInstalled() {
    return installed;
  }

  public void setInstalled(boolean installed) {
    this.installed = installed;
  }

  public List<List<StageInfoJson>> getStageDefList() {
    return stageDefList;
  }

  public void setStageDefList(List<List<StageInfoJson>> stageDefList) {
    this.stageDefList = stageDefList;
  }
}
