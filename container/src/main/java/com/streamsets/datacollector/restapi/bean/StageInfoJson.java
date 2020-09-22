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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.streamsets.datacollector.config.StageDefinition;

import java.util.ArrayList;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class StageInfoJson {

  private String name;
  private String type;
  private String label;
  private String description;
  private int version;
  private boolean errorStage;
  private boolean statsAggregatorStage;
  private boolean connectionVerifierStage;
  private boolean beta;
  private String icon;
  private String onlineHelpRefUrl;
  private List<String> tags = new ArrayList<>();

  public StageInfoJson() { }

  public StageInfoJson(StageDefinition stageDefinition) {
    name = stageDefinition.getName();
    type = stageDefinition.getType().name();
    label = stageDefinition.getLabel();
    description = stageDefinition.getDescription();
    version = stageDefinition.getVersion();
    errorStage = stageDefinition.isErrorStage();
    statsAggregatorStage = stageDefinition.isStatsAggregatorStage();
    connectionVerifierStage = stageDefinition.isConnectionVerifierStage();
    beta = stageDefinition.isBeta();
    icon = stageDefinition.getIcon();
    tags = stageDefinition.getTags();
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getLabel() {
    return label;
  }

  public void setLabel(String label) {
    this.label = label;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  public boolean isErrorStage() {
    return errorStage;
  }

  public void setErrorStage(boolean errorStage) {
    this.errorStage = errorStage;
  }

  public boolean isStatsAggregatorStage() {
    return statsAggregatorStage;
  }

  public void setStatsAggregatorStage(boolean statsAggregatorStage) {
    this.statsAggregatorStage = statsAggregatorStage;
  }

  public boolean isBeta() {
    return beta;
  }

  public void setBeta(boolean beta) {
    this.beta = beta;
  }

  public String getIcon() {
    return icon;
  }

  public void setIcon(String icon) {
    this.icon = icon;
  }

  public String getOnlineHelpRefUrl() {
    return onlineHelpRefUrl;
  }

  public void setOnlineHelpRefUrl(String onlineHelpRefUrl) {
    this.onlineHelpRefUrl = onlineHelpRefUrl;
  }

  public List<String> getTags() {
    return tags;
  }

  public void setTags(List<String> tags) {
    this.tags = tags;
  }

  public boolean isConnectionVerifierStage() {
    return connectionVerifierStage;
  }

  public void setConnectionVerifierStage(boolean connectionVerifierStage) {
    this.connectionVerifierStage = connectionVerifierStage;
  }
}

