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
package com.streamsets.datacollector.client.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.datacollector.client.StringUtil;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ApiModel(description = "")
public class PipelineConfigurationJson   {

  private Integer schemaVersion = null;
  private Integer version = null;
  private String pipelineId = null;
  private String uuid = null;
  private String title = null;
  private String description = null;
  private List<ConfigConfigurationJson> configuration = new ArrayList<ConfigConfigurationJson>();
  private Map<String, Object> uiInfo = new HashMap<String, Object>();
  private List<StageConfigurationJson> stages = new ArrayList<StageConfigurationJson>();
  private StageConfigurationJson errorStage = null;
  private PipelineInfoJson info = null;
  private IssuesJson issues = null;
  private Boolean valid = null;
  private Boolean previewable = null;
  private StageConfigurationJson statsAggregatorStage = null;
  private Map<String, Object> metadata = new HashMap<String, Object>();
  private List<StageConfigurationJson> startEventStages;
  private List<StageConfigurationJson> stopEventStages;

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("schemaVersion")
  public Integer getSchemaVersion() {
    return schemaVersion;
  }
  public void setSchemaVersion(Integer schemaVersion) {
    this.schemaVersion = schemaVersion;
  }

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("version")
  public Integer getVersion() {
    return version;
  }
  public void setVersion(Integer version) {
    this.version = version;
  }

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("pipelineId")
  public String getPipelineId() {
    return pipelineId;
  }
  public void setPipelineId(String pipelineId) {
    this.pipelineId = pipelineId;
  }

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("uuid")
  public String getUuid() {
    return uuid;
  }
  public void setUuid(String uuid) {
    this.uuid = uuid;
  }

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("title")
  public String getTitle() {
    return title;
  }
  public void setTitle(String title) {
    this.title = title;
  }

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("description")
  public String getDescription() {
    return description;
  }
  public void setDescription(String description) {
    this.description = description;
  }

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("configuration")
  public List<ConfigConfigurationJson> getConfiguration() {
    return configuration;
  }
  public void setConfiguration(List<ConfigConfigurationJson> configuration) {
    this.configuration = configuration;
  }

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("uiInfo")
  public Map<String, Object> getUiInfo() {
    return uiInfo;
  }
  public void setUiInfo(Map<String, Object> uiInfo) {
    this.uiInfo = uiInfo;
  }

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("stages")
  public List<StageConfigurationJson> getStages() {
    return stages;
  }
  public void setStages(List<StageConfigurationJson> stages) {
    this.stages = stages;
  }

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("errorStage")
  public StageConfigurationJson getErrorStage() {
    return errorStage;
  }
  public void setErrorStage(StageConfigurationJson errorStage) {
    this.errorStage = errorStage;
  }

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("info")
  public PipelineInfoJson getInfo() {
    return info;
  }
  public void setInfo(PipelineInfoJson info) {
    this.info = info;
  }

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("issues")
  public IssuesJson getIssues() {
    return issues;
  }
  public void setIssues(IssuesJson issues) {
    this.issues = issues;
  }

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("valid")
  public Boolean getValid() {
    return valid;
  }
  public void setValid(Boolean valid) {
    this.valid = valid;
  }

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("previewable")
  public Boolean getPreviewable() {
    return previewable;
  }
  public void setPreviewable(Boolean previewable) {
    this.previewable = previewable;
  }

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("statsAggregatorStage")
  public StageConfigurationJson getStatsAggregatorStage() {
    return statsAggregatorStage;
  }
  public void setStatsAggregatorStage(StageConfigurationJson statsAggregatorStage) {
    this.statsAggregatorStage = statsAggregatorStage;
  }

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("startEventStages")
  public List<StageConfigurationJson> getStartEventStages() {
    return startEventStages;
  }
  public void setStartEventStages(List<StageConfigurationJson> startEventStages) {
    this.startEventStages = startEventStages;
  }

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("stopEventStages")
  public List<StageConfigurationJson> getStopEventStages() {
    return stopEventStages;
  }
  public void setStopEventStages(List<StageConfigurationJson> stopEventStages) {
    this.stopEventStages = stopEventStages;
  }

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("metadata")
  public Map<String, Object> getMetadata() {
    return metadata;
  }
  public void setMetadata(Map<String, Object> metadata) {
    this.metadata = metadata;
  }

  @Override
  public String toString()  {
    StringBuilder sb = new StringBuilder();
    sb.append("class PipelineConfigurationJson {\n");

    sb.append("    schemaVersion: ").append(StringUtil.toIndentedString(schemaVersion)).append("\n");
    sb.append("    version: ").append(StringUtil.toIndentedString(version)).append("\n");
    sb.append("    uuid: ").append(StringUtil.toIndentedString(uuid)).append("\n");
    sb.append("    description: ").append(StringUtil.toIndentedString(description)).append("\n");
    sb.append("    configuration: ").append(StringUtil.toIndentedString(configuration)).append("\n");
    sb.append("    uiInfo: ").append(StringUtil.toIndentedString(uiInfo)).append("\n");
    sb.append("    stages: ").append(StringUtil.toIndentedString(stages)).append("\n");
    sb.append("    errorStage: ").append(StringUtil.toIndentedString(errorStage)).append("\n");
    sb.append("    info: ").append(StringUtil.toIndentedString(info)).append("\n");
    sb.append("    issues: ").append(StringUtil.toIndentedString(issues)).append("\n");
    sb.append("    valid: ").append(StringUtil.toIndentedString(valid)).append("\n");
    sb.append("    previewable: ").append(StringUtil.toIndentedString(previewable)).append("\n");
    sb.append("    statsAggregatorStage: ").append(StringUtil.toIndentedString(statsAggregatorStage)).append("\n");
    sb.append("    metadata: ").append(StringUtil.toIndentedString(metadata)).append("\n");
    sb.append("}");
    return sb.toString();
  }
}
