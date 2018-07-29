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
public class HeaderJson   {

  private String stageCreator = null;
  private String sourceId = null;
  private String stagesPath = null;
  private String trackingId = null;
  private String previousTrackingId = null;
  private List<String> raw = new ArrayList<String>();
  private String rawMimeType = null;
  private String errorDataCollectorId = null;
  private String errorPipelineName = null;
  private String errorStage = null;
  private String errorCode = null;
  private String errorMessage = null;
  private Long errorTimestamp = null;
  private Map<String, String> values = new HashMap<String, String>();


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("stageCreator")
  public String getStageCreator() {
    return stageCreator;
  }
  public void setStageCreator(String stageCreator) {
    this.stageCreator = stageCreator;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("sourceId")
  public String getSourceId() {
    return sourceId;
  }
  public void setSourceId(String sourceId) {
    this.sourceId = sourceId;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("stagesPath")
  public String getStagesPath() {
    return stagesPath;
  }
  public void setStagesPath(String stagesPath) {
    this.stagesPath = stagesPath;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("trackingId")
  public String getTrackingId() {
    return trackingId;
  }
  public void setTrackingId(String trackingId) {
    this.trackingId = trackingId;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("previousTrackingId")
  public String getPreviousTrackingId() {
    return previousTrackingId;
  }
  public void setPreviousTrackingId(String previousTrackingId) {
    this.previousTrackingId = previousTrackingId;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("raw")
  public List<String> getRaw() {
    return raw;
  }
  public void setRaw(List<String> raw) {
    this.raw = raw;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("rawMimeType")
  public String getRawMimeType() {
    return rawMimeType;
  }
  public void setRawMimeType(String rawMimeType) {
    this.rawMimeType = rawMimeType;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("errorDataCollectorId")
  public String getErrorDataCollectorId() {
    return errorDataCollectorId;
  }
  public void setErrorDataCollectorId(String errorDataCollectorId) {
    this.errorDataCollectorId = errorDataCollectorId;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("errorPipelineName")
  public String getErrorPipelineName() {
    return errorPipelineName;
  }
  public void setErrorPipelineName(String errorPipelineName) {
    this.errorPipelineName = errorPipelineName;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("errorStage")
  public String getErrorStage() {
    return errorStage;
  }
  public void setErrorStage(String errorStage) {
    this.errorStage = errorStage;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("errorCode")
  public String getErrorCode() {
    return errorCode;
  }
  public void setErrorCode(String errorCode) {
    this.errorCode = errorCode;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("errorMessage")
  public String getErrorMessage() {
    return errorMessage;
  }
  public void setErrorMessage(String errorMessage) {
    this.errorMessage = errorMessage;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("errorTimestamp")
  public Long getErrorTimestamp() {
    return errorTimestamp;
  }
  public void setErrorTimestamp(Long errorTimestamp) {
    this.errorTimestamp = errorTimestamp;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("values")
  public Map<String, String> getValues() {
    return values;
  }
  public void setValues(Map<String, String> values) {
    this.values = values;
  }



  @Override
  public String toString()  {
    StringBuilder sb = new StringBuilder();
    sb.append("class HeaderJson {\n");

    sb.append("    stageCreator: ").append(StringUtil.toIndentedString(stageCreator)).append("\n");
    sb.append("    sourceId: ").append(StringUtil.toIndentedString(sourceId)).append("\n");
    sb.append("    stagesPath: ").append(StringUtil.toIndentedString(stagesPath)).append("\n");
    sb.append("    trackingId: ").append(StringUtil.toIndentedString(trackingId)).append("\n");
    sb.append("    previousTrackingId: ").append(StringUtil.toIndentedString(previousTrackingId)).append("\n");
    sb.append("    raw: ").append(StringUtil.toIndentedString(raw)).append("\n");
    sb.append("    rawMimeType: ").append(StringUtil.toIndentedString(rawMimeType)).append("\n");
    sb.append("    errorDataCollectorId: ").append(StringUtil.toIndentedString(errorDataCollectorId)).append("\n");
    sb.append("    errorPipelineName: ").append(StringUtil.toIndentedString(errorPipelineName)).append("\n");
    sb.append("    errorStage: ").append(StringUtil.toIndentedString(errorStage)).append("\n");
    sb.append("    errorCode: ").append(StringUtil.toIndentedString(errorCode)).append("\n");
    sb.append("    errorMessage: ").append(StringUtil.toIndentedString(errorMessage)).append("\n");
    sb.append("    errorTimestamp: ").append(StringUtil.toIndentedString(errorTimestamp)).append("\n");
    sb.append("    values: ").append(StringUtil.toIndentedString(values)).append("\n");
    sb.append("}");
    return sb.toString();
  }
}
