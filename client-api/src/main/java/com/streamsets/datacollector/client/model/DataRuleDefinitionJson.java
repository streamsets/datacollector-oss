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

import com.streamsets.datacollector.client.StringUtil;



import io.swagger.annotations.*;
import com.fasterxml.jackson.annotation.JsonProperty;


@ApiModel(description = "")
public class DataRuleDefinitionJson   {

  private String id = null;
  private String label = null;
  private String lane = null;
  private Double samplingPercentage = null;
  private Integer samplingRecordsToRetain = null;
  private String condition = null;
  private Boolean alertEnabled = null;
  private String alertText = null;

public enum ThresholdTypeEnum {
  COUNT("COUNT"), PERCENTAGE("PERCENTAGE");

  private String value;

  ThresholdTypeEnum(String value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return value;
  }
}

  private ThresholdTypeEnum thresholdType = null;
  private String thresholdValue = null;
  private Long minVolume = null;
  private Boolean meterEnabled = null;
  private Boolean sendEmail = null;
  private Boolean enabled = null;
  private Boolean valid = null;
  private long timestamp;


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("id")
  public String getId() {
    return id;
  }
  public void setId(String id) {
    this.id = id;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("label")
  public String getLabel() {
    return label;
  }
  public void setLabel(String label) {
    this.label = label;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("lane")
  public String getLane() {
    return lane;
  }
  public void setLane(String lane) {
    this.lane = lane;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("samplingPercentage")
  public Double getSamplingPercentage() {
    return samplingPercentage;
  }
  public void setSamplingPercentage(Double samplingPercentage) {
    this.samplingPercentage = samplingPercentage;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("samplingRecordsToRetain")
  public Integer getSamplingRecordsToRetain() {
    return samplingRecordsToRetain;
  }
  public void setSamplingRecordsToRetain(Integer samplingRecordsToRetain) {
    this.samplingRecordsToRetain = samplingRecordsToRetain;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("condition")
  public String getCondition() {
    return condition;
  }
  public void setCondition(String condition) {
    this.condition = condition;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("alertEnabled")
  public Boolean getAlertEnabled() {
    return alertEnabled;
  }
  public void setAlertEnabled(Boolean alertEnabled) {
    this.alertEnabled = alertEnabled;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("alertText")
  public String getAlertText() {
    return alertText;
  }
  public void setAlertText(String alertText) {
    this.alertText = alertText;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("thresholdType")
  public ThresholdTypeEnum getThresholdType() {
    return thresholdType;
  }
  public void setThresholdType(ThresholdTypeEnum thresholdType) {
    this.thresholdType = thresholdType;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("thresholdValue")
  public String getThresholdValue() {
    return thresholdValue;
  }
  public void setThresholdValue(String thresholdValue) {
    this.thresholdValue = thresholdValue;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("minVolume")
  public Long getMinVolume() {
    return minVolume;
  }
  public void setMinVolume(Long minVolume) {
    this.minVolume = minVolume;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("meterEnabled")
  public Boolean getMeterEnabled() {
    return meterEnabled;
  }
  public void setMeterEnabled(Boolean meterEnabled) {
    this.meterEnabled = meterEnabled;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("sendEmail")
  public Boolean getSendEmail() {
    return sendEmail;
  }
  public void setSendEmail(Boolean sendEmail) {
    this.sendEmail = sendEmail;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("enabled")
  public Boolean getEnabled() {
    return enabled;
  }
  public void setEnabled(Boolean enabled) {
    this.enabled = enabled;
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
  @JsonProperty("timestamp")
  public long getTimestamp() {
    return timestamp;
  }
  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public String toString()  {
    StringBuilder sb = new StringBuilder();
    sb.append("class DataRuleDefinitionJson {\n");

    sb.append("    id: ").append(StringUtil.toIndentedString(id)).append("\n");
    sb.append("    label: ").append(StringUtil.toIndentedString(label)).append("\n");
    sb.append("    lane: ").append(StringUtil.toIndentedString(lane)).append("\n");
    sb.append("    samplingPercentage: ").append(StringUtil.toIndentedString(samplingPercentage)).append("\n");
    sb.append("    samplingRecordsToRetain: ").append(StringUtil.toIndentedString(samplingRecordsToRetain)).append("\n");
    sb.append("    condition: ").append(StringUtil.toIndentedString(condition)).append("\n");
    sb.append("    alertEnabled: ").append(StringUtil.toIndentedString(alertEnabled)).append("\n");
    sb.append("    alertText: ").append(StringUtil.toIndentedString(alertText)).append("\n");
    sb.append("    thresholdType: ").append(StringUtil.toIndentedString(thresholdType)).append("\n");
    sb.append("    thresholdValue: ").append(StringUtil.toIndentedString(thresholdValue)).append("\n");
    sb.append("    minVolume: ").append(StringUtil.toIndentedString(minVolume)).append("\n");
    sb.append("    meterEnabled: ").append(StringUtil.toIndentedString(meterEnabled)).append("\n");
    sb.append("    sendEmail: ").append(StringUtil.toIndentedString(sendEmail)).append("\n");
    sb.append("    enabled: ").append(StringUtil.toIndentedString(enabled)).append("\n");
    sb.append("    valid: ").append(StringUtil.toIndentedString(valid)).append("\n");
    sb.append("    timestamp: ").append(StringUtil.toIndentedString(timestamp)).append("\n");
    sb.append("}");
    return sb.toString();
  }
}
