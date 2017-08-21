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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.pipeline.api.impl.Utils;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DataRuleDefinitionJson {

  private final com.streamsets.datacollector.config.DataRuleDefinition dataRuleDefinition;

  @JsonCreator
  public DataRuleDefinitionJson(
      @JsonProperty("id") String id,
      @JsonProperty("label") String label,
      @JsonProperty("lane") String lane,
      @JsonProperty("samplingPercentage") double samplingPercentage,
      @JsonProperty("samplingRecordsToRetain") int samplingRecordsToRetain,
      @JsonProperty("condition") String condition,
      @JsonProperty("alertEnabled") boolean alertEnabled,
      @JsonProperty("alertText") String alertText,
      @JsonProperty("thresholdType") ThresholdTypeJson thresholdTypeJson,
      @JsonProperty("thresholdValue") String thresholdValue,
      @JsonProperty("minVolume") long minVolume,
      @JsonProperty("meterEnabled") boolean meterEnabled,
      @JsonProperty("sendEmail") boolean sendEmail,
      @JsonProperty("enabled") boolean enabled,
      @JsonProperty("timestamp") long timestamp
  ) {
    this.dataRuleDefinition = new com.streamsets.datacollector.config.DataRuleDefinition(
        id,
        label,
        lane,
        samplingPercentage,
        samplingRecordsToRetain,
        condition,
        alertEnabled,
        alertText,
        BeanHelper.unwrapThresholdType(thresholdTypeJson),
        thresholdValue,
        minVolume,
        meterEnabled,
        sendEmail,
        enabled,
        timestamp);
  }

  public DataRuleDefinitionJson(com.streamsets.datacollector.config.DataRuleDefinition dataRuleDefinition) {
    Utils.checkNotNull(dataRuleDefinition, "dataRuleDefinition");
    this.dataRuleDefinition = dataRuleDefinition;
  }

  public String getId() {
    return dataRuleDefinition.getId();
  }

  public String getAlertText() {
    return dataRuleDefinition.getAlertText();
  }

  public String getCondition() {
    return dataRuleDefinition.getCondition();
  }

  public boolean isSendEmail() {
    return dataRuleDefinition.isSendEmail();
  }

  public boolean isEnabled() {
    return dataRuleDefinition.isEnabled();
  }

  public boolean isValid() {
    return dataRuleDefinition.isValid();
  }

  public String getLabel() {
    return dataRuleDefinition.getLabel();
  }

  public String getLane() {
    return dataRuleDefinition.getLane();
  }

  public double getSamplingPercentage() {
    return dataRuleDefinition.getSamplingPercentage();
  }

  public int getSamplingRecordsToRetain() {
    return dataRuleDefinition.getSamplingRecordsToRetain();
  }

  public boolean isAlertEnabled() {
    return dataRuleDefinition.isAlertEnabled();
  }

  public ThresholdTypeJson getThresholdType() {
    return BeanHelper.wrapThresholdType(dataRuleDefinition.getThresholdType());
  }

  public String getThresholdValue() {
    return dataRuleDefinition.getThresholdValue();
  }

  public long getMinVolume() {
    return dataRuleDefinition.getMinVolume();
  }

  public boolean isMeterEnabled() {
    return dataRuleDefinition.isMeterEnabled();
  }

  public long getTimestamp() {
    return dataRuleDefinition.getTimestamp();
  }

  @JsonIgnore
  public com.streamsets.datacollector.config.DataRuleDefinition getDataRuleDefinition() {
    return dataRuleDefinition;
  }
}
