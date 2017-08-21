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
import com.streamsets.datacollector.config.DriftRuleDefinition;
import com.streamsets.pipeline.api.impl.Utils;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DriftRuleDefinitionJson {

  private final com.streamsets.datacollector.config.DriftRuleDefinition driftRuleDefinition;

  @JsonCreator
  public DriftRuleDefinitionJson(
      @JsonProperty("id") String id,
      @JsonProperty("label") String label,
      @JsonProperty("lane") String lane,
      @JsonProperty("samplingPercentage") double samplingPercentage,
      @JsonProperty("samplingRecordsToRetain") int samplingRecordsToRetain,
      @JsonProperty("condition") String condition,
      @JsonProperty("alertEnabled") boolean alertEnabled,
      @JsonProperty("alertText") String alertText,
      @JsonProperty("meterEnabled") boolean meterEnabled,
      @JsonProperty("sendEmail") boolean sendEmail,
      @JsonProperty("enabled") boolean enabled,
      @JsonProperty("timestamp") long timestamp
  ) {
    this.driftRuleDefinition = new DriftRuleDefinition(
        id,
        label,
        lane,
        samplingPercentage,
        samplingRecordsToRetain,
        condition,
        alertEnabled,
        alertText,
        meterEnabled,
        sendEmail,
        enabled,
        timestamp
    );
  }

  public DriftRuleDefinitionJson(DriftRuleDefinition driftRuleDefinition) {
    Utils.checkNotNull(driftRuleDefinition, "driftRuleDefinition");
    this.driftRuleDefinition = driftRuleDefinition;
  }

  public String getId() {
    return driftRuleDefinition.getId();
  }

  public String getAlertText() {
    return driftRuleDefinition.getAlertText();
  }

  public String getCondition() {
    return driftRuleDefinition.getCondition();
  }

  public boolean isSendEmail() {
    return driftRuleDefinition.isSendEmail();
  }

  public boolean isEnabled() {
    return driftRuleDefinition.isEnabled();
  }

  public boolean isValid() {
    return driftRuleDefinition.isValid();
  }

  public String getLabel() {
    return driftRuleDefinition.getLabel();
  }

  public String getLane() {
    return driftRuleDefinition.getLane();
  }

  public double getSamplingPercentage() {
    return driftRuleDefinition.getSamplingPercentage();
  }

  public int getSamplingRecordsToRetain() {
    return driftRuleDefinition.getSamplingRecordsToRetain();
  }

  public boolean isAlertEnabled() {
    return driftRuleDefinition.isAlertEnabled();
  }

  public boolean isMeterEnabled() {
    return driftRuleDefinition.isMeterEnabled();
  }

  public long getTimestamp() {
    return driftRuleDefinition.getTimestamp();
  }

  @JsonIgnore
  public DriftRuleDefinition getDriftRuleDefinition() {
    return driftRuleDefinition;
  }
}
