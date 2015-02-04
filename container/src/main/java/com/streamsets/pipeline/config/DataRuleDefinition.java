/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class DataRuleDefinition {

  private final String id;
  private final String label;
  private final String lane;
  private final double samplingPercentage;
  private final int samplingRecordsToRetain;
  private final String condition;

  /*alert related options*/
  private final boolean alertEnabled;
  private final ThresholdType thresholdType;
  private final String thresholdValue;
  private final long minVolume;

  /*create a meter to indicate rate of records matching the condition over time.*/
  private final boolean meterEnabled;

  /*enable alert by email*/
  private final boolean sendEmail;
  private final List<String> emailIds;

  /*is this rule definition enabled*/
  private final boolean enabled;

  @JsonCreator
  public DataRuleDefinition(@JsonProperty("id") String id,
                            @JsonProperty("label") String label,
                            @JsonProperty("lane") String lane,
                            @JsonProperty("samplingPercentage") double samplingPercentage,
                            @JsonProperty("samplingRecordsToRetain") int samplingRecordsToRetain,
                            @JsonProperty("condition") String condition,
                            @JsonProperty("alertEnabled") boolean alertEnabled,
                            @JsonProperty("thresholdType") ThresholdType thresholdType,
                            @JsonProperty("thresholdValue") String thresholdValue,
                            @JsonProperty("minVolume") long minVolume,
                            @JsonProperty("meterEnabled") boolean meterEnabled,
                            @JsonProperty("sendEmail") boolean sendEmail,
                            @JsonProperty("emailIds") List<String> emailIds,
                            @JsonProperty("enabled") boolean enabled) {
    this.id = id;
    this.label = label;
    this.lane = lane;
    this.samplingPercentage = samplingPercentage;
    this.samplingRecordsToRetain = samplingRecordsToRetain;
    this.condition = condition;
    this.alertEnabled = alertEnabled;
    this.thresholdType = thresholdType;
    this.thresholdValue = thresholdValue;
    this.minVolume = minVolume;
    this.meterEnabled = meterEnabled;
    this.enabled = enabled;
    this.sendEmail = sendEmail;
    this.emailIds = emailIds;
  }

  public String getId() {
    return id;
  }

  public String getLabel() {
    return label;
  }

  public String getLane() {
    return lane;
  }

  public double getSamplingPercentage() {
    return samplingPercentage;
  }

  public int getSamplingRecordsToRetain() {
    return samplingRecordsToRetain;
  }

  public String getCondition() {
    return condition;
  }

  public boolean isAlertEnabled() {
    return alertEnabled;
  }

  public ThresholdType getThresholdType() {
    return thresholdType;
  }

  public String getThresholdValue() {
    return thresholdValue;
  }

  public long getMinVolume() {
    return minVolume;
  }

  public boolean isMeterEnabled() {
    return meterEnabled;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public boolean isSendEmail() {
    return sendEmail;
  }

  public List<String> getEmailIds() {
    return emailIds;
  }
}
