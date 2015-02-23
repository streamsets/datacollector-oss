/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

public class DataRuleDefinition extends RuleDefinition {

  private final String label;
  private final String lane;
  private final double samplingPercentage;
  private final int samplingRecordsToRetain;

  /*alert related options*/
  private final boolean alertEnabled;
  private final ThresholdType thresholdType;
  private final String thresholdValue;
  private final long minVolume;

  /*create a meter to indicate rate of records matching the condition over time.*/
  private final boolean meterEnabled;

  public DataRuleDefinition(String id, String label, String lane, double samplingPercentage,
                            int samplingRecordsToRetain, String condition, boolean alertEnabled, String alertText,
                            ThresholdType thresholdType, String thresholdValue, long minVolume, boolean meterEnabled,
                            boolean sendEmail, boolean enabled) {
    super(id, condition, alertText, sendEmail, enabled);
    this.label = label;
    this.lane = lane;
    this.samplingPercentage = samplingPercentage;
    this.samplingRecordsToRetain = samplingRecordsToRetain;
    this.alertEnabled = alertEnabled;
    this.thresholdType = thresholdType;
    this.thresholdValue = thresholdValue;
    this.minVolume = minVolume;
    this.meterEnabled = meterEnabled;
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

}
