/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License; Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing; software
 * distributed under the License is distributed on an "AS IS" BASIS;
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND; either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.client.model;

public class DriftRuleDefinitionJson {
  private String id;
  private String label;
  private String lane;
  private double samplingPercentage;
  private int samplingRecordsToRetain;
  private String condition;
  private boolean alertEnabled;
  private String alertText;
  private boolean meterEnabled;
  private boolean sendEmail;
  private boolean enabled;
  private long timestamp;
  private Boolean valid = null;

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

  public String getLane() {
    return lane;
  }

  public void setLane(String lane) {
    this.lane = lane;
  }

  public double getSamplingPercentage() {
    return samplingPercentage;
  }

  public void setSamplingPercentage(double samplingPercentage) {
    this.samplingPercentage = samplingPercentage;
  }

  public int getSamplingRecordsToRetain() {
    return samplingRecordsToRetain;
  }

  public void setSamplingRecordsToRetain(int samplingRecordsToRetain) {
    this.samplingRecordsToRetain = samplingRecordsToRetain;
  }

  public String getCondition() {
    return condition;
  }

  public void setCondition(String condition) {
    this.condition = condition;
  }

  public boolean isAlertEnabled() {
    return alertEnabled;
  }

  public void setAlertEnabled(boolean alertEnabled) {
    this.alertEnabled = alertEnabled;
  }

  public String getAlertText() {
    return alertText;
  }

  public void setAlertText(String alertText) {
    this.alertText = alertText;
  }

  public boolean isMeterEnabled() {
    return meterEnabled;
  }

  public void setMeterEnabled(boolean meterEnabled) {
    this.meterEnabled = meterEnabled;
  }

  public boolean isSendEmail() {
    return sendEmail;
  }

  public void setSendEmail(boolean sendEmail) {
    this.sendEmail = sendEmail;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public Boolean getValid() {
    return valid;
  }

  public void setValid(Boolean valid) {
    this.valid = valid;
  }
}
