/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.pipeline.api.impl.Utils;

public class MetricsAlertDefinition {

  private final String id;
  private final String alertText;
  private final String metricId;
  private final MetricType metricType;
  private final MetricElement metricElement;
  private final String condition;
  private final boolean enabled;
  private boolean valid = true;

  /*enable alert by email*/
  private final boolean sendEmail;

  @JsonCreator
  public MetricsAlertDefinition(@JsonProperty("id") String id,
                                @JsonProperty("alertText") String alertText,
                                @JsonProperty("metricId") String metricId,
                                @JsonProperty("metricType") MetricType metricType,
                                @JsonProperty("metricElement") MetricElement metricElement,
                                @JsonProperty("condition") String condition,
                                @JsonProperty("sendMail") boolean sendEmail,
                                @JsonProperty("enabled") boolean enabled) {
    this.id = id;
    this.alertText = alertText;
    this.metricId = metricId;
    this.metricType = metricType;
    this.metricElement = metricElement;
    this.condition = condition;
    this.enabled = enabled;
    this.sendEmail = sendEmail;
  }

  public String getId() {
    return id;
  }

  public String getAlertText() {
    return alertText;
  }

  public String getMetricId() {
    return metricId;
  }

  public MetricElement getMetricElement() {
    return metricElement;
  }

  public String getCondition() {
    return condition;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public MetricType getMetricType() {
    return metricType;
  }

  public boolean isSendEmail() {
    return sendEmail;
  }

  public boolean isValid() {
    return valid;
  }

  public void setValid(boolean valid) {
    this.valid = valid;
  }

  @Override
  public String toString() {
    return Utils.format(
      "MetricsAlertDefinition[id='{}' label='{}' condition='{}' enabled='{}', metricType='{}', metricElement='{}']",
      getId(), getAlertText(), getCondition(), isEnabled(), getMetricType().name(), getMetricElement().name());
  }
}
