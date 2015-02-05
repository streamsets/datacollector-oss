/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.List;

public class MetricsAlertDefinition {

  private final String id;
  private final String label;
  private final String metricId;
  private final MetricType metricType;
  private final MetricElement metricElement;
  private final String condition;
  private final boolean enabled;
  private boolean valid = true;

  /*enable alert by email*/
  private final boolean sendEmail;
  private final List<String> emailIds;

  @JsonCreator
  public MetricsAlertDefinition(@JsonProperty("id") String id,
                                @JsonProperty("label") String label,
                                @JsonProperty("metricId") String metricId,
                                @JsonProperty("metricType") MetricType metricType,
                                @JsonProperty("metricElement") MetricElement metricElement,
                                @JsonProperty("condition") String condition,
                                @JsonProperty("sendMail") boolean sendEmail,
                                @JsonProperty("emailIds") List<String> emailIds,
                                @JsonProperty("enabled") boolean enabled) {
    this.id = id;
    this.label = label;
    this.metricId = metricId;
    this.metricType = metricType;
    this.metricElement = metricElement;
    this.condition = condition;
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

  public List<String> getEmailIds() {
    return emailIds;
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
      getId(), getLabel(), getCondition(), isEnabled(), getMetricType().name(), getMetricElement().name());
  }
}
