/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi.bean;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.pipeline.api.impl.Utils;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MetricsRuleDefinitionJson {

 private final com.streamsets.pipeline.config.MetricsRuleDefinition metricsRuleDefinition;

  @JsonCreator
  public MetricsRuleDefinitionJson(@JsonProperty("id") String id,
                                   @JsonProperty("alertText") String alertText,
                                   @JsonProperty("metricId") String metricId,
                                   @JsonProperty("metricType") MetricTypeJson metricType,
                                   @JsonProperty("metricElement") MetricElementJson metricElementJson,
                                   @JsonProperty("condition") String condition,
                                   @JsonProperty("sendEmail") boolean sendEmail,
                                   @JsonProperty("enabled") boolean enabled) {
    this.metricsRuleDefinition = new com.streamsets.pipeline.config.MetricsRuleDefinition(id, alertText, metricId,
      BeanHelper.unwrapMetricType(metricType), BeanHelper.unwrapMetricElement(metricElementJson), condition, sendEmail,
      enabled);

  }

  public MetricsRuleDefinitionJson(com.streamsets.pipeline.config.MetricsRuleDefinition metricsRuleDefinition) {
    Utils.checkNotNull(metricsRuleDefinition, "metricsRuleDefinition");
    this.metricsRuleDefinition = metricsRuleDefinition;
  }

  public String getId() {
    return metricsRuleDefinition.getId();
  }

  public String getAlertText() {
    return metricsRuleDefinition.getAlertText();
  }

  public String getCondition() {
    return metricsRuleDefinition.getCondition();
  }

  public boolean isSendEmail() {
    return metricsRuleDefinition.isSendEmail();
  }

  public boolean isEnabled() {
    return metricsRuleDefinition.isEnabled();
  }

  public boolean isValid() {
    return metricsRuleDefinition.isValid();
  }

  public String getMetricId() {
    return metricsRuleDefinition.getMetricId();
  }

  public MetricElementJson getMetricElement() {
    return BeanHelper.wrapMetricElement(metricsRuleDefinition.getMetricElement());
  }

  public MetricTypeJson getMetricType() {
    return BeanHelper.wrapMetricType(metricsRuleDefinition.getMetricType());
  }

  @JsonIgnore
  public com.streamsets.pipeline.config.MetricsRuleDefinition getMetricsRuleDefinition() {
    return metricsRuleDefinition;
  }
}