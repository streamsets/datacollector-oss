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
public class MetricsRuleDefinitionJson   {

  private String id = null;
  private String alertText = null;
  private String metricId = null;

public enum MetricTypeEnum {
  GAUGE("GAUGE"),
  COUNTER("COUNTER"),
  HISTOGRAM("HISTOGRAM"),
  METER("METER"),
  TIMER("TIMER");

  private String value;

  MetricTypeEnum(String value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return value;
  }
}

  private MetricTypeEnum metricType = null;

public enum MetricElementEnum {
  // Related to Counters
  COUNTER_COUNT("COUNTER_COUNT"),

  // Related to Histogram
  HISTOGRAM_COUNT("HISTOGRAM_COUNT"),
  HISTOGRAM_MAX("HISTOGRAM_MAX"),
  HISTOGRAM_MIN("HISTOGRAM_MIN"),
  HISTOGRAM_MEAN("HISTOGRAM_MEAN"),
  HISTOGRAM_MEDIAN("HISTOGRAM_MEDIAN"),
  HISTOGRAM_P50("HISTOGRAM_P50"),
  HISTOGRAM_P75("HISTOGRAM_P75"),
  HISTOGRAM_P95("HISTOGRAM_P95"),
  HISTOGRAM_P98("HISTOGRAM_P98"),
  HISTOGRAM_P99("HISTOGRAM_P99"),
  HISTOGRAM_P999("HISTOGRAM_P999"),
  HISTOGRAM_STD_DEV("HISTOGRAM_STD_DEV"),

  // Meters
  METER_COUNT("METER_COUNT"),
  METER_M1_RATE("METER_M1_RATE"),
  METER_M5_RATE("METER_M5_RATE"),
  METER_M15_RATE("METER_M15_RATE"),
  METER_M30_RATE("METER_M30_RATE"),
  METER_H1_RATE("METER_H1_RATE"),
  METER_H6_RATE("METER_H6_RATE"),
  METER_H12_RATE("METER_H12_RATE"),
  METER_H24_RATE("METER_H24_RATE"),
  METER_MEAN_RATE("METER_MEAN_RATE"),

  // Timer
  TIMER_COUNT("TIMER_COUNT"),
  TIMER_MAX("TIMER_MAX"),
  TIMER_MIN("TIMER_MIN"),
  TIMER_MEAN("TIMER_MEAN"),
  TIMER_P50("TIMER_P50"),
  TIMER_P75("TIMER_P75"),
  TIMER_P95("TIMER_P95"),
  TIMER_P98("TIMER_P98"),
  TIMER_P99("TIMER_P99"),
  TIMER_P999("TIMER_P999"),
  TIMER_STD_DEV("TIMER_STD_DEV"),
  TIMER_M1_RATE("TIMER_M1_RATE"),
  TIMER_M5_RATE("TIMER_M5_RATE"),
  TIMER_M15_RATE("TIMER_M15_RATE"),
  TIMER_MEAN_RATE("TIMER_MEAN_RATE"),

  // Gauge - Related to Runtime Stats
  CURRENT_BATCH_AGE("CURRENT_BATCH_AGE"),
  TIME_IN_CURRENT_STAGE("TIME_IN_CURRENT_STAGE"),
  TIME_OF_LAST_RECEIVED_RECORD("TIME_OF_LAST_RECEIVED_RECORD"),
  LAST_BATCH_INPUT_RECORDS_COUNT("LAST_BATCH_INPUT_RECORDS_COUNT"),
  LAST_BATCH_OUTPUT_RECORDS_COUNT("LAST_BATCH_OUTPUT_RECORDS_COUNT"),
  LAST_BATCH_ERROR_RECORDS_COUNT("LAST_BATCH_ERROR_RECORDS_COUNT"),
  LAST_BATCH_ERROR_MESSAGES_COUNT("LAST_BATCH_ERROR_MESSAGES_COUNT"),
  ;

  private String value;

  MetricElementEnum(String value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return value;
  }
}

  private MetricElementEnum metricElement = null;
  private String condition = null;
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
  @JsonProperty("metricId")
  public String getMetricId() {
    return metricId;
  }
  public void setMetricId(String metricId) {
    this.metricId = metricId;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("metricType")
  public MetricTypeEnum getMetricType() {
    return metricType;
  }
  public void setMetricType(MetricTypeEnum metricType) {
    this.metricType = metricType;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("metricElement")
  public MetricElementEnum getMetricElement() {
    return metricElement;
  }
  public void setMetricElement(MetricElementEnum metricElement) {
    this.metricElement = metricElement;
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
    sb.append("class MetricsRuleDefinitionJson {\n");

    sb.append("    id: ").append(StringUtil.toIndentedString(id)).append("\n");
    sb.append("    alertText: ").append(StringUtil.toIndentedString(alertText)).append("\n");
    sb.append("    metricId: ").append(StringUtil.toIndentedString(metricId)).append("\n");
    sb.append("    metricType: ").append(StringUtil.toIndentedString(metricType)).append("\n");
    sb.append("    metricElement: ").append(StringUtil.toIndentedString(metricElement)).append("\n");
    sb.append("    condition: ").append(StringUtil.toIndentedString(condition)).append("\n");
    sb.append("    sendEmail: ").append(StringUtil.toIndentedString(sendEmail)).append("\n");
    sb.append("    enabled: ").append(StringUtil.toIndentedString(enabled)).append("\n");
    sb.append("    valid: ").append(StringUtil.toIndentedString(valid)).append("\n");
    sb.append("    timestamp: ").append(StringUtil.toIndentedString(timestamp)).append("\n");
    sb.append("}");
    return sb.toString();
  }
}
