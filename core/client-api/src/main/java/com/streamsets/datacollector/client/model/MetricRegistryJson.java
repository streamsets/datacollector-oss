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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.datacollector.client.StringUtil;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@ApiModel(description = "")
public class MetricRegistryJson   {

  private String version = null;
  private Map<String, Object> gauges = new HashMap<String, Object>();
  private Map<String, CounterJson> counters = new HashMap<String, CounterJson>();
  private Map<String, HistogramJson> histograms = new HashMap<String, HistogramJson>();
  private Map<String, MeterJson> meters = new HashMap<String, MeterJson>();
  private Map<String, TimerJson> timers = new HashMap<String, TimerJson>();
  private List<String> slaves = new ArrayList<String>();


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("version")
  public String getVersion() {
    return version;
  }
  public void setVersion(String version) {
    this.version = version;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("gauges")
  public Map<String, Object> getGauges() {
    return gauges;
  }
  public void setGauges(Map<String, Object> gauges) {
    this.gauges = gauges;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("counters")
  public Map<String, CounterJson> getCounters() {
    return counters;
  }
  public void setCounters(Map<String, CounterJson> counters) {
    this.counters = counters;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("histograms")
  public Map<String, HistogramJson> getHistograms() {
    return histograms;
  }
  public void setHistograms(Map<String, HistogramJson> histograms) {
    this.histograms = histograms;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("meters")
  public Map<String, MeterJson> getMeters() {
    return meters;
  }
  public void setMeters(Map<String, MeterJson> meters) {
    this.meters = meters;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("timers")
  public Map<String, TimerJson> getTimers() {
    return timers;
  }
  public void setTimers(Map<String, TimerJson> timers) {
    this.timers = timers;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("slaves")
  public List<String> getSlaves() {
    return slaves;
  }
  public void setSlaves(List<String> slaves) {
    this.slaves = slaves;
  }



  @Override
  public String toString()  {
    StringBuilder sb = new StringBuilder();
    sb.append("class MetricRegistryJson {\n");

    sb.append("    version: ").append(StringUtil.toIndentedString(version)).append("\n");
    sb.append("    gauges: ").append(StringUtil.toIndentedString(gauges)).append("\n");
    sb.append("    counters: ").append(StringUtil.toIndentedString(counters)).append("\n");
    sb.append("    histograms: ").append(StringUtil.toIndentedString(histograms)).append("\n");
    sb.append("    meters: ").append(StringUtil.toIndentedString(meters)).append("\n");
    sb.append("    timers: ").append(StringUtil.toIndentedString(timers)).append("\n");
    sb.append("    slaves: ").append(StringUtil.toIndentedString(slaves)).append("\n");
    sb.append("}");
    return sb.toString();
  }
}
