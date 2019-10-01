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
package com.streamsets.datacollector.event.json;

import java.util.List;
import java.util.Map;

public class MetricRegistryJson {
  String version;
  Map<String, Object> gauges;
  Map<String, CounterJson> counters;
  Map<String, HistogramJson> histograms;
  Map<String, MeterJson> meters;
  Map<String, TimerJson> timers;
  List<String> slaves;

  public MetricRegistryJson() {
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public Map<String, Object> getGauges() {
    return gauges;
  }

  public void setGauges(Map<String, Object> gauges) {
    this.gauges = gauges;
  }

  public List<String> getSlaves() {
    return slaves;
  }

  public void setSlaves(List<String> slaves) {
    this.slaves = slaves;
  }

  public Map<String, CounterJson> getCounters() {
    return counters;
  }

  public void setCounters(Map<String, CounterJson> counters) {
    this.counters = counters;
  }

  public Map<String, HistogramJson> getHistograms() {
    return histograms;
  }

  public void setHistograms(Map<String, HistogramJson> histograms) {
    this.histograms = histograms;
  }

  public Map<String, MeterJson> getMeters() {
    return meters;
  }

  public void setMeters(Map<String, MeterJson> meters) {
    this.meters = meters;
  }

  public Map<String, TimerJson> getTimers() {
    return timers;
  }

  public void setTimers(Map<String, TimerJson> timers) {
    this.timers = timers;
  }
}
