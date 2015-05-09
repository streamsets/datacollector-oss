/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */

package com.streamsets.pipeline.restapi.bean;

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
