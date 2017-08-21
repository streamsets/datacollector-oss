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
package com.streamsets.datacollector.execution.runner.common;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.streamsets.datacollector.metrics.MetricsConfigurator;

import javax.inject.Inject;
import javax.inject.Named;

import java.util.HashMap;
import java.util.Map;

public class ThreadHealthReporter {

  private static final String HEALTH_PREFIX = "health.";

  private Map<String, ThreadHealthReportGauge> threadToGaugeMap;
  private MetricRegistry metrics;
  private final String name;
  private final String rev;

  @Inject
  public ThreadHealthReporter(@Named("name") String name, @Named("rev") String rev, MetricRegistry metrics) {
    this.name = name;
    this.rev = rev;
    this.threadToGaugeMap = new HashMap<>();
    this.metrics = metrics;
  }

  /**
   * Updates gauge for the registered thread with the given details.
   * Note that the value of the threadName argument must match the one used to register.
   * @param threadName
   * @param scheduledDelay
   * @param timestamp
   * @return
   */
  public boolean reportHealth(String threadName, int scheduledDelay, long timestamp) {
    ThreadHealthReport threadHealthReport = new ThreadHealthReport(threadName, scheduledDelay, timestamp);
    if(threadToGaugeMap.containsKey(threadName)) {
      threadToGaugeMap.get(threadName).setThreadHealthReport(threadHealthReport);
      return true;
    }
    return false;
  }

  /**
   * Creates and registers a Gauge with the given thread name.
   * The same name must be used to report health.
   * @param threadName
   * @return
   */
  public boolean register(String threadName) {
    if(threadToGaugeMap.containsKey(threadName)) {
      return false;
    }
    ThreadHealthReportGauge threadHealthReportGauge = new ThreadHealthReportGauge();
    MetricsConfigurator.createGauge(metrics, getHealthGaugeName(threadName), threadHealthReportGauge, name, rev);
    threadToGaugeMap.put(threadName, threadHealthReportGauge);
    return true;
  }

  public boolean unregister(String threadName) {
    if(threadToGaugeMap.containsKey(threadName)) {
      threadToGaugeMap.remove(threadName);
      return MetricsConfigurator.removeGauge(metrics, getHealthGaugeName(threadName), name, rev);
    }
    return true;
  }

  public void setMetrics(MetricRegistry metrics) {
    this.metrics = metrics;
  }

  public void destroy() {
    for(String threadName : threadToGaugeMap.keySet()) {
      MetricsConfigurator.removeGauge(metrics, getHealthGaugeName(threadName), name ,rev);
    }
    threadToGaugeMap.clear();
  }

  @VisibleForTesting
  static class ThreadHealthReport {
    private final String threadName;
    private final int scheduledDelay;
    private final long timestamp;

    public ThreadHealthReport(String threadName, int scheduledDelay, long timestamp) {
      this.threadName = threadName;
      this.scheduledDelay = scheduledDelay;
      this.timestamp = timestamp;
    }

    public String getThreadName() {
      return threadName;
    }

    public int getScheduledDelay() {
      return scheduledDelay;
    }

    public long getTimestamp() {
      return timestamp;
    }

  }

  @VisibleForTesting
  static class ThreadHealthReportGauge implements Gauge<ThreadHealthReport> {

    private volatile ThreadHealthReport threadHealthReport;

    public void setThreadHealthReport(ThreadHealthReport threadHealthReport) {
      this.threadHealthReport = threadHealthReport;
    }

    @Override
    public ThreadHealthReport getValue() {
      return threadHealthReport;
    }
  }

  @VisibleForTesting
  static String getHealthGaugeName(String threadName) {
    return HEALTH_PREFIX + threadName;
  }
}
