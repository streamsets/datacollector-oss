/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.production;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.metrics.MetricsConfigurator;

import java.util.HashMap;
import java.util.Map;

public class ThreadHealthReporter {

  private static final String HEALTH_PREFIX = "health.";

  private Map<String, ThreadHealthReportGauge> threadToGaugeMap;
  private MetricRegistry metrics;

  public ThreadHealthReporter(MetricRegistry metrics) {
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
    MetricsConfigurator.createGauge(metrics, getHealthGaugeName(threadName), threadHealthReportGauge);
    threadToGaugeMap.put(threadName, threadHealthReportGauge);
    return true;
  }

  public boolean unregister(String threadName) {
    if(threadToGaugeMap.containsKey(threadName)) {
      threadToGaugeMap.remove(threadName);
      return MetricsConfigurator.removeGauge(metrics, getHealthGaugeName(threadName));
    }
    return true;
  }

  public void setMetrics(MetricRegistry metrics) {
    this.metrics = metrics;
  }

  public void destroy() {
    for(String threadName : threadToGaugeMap.keySet()) {
      MetricsConfigurator.removeGauge(metrics, getHealthGaugeName(threadName));
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
