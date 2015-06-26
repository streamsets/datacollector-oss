/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.runner;

import com.streamsets.pipeline.runner.production.ThreadHealthReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

public class MetricObserverRunnable implements Runnable {

  public static final int SCHEDULED_DELAY = 1;
  public static final String RUNNABLE_NAME = "MetricObserverRunnable";
  private static final Logger LOG = LoggerFactory.getLogger(MetricObserverRunnable.class);

  private final MetricsObserverRunner metricsObserverRunner;
  private final ThreadHealthReporter threadHealthReporter;

  @Inject
  public MetricObserverRunnable(ThreadHealthReporter threadHealthReporter, MetricsObserverRunner metricsObserverRunner) {
    this.metricsObserverRunner = metricsObserverRunner;
    this.threadHealthReporter = threadHealthReporter;
  }

  @Override
  public void run() {
    String originalName = Thread.currentThread().getName();
    Thread.currentThread().setName(originalName + "-" + RUNNABLE_NAME);
    try {
      threadHealthReporter.reportHealth(RUNNABLE_NAME, SCHEDULED_DELAY, System.currentTimeMillis());
      metricsObserverRunner.evaluate();
    } finally {
      Thread.currentThread().setName(originalName);
    }
  }
}
