/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.production;

import com.streamsets.pipeline.runner.Observer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RulesConfigLoaderRunnable implements Runnable {

  public static final int SCHEDULED_DELAY = 2;
  public static final String RUNNABLE_NAME = "RulesConfigLoaderRunnable";
  private static final Logger LOG = LoggerFactory.getLogger(RulesConfigLoaderRunnable.class);

  private final RulesConfigLoader rulesConfigLoader;
  private final Observer observer;
  private final ThreadHealthReporter threadHealthReporter;

  public RulesConfigLoaderRunnable(ThreadHealthReporter threadHealthReporter, RulesConfigLoader rulesConfigLoader,
                                   Observer observer) {
    this.rulesConfigLoader = rulesConfigLoader;
    this.observer = observer;
    this.threadHealthReporter = threadHealthReporter;
  }

  @Override
  public void run() {
    String originalName = Thread.currentThread().getName();
    Thread.currentThread().setName(originalName + "-" + RUNNABLE_NAME);
    try {
      threadHealthReporter.reportHealth(RUNNABLE_NAME, SCHEDULED_DELAY, System.currentTimeMillis());
      rulesConfigLoader.load(observer);
    } catch (Exception e) {
      LOG.error("Stopping the Rules Config Loader, Reason: {}", e.getMessage(), e);
      return;
    } finally {
      Thread.currentThread().setName(originalName);
    }
  }
}
