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

  private static final Logger LOG = LoggerFactory.getLogger(RulesConfigLoaderRunnable.class);

  private final RulesConfigLoader rulesConfigLoader;
  private final Observer observer;

  public RulesConfigLoaderRunnable(RulesConfigLoader rulesConfigLoader, Observer observer) {
    this.rulesConfigLoader = rulesConfigLoader;
    this.observer = observer;
  }

  @Override
  public void run() {
    Thread.currentThread().setName("RulesConfigLoaderRunnable");
    try {
      rulesConfigLoader.load(observer);
    } catch (Exception e) {
      LOG.error("Stopping the Rules Config Loader, Reason: {}", e.getMessage(), e);
      return;
    }
  }
}
