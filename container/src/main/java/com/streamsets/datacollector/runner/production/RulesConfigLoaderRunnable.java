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
package com.streamsets.datacollector.runner.production;

import com.streamsets.datacollector.execution.runner.common.RulesConfigLoader;
import com.streamsets.datacollector.execution.runner.common.ThreadHealthReporter;
import com.streamsets.datacollector.runner.Observer;

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
      LOG.warn("Stopping the Rules Config Loader, Reason: {}", e.toString(), e);
      return;
    } finally {
      Thread.currentThread().setName(originalName);
    }
  }
}
