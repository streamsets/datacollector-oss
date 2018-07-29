/*
 * Copyright 2018 StreamSets Inc.
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This runnable can be scheduled to run periodically with an instance of ProductionPipelineRunner to call the method
 * produceEmptyBatchesForIdleRunners() - e.g. to make sure that each runner is executed at least once in given time
 * period.
 */
public class ProduceEmptyBatchesForIdleRunnersRunnable implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(ProduceEmptyBatchesForIdleRunnersRunnable.class);

  /**
   * Pipeline Runner responsible for running this pipeline.
   */
  private final ProductionPipelineRunner pipelineRunner;

  /**
   * Idle time in ms.
   */
  private final long idleTime;

  public ProduceEmptyBatchesForIdleRunnersRunnable(
    ProductionPipelineRunner pipelineRunner,
    long idleTime
  ) {
    this.pipelineRunner = pipelineRunner;
    this.idleTime = idleTime;
  }

  @Override
  public void run() {
    String originalName = Thread.currentThread().getName();
    Thread.currentThread().setName("Pipeline Idle Runner");
    try {
      pipelineRunner.produceEmptyBatchesForIdleRunners(idleTime);
    } catch (Exception e) {
      LOG.error("Error when producing empty batch for idle runner: " + e.toString(), e);
    }
    Thread.currentThread().setName(originalName);
  }
}
