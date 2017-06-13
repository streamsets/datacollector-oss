/**
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
package com.streamsets.datacollector.runner;

import com.codahale.metrics.Histogram;
import com.streamsets.datacollector.util.ContainerError;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Small abstraction on top of blocking queue to model a pool of runners.
 */
public class RunnerPool <T> {

  /**
   * Internal blocking queue with all available runners
   */
  private final ArrayBlockingQueue<T> queue;

  /**
   * Runtime stats to keep info about available runners.
   */
  private final RuntimeStats runtimeStats;

  /**
   * Histogram with available runners
   */
  private final Histogram histogram;

  /**
   * Create new runner pool.
   *
   * @param runners Runners that this pool object should manage
   */
  public RunnerPool(List<T> runners, RuntimeStats runtimeStats, Histogram histogram) {
    queue = new ArrayBlockingQueue<>(runners.size());
    queue.addAll(runners);

    this.runtimeStats = runtimeStats;
    this.runtimeStats.setTotalRunners(queue.size());
    this.runtimeStats.setAvailableRunners(queue.size());
    this.histogram = histogram;
  }

  /**
   * Get exclusive runner for use.
   *
   * @return Runner that is not being used by anyone else.
   * @throws PipelineRuntimeException Thrown in case that current thread is unexpectedly interrupted
   */
  public T getRunner() throws PipelineRuntimeException {
    try {
      return queue.take();
    } catch (InterruptedException e) {
      throw new PipelineRuntimeException(ContainerError.CONTAINER_0801, e);
    } finally {
      runtimeStats.setAvailableRunners(queue.size());
      histogram.update(queue.size());
    }
  }

  /**
   * Return given runner back to the pool.
   *
   * @param runner Runner to be returned
   */
  public void returnRunner(T runner) {
    queue.add(runner);
    runtimeStats.setAvailableRunners(queue.size());
    histogram.update(queue.size());
  }
}
