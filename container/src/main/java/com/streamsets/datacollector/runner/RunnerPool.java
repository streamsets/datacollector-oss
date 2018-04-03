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
package com.streamsets.datacollector.runner;

import com.codahale.metrics.Histogram;
import com.streamsets.datacollector.util.ContainerError;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Small abstraction on top of blocking queue to model a pool of runners.
 */
public class RunnerPool <T> {

  /**
   * Wrapper for the pool items to remember when they were inserted (what time).
   */
  private static class QueueItem<T> implements Comparable<QueueItem> {
    /**
     * Timestamp of the insertion to the queue.
     */
    long timestamp;

    /**
     * Runner instance itself.
     */
    T runner;

    QueueItem(T runner) {
      this.runner = runner;
      this.timestamp = System.currentTimeMillis();
    }

    @Override
    public int compareTo(@NotNull QueueItem other) {
      return (int) (this.timestamp - other.timestamp);
    }
  }

  /**
   * Internal blocking queue with all available runners
   */
  private final PriorityBlockingQueue<QueueItem<T>> queue;

  /**
   * Runtime stats to keep info about available runners.
   */
  private final RuntimeStats runtimeStats;

  /**
   * Histogram with available runners
   */
  private final Histogram histogram;

  /**
   * Internal flag keeping state of the runner.
   */
  private final AtomicBoolean destroyed;

  /**
   * Create new runner pool.
   *
   * @param runners Runners that this pool object should manage
   */
  public RunnerPool(List<T> runners, RuntimeStats runtimeStats, Histogram histogram) {
    queue = new PriorityBlockingQueue<>(runners.size());
    runners.forEach(runner -> queue.add(new QueueItem<>(runner)));

    this.runtimeStats = runtimeStats;
    this.runtimeStats.setTotalRunners(queue.size());
    this.runtimeStats.setAvailableRunners(queue.size());
    this.histogram = histogram;
    this.destroyed = new AtomicBoolean(false);
  }

  /**
   * Get exclusive runner for use.
   *
   * @return Runner that is not being used by anyone else.
   * @throws PipelineRuntimeException Thrown in case that current thread is unexpectedly interrupted
   */
  public T getRunner() throws PipelineRuntimeException {
    validateNotDestroyed();

    try {
      return queue.take().runner;
    } catch (InterruptedException e) {
      throw new PipelineRuntimeException(ContainerError.CONTAINER_0801, e);
    } finally {
      runtimeStats.setAvailableRunners(queue.size());
      histogram.update(queue.size());
    }
  }

  /**
   * Return a runner that haven't been used at least for the configured number of milliseconds.
   *
   * @param idleTime Number of milliseconds when a runner is considered an "idle"
   * @return First runner that fits such criteria or null if there is no such runner
   */
  public T getIdleRunner(long idleTime) {
    // Take the first runner
    QueueItem<T> item = queue.poll();

    // All runners might be currently in use, which is fine in this case.
    if(item == null) {
      return null;
    }

    // If the runner wasn't idle for the expected time, we need to put it back to the queue (it will be added to the
    // begging again).
    if((System.currentTimeMillis() - item.timestamp) < idleTime) {
      queue.add(item);
      return null;
    }

    // Otherwise we do have runner that hasn't been used for at least idleTime, so we can return it now
    return item.runner;
  }

  /**
   * Return given runner back to the pool.
   *
   * @param runner Runner to be returned
   */
  public void returnRunner(T runner) throws PipelineRuntimeException {
    validateNotDestroyed();

    queue.add(new QueueItem<>(runner));
    runtimeStats.setAvailableRunners(queue.size());
    histogram.update(queue.size());
  }

  /**
   * Destroy only the pool itself - not the individual pipe runners.
   *
   * This method will also validate that all runners were properly returned to the pool. PipelineRuntimeException will
   * be thrown if some runners are still running.
   *
   * @throws PipelineRuntimeException
   */
  public void destroy() throws PipelineRuntimeException {
    // Firstly set this runner as destroyed
    destroyed.set(true);

    // Validate that this thread pool have all runners back, otherwise we're missing something and that is sign of
    // a trouble.
    if(queue.size() < runtimeStats.getTotalRunners()) {
      throw new PipelineRuntimeException(ContainerError.CONTAINER_0802, queue.size(), runtimeStats.getTotalRunners());
    }
  }

  /**
   * Throw an exception if the runner was already destroyed.
   *
   * @throws PipelineRuntimeException
   */
  private void validateNotDestroyed() throws PipelineRuntimeException {
    if(destroyed.get()) {
      throw new PipelineRuntimeException(ContainerError.CONTAINER_0803, queue.size(), runtimeStats.getTotalRunners());
    }
  }
}
