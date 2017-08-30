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
package com.streamsets.datacollector.metrics;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class MetricSafeScheduledExecutorService extends SafeScheduledExecutorService {

  // Gauge with all "latest" values that are interesting
  static String KEY_MAX = "max";
  static String KEY_WAITING_COUNT = "waiting";
  static String KEY_PERIODIC_COUNT = "periodic";
  static String KEY_RUNNING_COUNT = "running";
  private final Map<String, Object> gaugeMap;

  public MetricSafeScheduledExecutorService(int corePoolSize, String name, MetricRegistry metricRegistry) {
    super(corePoolSize, name);

    this.gaugeMap = MetricsConfigurator.createFrameworkGauge(
      metricRegistry,
      "safe-executor." + name,
      "runtime",
      null
    ).getValue();
    this.gaugeMap.put(KEY_RUNNING_COUNT, new AtomicInteger(0));
    this.gaugeMap.put(KEY_WAITING_COUNT, new AtomicInteger(0));
    this.gaugeMap.put(KEY_PERIODIC_COUNT, new AtomicInteger(0));
    this.gaugeMap.put(KEY_MAX, corePoolSize);
  }

  @Override
  protected <V> RunnableScheduledFuture<V> decorateTask(Runnable runnable, RunnableScheduledFuture<V> task) {
    return new MetricsTask<>(task);
  }

  @Override
  protected <V> RunnableScheduledFuture<V> decorateTask(Callable<V> callable, RunnableScheduledFuture<V> task) {
    return new MetricsTask<>(task);
  }

  class MetricsTask<V> implements RunnableScheduledFuture<V> {

    private final RunnableScheduledFuture<V> delegate;

    public MetricsTask(RunnableScheduledFuture<V> delegate) {
      this.delegate = delegate;
      if(isPeriodic()) {
        ((AtomicInteger) gaugeMap.get(KEY_PERIODIC_COUNT)).incrementAndGet();
      } else {
        ((AtomicInteger) gaugeMap.get(KEY_WAITING_COUNT)).incrementAndGet();
      }
    }

    @Override
    public boolean isPeriodic() {
      return delegate.isPeriodic();
    }

    @Override
    public long getDelay(@NotNull TimeUnit unit) {
      return delegate.getDelay(unit);
    }

    @Override
    public int compareTo(@NotNull Delayed o) {
      return delegate.compareTo(o);
    }

    @Override
    public void run() {
      if(!isPeriodic()) {
        ((AtomicInteger) gaugeMap.get(KEY_WAITING_COUNT)).decrementAndGet();
      }
      ((AtomicInteger) gaugeMap.get(KEY_RUNNING_COUNT)).incrementAndGet();
      try {
        delegate.run();
      } finally {
        ((AtomicInteger) gaugeMap.get(KEY_RUNNING_COUNT)).decrementAndGet();
      }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      if(isPeriodic()) {
        ((AtomicInteger) gaugeMap.get(KEY_PERIODIC_COUNT)).decrementAndGet();
      }
      return delegate.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
      return delegate.isCancelled();
    }

    @Override
    public boolean isDone() {
      return delegate.isDone();
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
      return delegate.get();
    }

    @Override
    public V get(long timeout, @NotNull TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      return delegate.get(timeout, unit);
    }
  }

}
