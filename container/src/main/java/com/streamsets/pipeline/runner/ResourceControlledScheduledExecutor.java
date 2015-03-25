/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class ResourceControlledScheduledExecutor {
  private static final long DELAY_MINIMUM = 5000; // ms
  private static final Logger LOG = LoggerFactory.getLogger(ResourceControlledScheduledExecutor.class);
  private final ScheduledExecutorService scheduledExecutorService;
  private final List<Runnable> tasks = new CopyOnWriteArrayList<>();
  public ResourceControlledScheduledExecutor(final float maxCpuConsumption) {
    this(maxCpuConsumption, DELAY_MINIMUM);
  }
  public ResourceControlledScheduledExecutor(final float maxCpuConsumption, final long minimumDelay) {
    Utils.checkArgument(maxCpuConsumption > 0, "Max CPU Consumption cannot be less than zero");
    scheduledExecutorService = Executors.newScheduledThreadPool(1, new ThreadFactory()  {
      private final ThreadFactory defaultThreadFactory = Executors.defaultThreadFactory();
      @Override
      public Thread newThread(Runnable r) {
        Thread thread = defaultThreadFactory.newThread(r);
        thread.setDaemon(true);
        thread.setName(thread.getName() + "-ResourceControlledScheduledExecutor");
        return thread;
      }
    });
    scheduledExecutorService.schedule(new Runnable() {
      private final ExponentiallyDecayingReservoir decayingReservoir =
        new ExponentiallyDecayingReservoir();
      @Override
      public void run() {
        long start = System.currentTimeMillis();
        boolean anyThrewError = false;
        for (Runnable task : tasks) {
          try {
            task.run();
          } catch (Throwable throwable) {
            anyThrewError = true;
            // unfortunately ScheduledExecutorService will eat throwables
            // and then stop scheduling runnables which threw them
            LOG.error("Task " + task + " had error: " + throwable, throwable);
          }
        }
        long delay = minimumDelay;
        if (!tasks.isEmpty()) {
          decayingReservoir.update(System.currentTimeMillis() - start);
          delay = calculateDelay(decayingReservoir.getSnapshot().getMedian(), maxCpuConsumption);
        }
        if (anyThrewError) {
          // if a task fails with an exception it may have failed very quickly in which
          // cause we will spin quite quickly spewing exceptions to the logs. If anything
          // errors then we should proceed with caution
          delay = Math.max(delay, TimeUnit.MINUTES.toMillis(1));
        } else if (delay < minimumDelay) {
          delay = minimumDelay;
        }
        scheduledExecutorService.schedule(this, delay, TimeUnit.MILLISECONDS);
      }
    }, 10, TimeUnit.MILLISECONDS);
  }

  public void submit(final Runnable runnable) {
    tasks.add(runnable);
  }

  public void shutdown() {
    scheduledExecutorService.shutdown();
  }

  @VisibleForTesting
  static long calculateDelay(double average, double maxCpuConsumption) {
    return ((long)(average / maxCpuConsumption)) - (long)average;
  }
}
