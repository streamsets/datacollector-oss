/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.executor;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class SafeScheduledExecutorService {
  private static final Logger LOG = LoggerFactory.getLogger(SafeScheduledExecutorService.class);
  private final ScheduledExecutorService scheduledExecutorService;
  private ExecutorSupport executorSupport = new ExecutorSupport(LOG);

  public SafeScheduledExecutorService(int corePoolSize, final String prefix) {
    this(corePoolSize, new ThreadFactory() {
      private final ThreadFactory defaultThreadFactory = Executors.defaultThreadFactory();
      @Override
      public Thread newThread(Runnable r) {
        Thread thread = defaultThreadFactory.newThread(r);
        thread.setDaemon(true);
        thread.setName(prefix + "-" + thread.getName());
        return thread;
      }
    });
  }

  public SafeScheduledExecutorService(int corePoolSize, ThreadFactory threadFactory) {
    scheduledExecutorService = Executors.newScheduledThreadPool(corePoolSize, threadFactory);
  }

  @VisibleForTesting
  void setExecutorSupport(ExecutorSupport executorSupport) {
    this.executorSupport = executorSupport;
  }

  public void shutdown() {
    scheduledExecutorService.shutdown();
  }

  public boolean isShutdown() {
    return scheduledExecutorService.isShutdown();
  }

  public void shutdownNow() {
    scheduledExecutorService.shutdownNow();
  }

  public void awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    scheduledExecutorService.awaitTermination(timeout, unit);
  }

  public Future<?> submitReturnFuture(final Runnable runnable) {

    return scheduledExecutorService.submit(new SafeRunnable(runnable, true));
  }

  public void submit(final Runnable runnable) {
    scheduledExecutorService.submit(new SafeRunnable(runnable, false));
  }

  public void scheduleAtFixedRate(Runnable command,
                                                            long initialDelay,
                                                            long period,
                                                            TimeUnit unit) {
    scheduledExecutorService.scheduleAtFixedRate(new SafeRunnable(command, false), initialDelay, period, unit);
  }

  public ScheduledFuture<?> scheduleAtFixedRateReturnFuture(Runnable command,
                                                            long initialDelay,
                                                            long period,
                                                            TimeUnit unit) {
    return scheduledExecutorService.scheduleAtFixedRate(new SafeRunnable(command, true), initialDelay, period, unit);
  }
  public void scheduleWithFixedDelay(Runnable command,
                                                   long initialDelay,
                                                   long period,
                                                   TimeUnit unit) {
    scheduledExecutorService.scheduleWithFixedDelay(new SafeRunnable(command, false), initialDelay, period, unit);
  }
  public ScheduledFuture<?> scheduleWithFixedDelayReturnFuture(Runnable command,
                                                long initialDelay,
                                                long period,
                                                TimeUnit unit) {
    return scheduledExecutorService.scheduleWithFixedDelay(new SafeRunnable(command, true), initialDelay, period, unit);
  }
  public ScheduledFuture<?> scheduleReturnFuture(Runnable command, long delay, TimeUnit unit) {
    return scheduledExecutorService.schedule(new SafeRunnable(command, true), delay, unit);
  }
  public void schedule(Runnable command, long delay, TimeUnit unit) {
    scheduledExecutorService.schedule(new SafeRunnable(command, false), delay, unit);
  }
  public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
    return scheduledExecutorService.schedule(callable, delay, unit);
  }

  private class SafeRunnable implements Runnable {
    private final Runnable delegate;
    private final String delegateName;
    private final boolean propagateErrors;
    public SafeRunnable(Runnable delegate, boolean propagateErrors) {
      this.delegate = delegate;
      this.delegateName = delegate.toString(); // call toString() in caller thread in case of error
      this.propagateErrors = propagateErrors;
    }
    public void run() {
      try {
        delegate.run();
      } catch (Throwable throwable) {
        executorSupport.uncaughtThrowableInRunnable(throwable, delegate, delegateName);
        if (propagateErrors) {
          if (throwable instanceof RuntimeException) {
            throw (RuntimeException)throwable;
          } else if (throwable instanceof Error) {
            throw (Error)throwable;
          } else {
            throw new RuntimeException(throwable);
          }
        }
      }
    }
  }
}
