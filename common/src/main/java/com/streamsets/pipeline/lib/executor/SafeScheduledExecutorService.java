/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.executor;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SafeScheduledExecutorService implements ScheduledExecutorService {
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

  public void shutdown() {
    scheduledExecutorService.shutdown();
  }

  public boolean isShutdown() {
    return scheduledExecutorService.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return false;
  }

  public List<Runnable> shutdownNow() {
    return scheduledExecutorService.shutdownNow();
  }

  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return scheduledExecutorService.awaitTermination(timeout, unit);
  }

  public Future<?> submit(final Runnable runnable) {
    return scheduledExecutorService.submit(new SafeRunnable(runnable, true));
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
    return scheduledExecutorService.invokeAll(tasks);
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
    throws InterruptedException {
    return scheduledExecutorService.invokeAll(tasks, timeout, unit);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
    return scheduledExecutorService.invokeAny(tasks);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
    throws InterruptedException, ExecutionException, TimeoutException {
    return scheduledExecutorService.invokeAny(tasks, timeout, unit);
  }

  public <T> Future<T> submit(Callable<T> task) {
    return scheduledExecutorService.submit(new SafeCallable<>(task, true));
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    return scheduledExecutorService.submit(new SafeRunnable(task, true), result);
  }

  public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
    return scheduledExecutorService.scheduleAtFixedRate(new SafeRunnable(command, true), initialDelay, period, unit);
  }

  public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long period, TimeUnit unit) {
    return scheduledExecutorService.scheduleWithFixedDelay(new SafeRunnable(command, true), initialDelay, period, unit);
  }

  public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
    return scheduledExecutorService.schedule(new SafeRunnable(command, true), delay, unit);
  }

  public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
    return scheduledExecutorService.schedule(callable, delay, unit);
  }

  @Override
  public void execute(Runnable command) {
    scheduledExecutorService.execute(new SafeRunnable(command, false));
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
    @Override
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

  private class SafeCallable<T> implements Callable<T> {

    private final Callable<T> delegate;
    private final String delegateName;
    private final boolean propagateErrors;

    public SafeCallable(Callable<T> delegate, boolean propagateErrors) {
      this.delegate = delegate;
      this.delegateName = delegate.toString(); // call toString() in caller thread in case of error
      this.propagateErrors = propagateErrors;
    }

    @Override
    public T call() throws Exception {
      try {
        return delegate.call();
      } catch (Throwable throwable) {
        executorSupport.uncaughtThrowableInCallable(throwable, delegate, delegateName);
        if (propagateErrors) {
          //Not wrapping in Runtime Exception as it could be StageException when preview validation fails or
          //PipelineRuntimeException when running preview fails.
          throw throwable;
        }
        return null;
      }
    }
  }

  @VisibleForTesting
  void setExecutorSupport(ExecutorSupport executorSupport) {
    this.executorSupport = executorSupport;
  }

  public void scheduleAtFixedRateAndForget(Runnable command, long initialDelay, long period, TimeUnit unit) {
    scheduledExecutorService.scheduleAtFixedRate(new SafeRunnable(command, false), initialDelay, period, unit);
  }

  public void submitAndForget(final Runnable runnable) {
    scheduledExecutorService.submit(new SafeRunnable(runnable, false));
  }

  public void scheduleWithFixedDelayAndForget(Runnable command, long initialDelay, long period, TimeUnit unit) {
    scheduledExecutorService.scheduleWithFixedDelay(new SafeRunnable(command, false), initialDelay, period, unit);
  }

  public void scheduleAndForget(Runnable command, long delay, TimeUnit unit) {
    scheduledExecutorService.schedule(new SafeRunnable(command, false), delay, unit);
  }
}
