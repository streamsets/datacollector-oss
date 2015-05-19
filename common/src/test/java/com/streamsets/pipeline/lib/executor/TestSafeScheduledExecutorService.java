/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.executor;

import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import org.junit.Test;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TestSafeScheduledExecutorService {
  private static final Logger LOG = LoggerFactory.getLogger(TestSafeScheduledExecutorService.class);
  @Test
  public void testCorePoolSize() throws Exception {
    long start = System.currentTimeMillis();
    SafeScheduledExecutorService executorService = new SafeScheduledExecutorService(1, "test");
    Runnable sleepy = new Runnable() {
      @Override
      public void run() {
        Utils.checkState(ThreadUtil.sleep(110), "Interrupted while sleeping");
      }
    };
    Future<?> future1 = executorService.submitReturnFuture(sleepy);
    Future<?> future2 = executorService.submitReturnFuture(sleepy);
    future1.get();
    future2.get();
    long elapsed = System.currentTimeMillis() - start;
    // the important aspect of this test is that the time is over 200
    Assert.assertTrue("Elapsed was "  + elapsed + " expected between 200 and 30000",
      elapsed > 200 && elapsed < 30000);
  }
  static class RunnableWhichThrows implements Runnable {
    @Override
    public void run() {
      throw new SafeScheduledExecutorServiceRethrowsException();
    }
  }
  static class ExecutorSupportForTests extends ExecutorSupport {
    private Throwable throwable;
    private AtomicInteger uncaughtThrowableInRunnableCount = new AtomicInteger(0);
    public ExecutorSupportForTests(Logger logger) {
      super(logger);
    }
    public void uncaughtThrowableInRunnable(Throwable throwable, Runnable delegate, String delegateName) {
      uncaughtThrowableInRunnableCount.incrementAndGet();
      this.throwable = throwable;
      super.uncaughtThrowableInRunnable(throwable, delegate, delegateName);
    }
  }
  static class SafeScheduledExecutorServiceRethrowsException extends RuntimeException {

  }
  @Test(expected = SafeScheduledExecutorServiceRethrowsException.class)
  public void testSubmitReturnFutureThrowsException() throws Throwable {
    SafeScheduledExecutorService executorService = new SafeScheduledExecutorService(1, "test");
    Future<?> future = executorService.submitReturnFuture(new RunnableWhichThrows());
    try {
      future.get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }
  @Test(expected = SafeScheduledExecutorServiceRethrowsException.class)
  public void testScheduleAtFixedRateReturnFutureThrowsException() throws Throwable {
    SafeScheduledExecutorService executorService = new SafeScheduledExecutorService(1, "test");
    Future<?> future = executorService.scheduleAtFixedRateReturnFuture(new RunnableWhichThrows(), 0, 10,
      TimeUnit.DAYS);
    try {
      future.get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
    executorService.shutdown();
  }
  @Test(expected = SafeScheduledExecutorServiceRethrowsException.class)
  public void testScheduleWithFixedDelayReturnFutureThrowsException() throws Throwable {
    SafeScheduledExecutorService executorService = new SafeScheduledExecutorService(1, "test");
    Future<?> future = executorService.scheduleWithFixedDelayReturnFuture(new RunnableWhichThrows(), 0, 10,
      TimeUnit.DAYS);
    try {
      future.get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
    executorService.shutdown();
  }
  @Test(expected = SafeScheduledExecutorServiceRethrowsException.class)
  public void testScheduleReturnFutureThrowsException() throws Throwable {
    SafeScheduledExecutorService executorService = new SafeScheduledExecutorService(1, "test");
    Future<?> future = executorService.scheduleReturnFuture(new RunnableWhichThrows(), 0, TimeUnit.DAYS);
    try {
      future.get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
    executorService.shutdown();
  }
  @Test
  public void testScheduleAtFixedRateReturnFutureDoesNotRethrow() throws Throwable {
    SafeScheduledExecutorService executorService = new SafeScheduledExecutorService(1, "test");
    ExecutorSupportForTests executorSupport = new ExecutorSupportForTests(LOG);
    executorService.setExecutorSupport(executorSupport);
    executorService.scheduleAtFixedRate(new RunnableWhichThrows(), 0, 10, TimeUnit.MILLISECONDS);
    TimeUnit.MILLISECONDS.sleep(25);
    int executionCount = executorSupport.uncaughtThrowableInRunnableCount.get();
    Assert.assertTrue("executionCount is " + executionCount + " which should be >= 2 and <= 4",
      executionCount >= 2 && executionCount <= 5);
  }
}
