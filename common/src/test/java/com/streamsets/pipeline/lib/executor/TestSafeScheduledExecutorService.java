/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.executor;

import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TestSafeScheduledExecutorService {
  private static final Logger LOG = LoggerFactory.getLogger(TestSafeScheduledExecutorService.class);
  @Test
  public void testCorePoolSize() throws Exception {
    long start = System.currentTimeMillis();
    ScheduledExecutorService executorService = new SafeScheduledExecutorService(1, "test");
    Runnable sleepy = new Runnable() {
      @Override
      public void run() {
        Utils.checkState(ThreadUtil.sleep(110), "Interrupted while sleeping");
      }
    };
    Future<?> future1 = executorService.submit(sleepy);
    Future<?> future2 = executorService.submit(sleepy);
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
    ScheduledExecutorService executorService = new SafeScheduledExecutorService(1, "test");
    Future<?> future = executorService.submit(new RunnableWhichThrows());
    try {
      future.get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }
  @Test(expected = SafeScheduledExecutorServiceRethrowsException.class)
  public void testScheduleAtFixedRateReturnFutureThrowsException() throws Throwable {
    ScheduledExecutorService executorService = new SafeScheduledExecutorService(1, "test");
    Future<?> future = executorService.scheduleAtFixedRate(new RunnableWhichThrows(), 0, 10,
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
    ScheduledExecutorService executorService = new SafeScheduledExecutorService(1, "test");
    Future<?> future = executorService.scheduleWithFixedDelay(new RunnableWhichThrows(), 0, 10,
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
    ScheduledExecutorService executorService = new SafeScheduledExecutorService(1, "test");
    Future<?> future = executorService.schedule(new RunnableWhichThrows(), 0, TimeUnit.DAYS);
    try {
      future.get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
    executorService.shutdown();
  }
  @Test
  public void testScheduleAtFixedRateDoesNotRethrow() throws Throwable {
    SafeScheduledExecutorService executorService = new SafeScheduledExecutorService(1, "test");
    ExecutorSupportForTests executorSupport = new ExecutorSupportForTests(LOG);
    executorService.setExecutorSupport(executorSupport);
    executorService.scheduleAtFixedRateAndForget(new RunnableWhichThrows(), 0, 10, TimeUnit.MILLISECONDS);
    TimeUnit.MILLISECONDS.sleep(25);
    int executionCount = executorSupport.uncaughtThrowableInRunnableCount.get();
    Assert.assertTrue("executionCount is " + executionCount + " which should be >= 2 and <= 5",
      executionCount >= 2 && executionCount <= 5);
  }
}
