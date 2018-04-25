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
package com.streamsets.pipeline.lib.executor;

import com.streamsets.datacollector.security.GroupsInScope;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.Callable;
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
    Assert.assertTrue("Elapsed was " + elapsed + " expected between 200 and 30000", elapsed > 200 && elapsed < 30000);
  }

  static class RunnableWhichThrows implements Runnable {
    @Override
    public void run() {
      throw new SafeScheduledExecutorServiceRethrowsException();
    }
  }

  static class ExecutorSupportForTests extends ExecutorSupport {
    private AtomicInteger uncaughtThrowableInRunnableCount = new AtomicInteger(0);

    public ExecutorSupportForTests(Logger logger) {
      super(logger);
    }

    public void uncaughtThrowableInRunnable(Throwable throwable, Runnable delegate, String delegateName) {
      uncaughtThrowableInRunnableCount.incrementAndGet();
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
    Future<?> future = executorService.scheduleAtFixedRate(new RunnableWhichThrows(), 0, 10, TimeUnit.DAYS);
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
    Future<?> future = executorService.scheduleWithFixedDelay(new RunnableWhichThrows(), 0, 10, TimeUnit.DAYS);
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

  @Ignore
  @Test
  public void testScheduleAtFixedRateDoesNotRethrow() throws Throwable {
    SafeScheduledExecutorService executorService = new SafeScheduledExecutorService(1, "test");
    ExecutorSupportForTests executorSupport = new ExecutorSupportForTests(LOG);
    executorService.setExecutorSupport(executorSupport);
    executorService.scheduleAtFixedRateAndForget(new RunnableWhichThrows(), 0, 10, TimeUnit.MILLISECONDS);
    TimeUnit.MILLISECONDS.sleep(25);
    int executionCount = executorSupport.uncaughtThrowableInRunnableCount.get();
    Assert.assertTrue(
        "executionCount is " + executionCount + " which should be >= 2 and <= 5",
        executionCount >= 2 && executionCount <= 5
    );
  }

  @Test
  public void testGroupsInContextRunnable() throws Exception {
    SafeScheduledExecutorService executorService = new SafeScheduledExecutorService(1, "test");

    GroupsInScope.execute(Collections.singleton("g"), () -> {
      Future future1 = executorService.submit(new Runnable() {
        @Override
        public void run(){
          Assert.assertEquals(Collections.singleton("g"), GroupsInScope.getUserGroupsInScope());
        }
      });
      future1.get();
      return null;
    });

    executorService.shutdown();
  }

  @Test
  public void testSubjectInContextCallable() throws Exception {
    SafeScheduledExecutorService executorService = new SafeScheduledExecutorService(1, "test");

    GroupsInScope.execute(Collections.singleton("g"), () -> {
      Future future1 = executorService.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          Assert.assertEquals(Collections.singleton("g"), GroupsInScope.getUserGroupsInScope());
          return null;
        }
      });
      future1.get();
      return null;
    });

    executorService.shutdown();
  }

}
