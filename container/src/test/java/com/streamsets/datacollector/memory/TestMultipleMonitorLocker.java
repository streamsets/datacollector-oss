/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.memory;

import com.streamsets.pipeline.lib.util.ThreadUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestMultipleMonitorLocker {
  private static final Logger LOG = LoggerFactory.getLogger(TestMultipleMonitorLocker.class);
  private ExecutorService executorService;

  @Before
  public void setup() {
    executorService = Executors.newCachedThreadPool();
  }

  @After
  public void shutdown() {
    LOG.info("Shutting down test");
    if (executorService != null) {
      executorService.shutdownNow();
    }
  }

  private static class DummyException extends Exception {
  }

  @Test
  public void testError() throws Exception {
    List<Object> locks = new ArrayList<>();
    locks.add(new Object());
    try {
      MultipleMonitorLocker.lock(locks, new Callable<Object>() {
        @Override
        public Object call() throws Exception {
          throw new DummyException();
        }
      });
      Assert.fail("Expected DummyException");
    } catch (DummyException ex) {
      // expected
    }
    Assert.assertFalse("Should not hold lock", Thread.holdsLock(locks.get(0)));
  }

  @Test
  public void testNoError() throws Exception {
    List<Object> locks = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      locks.add(new Object());
    }
    Object lockResult1 = MultipleMonitorLocker.lock(locks, new DummyCallable());
    Assert.assertNotNull("Should have been able to lock object", lockResult1);
    lockForPeriod(locks.get(locks.size() - 1), "testEndurance-1", 5000);
    Assert.assertTrue(ThreadUtil.sleep(500));
    Object lockResult2 = MultipleMonitorLocker.lock(locks, new DummyCallable());
    Assert.assertNull("Should not have been able to lock object", lockResult2);
  }

  private static class DummyCallable implements Callable<Object> {
    @Override
    public Object call() throws Exception {
      return new Object();
    }
  }

  private void lockForPeriod(final Object lock, final String name, final long lockTimeMs) {
    executorService.submit(new Runnable() {
      @Override
      public void run() {
       synchronized (lock) {
         if (!ThreadUtil.sleep(lockTimeMs)) {
          LOG.info("Lock thread {} was interrupted", name);
         }
       }
      }
    });
  }
}
