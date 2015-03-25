/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TestResourceControlledScheduledExecutor {

  private final long startTime = System.currentTimeMillis() - 60000L;


  @Test
  public void testTypicalAverage() {
    Assert.assertEquals(5940000, ResourceControlledScheduledExecutor.calculateDelay(60000L, 0.01));
    Assert.assertEquals(9900, ResourceControlledScheduledExecutor.calculateDelay(100, 0.01));
    Assert.assertEquals(1881, ResourceControlledScheduledExecutor.calculateDelay(19, 0.01));
    Assert.assertEquals(171, ResourceControlledScheduledExecutor.calculateDelay(19, 0.10));
  }
  @Test(expected = IllegalArgumentException.class)
  public void testNegativeCpuConstructor() {
    new ResourceControlledScheduledExecutor(-0.1f);
  }

  @Test
  public void testExecution() throws Exception {
    ResourceControlledScheduledExecutor executor = new ResourceControlledScheduledExecutor(0.5f, 1);
    final AtomicInteger executions = new AtomicInteger(0);
    Runnable runnable = new Runnable() {
      @Override
      public void run() {
        executions.incrementAndGet();
        long start = System.currentTimeMillis();
        while ((System.currentTimeMillis() - start) <= 100);
      }
    };
    // the first couple executions will happen quickly but overtime
    // we should see about 50% of the remaining 9 seconds consumed
    // with the runnable executing
    executor.submit(runnable);
    executor.submit(runnable);
    TimeUnit.SECONDS.sleep(9);
    executor.shutdown();
    int result = executions.get();
    Assert.assertTrue("Expected between 40 and 60 executions but got: " + result, result >= 40 && result <= 50);
  }
}
