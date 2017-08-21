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

import org.junit.Assert;
import org.junit.Test;

import com.streamsets.datacollector.runner.ResourceControlledScheduledExecutor;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TestResourceControlledScheduledExecutor {

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
    Assert.assertTrue("Expected between 30 and 60 executions but got: " + result, result >= 30 && result <= 60);
  }
}
