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

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestRunnerPool {

  private RunnerPool<String> runnerPool;

  @Before
  public void createRunnerPool() {
    this.runnerPool = new RunnerPool<>(
      ImmutableList.of("a", "b"),
      new RuntimeStats(),
      new Histogram(new ExponentiallyDecayingReservoir())
    );
  }

  @Test(expected = PipelineRuntimeException.class)
  public void testDestroyNotAllReturned() throws Exception {
    String runner = runnerPool.getRunner();
    Assert.assertNotNull(runner);

    runnerPool.destroy();
  }

  @Test(expected = PipelineRuntimeException.class)
  public void testBorrowAfterDestroy() throws Exception {
    runnerPool.destroy();
    runnerPool.getRunner();
  }

  @Test
  public void testGetIdleRunner() throws Exception {
    // There shouldn't be really any runner that hasn't been active for last 60 minutes.
    Assert.assertNull(runnerPool.getIdleRunner(60*60*1000));

    // We should be able to get all the runners as idle as they were all inserted around the same time and the idle
    // time is less then the wait time.
    Thread.sleep(10);
    Assert.assertNotNull(runnerPool.getIdleRunner(5));
    Assert.assertNotNull(runnerPool.getIdleRunner(5));
    Assert.assertNull(runnerPool.getIdleRunner(5));

    // Then we return back "a", followed by 10 ms difference and "b", only the first runner should be really considered
    // idle and it should only be "a". But we should still be able to get "b" using normal path.
    runnerPool.returnRunner("a");
    Thread.sleep(10);
    runnerPool.returnRunner("b");

    String runner = runnerPool.getIdleRunner(5);
    Assert.assertNull(runnerPool.getIdleRunner(5));
    Assert.assertEquals("a", runner);
    Assert.assertEquals("b", runnerPool.getRunner());

    // Same order as last time with the same time spacing - but this time we will ask for runner with more idle time
    // then we waiting, the order should not change.
    runnerPool.returnRunner("a");
    Thread.sleep(10);
    runnerPool.returnRunner("b");

    Assert.assertNull(runnerPool.getIdleRunner(60*60*1000));
    Assert.assertEquals("a", runnerPool.getRunner());
    Assert.assertEquals("b", runnerPool.getRunner());
  }
}
