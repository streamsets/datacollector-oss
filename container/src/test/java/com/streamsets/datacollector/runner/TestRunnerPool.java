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
}
