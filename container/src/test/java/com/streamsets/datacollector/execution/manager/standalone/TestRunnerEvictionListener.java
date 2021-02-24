/*
 * Copyright 2021 StreamSets Inc.
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

package com.streamsets.datacollector.execution.manager.standalone;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.TestRunner;
import com.streamsets.datacollector.execution.manager.PipelineStateImpl;
import com.streamsets.pipeline.api.ExecutionMode;
import org.junit.Assert;
import org.junit.Test;

public class TestRunnerEvictionListener {

  @Test
  public void testRunnerEvictionListenerEntryRemoved() {
    Cache<String, StandaloneAndClusterPipelineManager.RunnerInfo> cache = CacheBuilder.newBuilder()
        .build();
    RunnerEvictionListener runnerEvictionListener = new RunnerEvictionListener(cache);

    PipelineState pipelineStatus1 = new PipelineStateImpl("user",
        "ns:name",
        "rev",
        PipelineStatus.STOPPED,
        null,
        System.currentTimeMillis(),
        null,
        ExecutionMode.STANDALONE,
        null,
        0,
        -1
    );
    TestRunner.Runner1 testRunner = new TestRunner.Runner1();

    StandaloneAndClusterPipelineManager.RunnerInfo runnerInfo = new StandaloneAndClusterPipelineManager.RunnerInfo(testRunner,
        ExecutionMode.STANDALONE);
    cache.put(StandaloneAndClusterPipelineManager.getNameAndRevString(pipelineStatus1.getPipelineId(), pipelineStatus1.getRev())
        , runnerInfo);
    runnerEvictionListener.onStateChange(null, pipelineStatus1, null, null, null);
    Assert.assertEquals(0, cache.size());
  }

  @Test
  public void testRunnerEvictionListenerEntryNotRemoved() {
    Cache<String, StandaloneAndClusterPipelineManager.RunnerInfo> cache = CacheBuilder.newBuilder()
        .build();
    RunnerEvictionListener runnerEvictionListener = new RunnerEvictionListener(cache);

    PipelineState pipelineStatus1 = new PipelineStateImpl("user",
        "ns:name",
        "rev",
        PipelineStatus.RUNNING,
        null,
        System.currentTimeMillis(),
        null,
        ExecutionMode.STANDALONE,
        null,
        0,
        -1
    );
    TestRunner.Runner1 testRunner = new TestRunner.Runner1();

    StandaloneAndClusterPipelineManager.RunnerInfo runnerInfo = new StandaloneAndClusterPipelineManager.RunnerInfo(testRunner,
        ExecutionMode.STANDALONE);
    cache.put(StandaloneAndClusterPipelineManager.getNameAndRevString(pipelineStatus1.getPipelineId(), pipelineStatus1.getRev())
        , runnerInfo);
    runnerEvictionListener.onStateChange(null, pipelineStatus1, null, null, null);
    Assert.assertEquals(1, cache.size());
  }

}
