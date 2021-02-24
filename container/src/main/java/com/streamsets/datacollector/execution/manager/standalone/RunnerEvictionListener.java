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
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.Runner;
import com.streamsets.datacollector.execution.StateEventListener;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.dc.execution.manager.standalone.ThreadUsage;
import com.streamsets.pipeline.api.impl.Utils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.tuple.Pair;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class RunnerEvictionListener implements StateEventListener {

  private static final Logger LOG = LoggerFactory.getLogger(RunnerEvictionListener.class);
  private Cache<String, StandaloneAndClusterPipelineManager.RunnerInfo> runnerCache;
  public RunnerEvictionListener(
      Cache<String, StandaloneAndClusterPipelineManager.RunnerInfo> runnerCache
  ) {
    this.runnerCache = runnerCache;
  }

  @Override
  public void onStateChange(
      PipelineState fromState,
      PipelineState toState,
      String toStateJson,
      ThreadUsage threadUsage,
      Map<String, String> offset
  ) {
    if (!toState.getStatus().isActive()) {
      String cacheKey = StandaloneAndClusterPipelineManager.getNameAndRevString(toState.getPipelineId(), toState.getRev());
      StandaloneAndClusterPipelineManager.RunnerInfo runnerInfo = runnerCache.getIfPresent(cacheKey);
      if (runnerInfo != null) {
        Runner runner = runnerInfo.getRunner();
        LOG.info("Removing runner for pipeline {}", toState.getPipelineId());
        runnerCache.invalidate(cacheKey);
        runner.close();
      } else {
        // can happen when a pipeline is deleted
        LOG.debug("Runner is not present in cache for pipeline: {}", toState.getPipelineId());
      }
    }
  }


}


