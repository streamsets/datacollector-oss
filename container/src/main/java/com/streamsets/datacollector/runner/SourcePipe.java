/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.streamsets.datacollector.memory.MemoryUsageCollectorResourceBundle;
import com.streamsets.datacollector.restapi.bean.MetricRegistryJson;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.StageException;

import java.util.List;
import java.util.Map;

/**
 * Specific pipe for sources that have additional methods to deal with push ones.
 */
public class SourcePipe extends StagePipe {

  public SourcePipe(
    String name,
    String rev,
    Configuration configuration,
    StageRuntime stage,
    List<String> inputLanes,
    List<String> outputLanes,
    List<String> eventLanes,
    ResourceControlledScheduledExecutor scheduledExecutorService,
    MemoryUsageCollectorResourceBundle memoryUsageCollectorResourceBundle,
    MetricRegistryJson metricRegistryJson
  ) {
    super(
      name,
      rev,
      configuration,
      stage,
      inputLanes,
      outputLanes,
      eventLanes,
      scheduledExecutorService,
      memoryUsageCollectorResourceBundle,
      metricRegistryJson
    );
  }

  /**
   * Process method for Push source that will give control of the execution to the origin.
   *
   * @param batchSize Maximal configured batch size
   */
  public void process(int batchSize) throws StageException, PipelineRuntimeException {
    getStage().execute(
      null,
      batchSize,
      null,
      null,
      null,
      null
    );
  }

  /**
   * Called by PipelineRunner when push origin started a new batch to prepare context for it.
   *
   * @param batchContext BatchContext associated with the starting batch.
   */
  public void prepareBatchContext(BatchContextImpl batchContext) {
    PipeBatch pipeBatch = batchContext.getPipeBatch();

    // Start stage in the pipe batch and persist reference to batch maker in the batch context
    BatchMakerImpl batchMaker = pipeBatch.startStage(this);
    batchContext.setBatchMaker(batchMaker);

    updateStatsAtStart(batchContext.getStartTime());
  }

  /**
   * Finish batch from the origin's perspective.
   *
   * @param batchContext Batch context enriched by prepareBatchContext
   * @return Map with statistics that are usually stored inside the Pipe object itself.
   */
  public Map<String, Object> finishBatchContext(BatchContextImpl batchContext) {
    return finishBatchAndCalculateMetrics(
      batchContext.getStartTime(),
      batchContext.getPipeBatch(),
      (BatchMakerImpl) batchContext.getBatchMaker(),
      batchContext.getPipeBatch().getBatch(this),
      batchContext.getPipeBatch().getErrorSink(),
      batchContext.getPipeBatch().getEventSink(),
      "ignored-offset-for-now"
    );
  }

}
