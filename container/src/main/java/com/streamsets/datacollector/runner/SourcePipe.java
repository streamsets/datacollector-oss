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

import com.streamsets.datacollector.memory.MemoryUsageCollectorResourceBundle;
import com.streamsets.datacollector.restapi.bean.MetricRegistryJson;
import com.streamsets.datacollector.runner.production.ReportErrorDelegate;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.ErrorMessage;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Specific pipe for sources that have additional methods to deal with push ones.
 */
public class SourcePipe extends StagePipe implements ReportErrorDelegate {

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
   * Report error delegate for PushSource.
   */
  private ReportErrorDelegate reportErrorDelegate;

  /**
   * Process method for Push source that will give control of the execution to the origin.
   *
   * @param offsets Offsets from last execution
   * @param batchSize Maximal configured batch size
   */
  public void process(
    Map<String, String> offsets,
    int batchSize,
    ReportErrorDelegate reportErrorDelegate
  ) throws StageException, PipelineRuntimeException {
    this.reportErrorDelegate = reportErrorDelegate;
    getStage().setReportErrorDelegate(this);
    getStage().execute(offsets, batchSize);
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

    batchContext.setOriginStageName(
      getStage().getInfo().getInstanceName(),
      getStage().getInfo().getLabel()
    );
  }

  /**
   * Finish batch from the origin's perspective.
   *
   * @param batchContext Batch context enriched by prepareBatchContext
   * @return Map with statistics that are usually stored inside the Pipe object itself.
   */
  public Map<String, Object> finishBatchContext(BatchContextImpl batchContext) throws StageException {
    return finishBatchAndCalculateMetrics(
      batchContext.getStartTime(),
      batchContext.getPipeBatch(),
      (BatchMakerImpl) batchContext.getBatchMaker(),
      batchContext.getPipeBatch().getBatch(this, Collections.emptyList()),
      batchContext.getPipeBatch().getErrorSink(),
      batchContext.getPipeBatch().getEventSink(),
      null
    );
  }

  @Override
  public void reportError(String stage, ErrorMessage errorMessage) {
    increaseStageErrorMetrics(1);
    reportErrorDelegate.reportError(stage, errorMessage);
  }
}
