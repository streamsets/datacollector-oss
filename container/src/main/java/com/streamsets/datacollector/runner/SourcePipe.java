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

import com.streamsets.datacollector.event.json.MetricRegistryJson;
import com.streamsets.datacollector.runner.production.ReportErrorDelegate;
import com.streamsets.datacollector.usagestats.StatsCollector;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.lib.log.LogConstants;
import org.apache.log4j.MDC;

import java.util.List;
import java.util.Map;

/**
 * Specific pipe for sources that have additional methods to deal with push ones.
 */
public class SourcePipe extends StagePipe implements ReportErrorDelegate {

  private final StatsCollector statsCollector;

  public SourcePipe(
    String name,
    String rev,
    StageRuntime stage,
    List<String> inputLanes,
    List<String> outputLanes,
    List<String> eventLanes,
    StatsCollector statsCollector,
    MetricRegistryJson metricRegistryJson
  ) {
    super(
      name,
      rev,
      stage,
      inputLanes,
      outputLanes,
      eventLanes,
      metricRegistryJson
    );
    this.statsCollector = statsCollector;
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

    try {
      MDC.put(LogConstants.STAGE, getStage().getInfo().getInstanceName());
      getStage().execute(offsets, batchSize);
    } finally {
      MDC.put(LogConstants.STAGE, "");
    }
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
      batchContext.getPipeBatch().getBatch(this),
      batchContext.getPipeBatch().getErrorSink(),
      batchContext.getPipeBatch().getEventSink(),
      null
    );
  }

  protected Map<String, Object> finishBatchAndCalculateMetrics(
    long startTimeInStage,
    PipeBatch pipeBatch,
    BatchMakerImpl batchMaker,
    BatchImpl batchImpl,
    ErrorSink errorSink,
    EventSink eventSink,
    String newOffset
  ) throws StageException {
    statsCollector.incrementRecordCount(batchMaker.getSize());

    return super.finishBatchAndCalculateMetrics(
      startTimeInStage,
      pipeBatch,
      batchMaker,
      batchImpl,
      errorSink,
      eventSink,
      newOffset
    );
  }

  @Override
  public void reportError(String stage, ErrorMessage errorMessage) {
    increaseStageErrorMetrics(1);
    reportErrorDelegate.reportError(stage, errorMessage);
  }
}
