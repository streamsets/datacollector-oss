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

import com.codahale.metrics.MetricRegistry;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.creation.PipelineConfigBean;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.event.json.MetricRegistryJson;
import com.streamsets.datacollector.runner.production.BadRecordsHandler;
import com.streamsets.datacollector.runner.production.StatsAggregationHandler;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;

import java.util.List;

/**
 * Implementation of this runner are responsible for running the pipeline - via pipes concept.
 *
 * We currently have two main implementation - one for preview and one for production run (that is shared for both
 * standalone and cluster modes).
 */
public interface PipelineRunner extends PipelineFinisherDelegate {

  BuildInfo getBuildInfo();

  public RuntimeInfo getRuntimeInfo();

  public boolean isPreview();

  public MetricRegistry getMetrics();

  /**
   * Run given event record (pipeline lifecycle event) on given stage (handler).
   */
  public void runLifecycleEvent(
    Record eventRecord,
    StageRuntime stageRuntime
  ) throws StageException, PipelineRuntimeException;

  /**
   * Run the pipeline's pipe.
   */
  public void run(
    SourcePipe originPipe,
    List<PipeRunner> pipes,
    BadRecordsHandler badRecordsHandler,
    StatsAggregationHandler statsAggregationHandler
  ) throws StageException, PipelineRuntimeException;

  /**
   * Run the pipeline with overriding some stage outputs.
   *
   * This is specifically for preview and will throw exception if used on non-preview runner.
   */
  public void run(
    SourcePipe originPipe,
    List<PipeRunner> pipes,
    BadRecordsHandler badRecordsHandler,
    List<StageOutput> stageOutputsToOverride,
    StatsAggregationHandler statsAggregationHandler
  ) throws StageException, PipelineRuntimeException;

  /**
   * Destroy the pipeline.
   *
   * Since stages are allowed to generate events during destroy phase, the destroy() is delegated to runner and it means
   * that we will run one final batch to make sure that all events are properly propagated and processed.
   */
  public void destroy(
    SourcePipe originPipe,
    List<PipeRunner> pipes,
    BadRecordsHandler badRecordsHandler,
    StatsAggregationHandler statsAggregationHandler
  ) throws StageException, PipelineRuntimeException;

  public List<List<StageOutput>> getBatchesOutput();

  public void setObserver(Observer observer);

  public void registerListener(BatchListener batchListener);

  public MetricRegistryJson getMetricRegistryJson();

  void errorNotification(
    SourcePipe originPipe,
    List<PipeRunner> pipes,
    Throwable throwable
  );

  /**
   * Configure various runtime structures that the might need about the pipeline execution.
   */
  public void setRuntimeConfiguration(
    PipeContext pipeContext,
    PipelineConfiguration pipelineConfiguration,
    PipelineConfigBean pipelineConfigBean
  );

}
