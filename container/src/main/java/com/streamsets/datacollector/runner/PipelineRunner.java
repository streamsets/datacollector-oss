/**
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
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.bean.MetricRegistryJson;
import com.streamsets.datacollector.runner.production.BadRecordsHandler;
import com.streamsets.datacollector.runner.production.StatsAggregationHandler;
import com.streamsets.pipeline.api.StageException;

import java.util.List;

public interface PipelineRunner extends PipelineFinisherDelegate {

  public RuntimeInfo getRuntimeInfo();

  public boolean isPreview();

  public MetricRegistry getMetrics();

  public void run(
    SourcePipe originPipe,
    List<PipeRunner> pipes,
    BadRecordsHandler badRecordsHandler,
    StatsAggregationHandler statsAggregationHandler
  ) throws StageException, PipelineRuntimeException;

  public void run(
    SourcePipe originPipe,
    List<PipeRunner> pipes,
    BadRecordsHandler badRecordsHandler,
    List<StageOutput> stageOutputsToOverride,
    StatsAggregationHandler statsAggregationHandler
  ) throws StageException, PipelineRuntimeException;

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

  public void setPipeContext(PipeContext pipeContext);

  public void setPipelineConfiguration(PipelineConfiguration pipelineConfiguration);

}
