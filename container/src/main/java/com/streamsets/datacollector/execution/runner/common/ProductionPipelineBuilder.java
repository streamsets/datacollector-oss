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
package com.streamsets.datacollector.execution.runner.common;

import com.streamsets.datacollector.blobstore.BlobStoreTask;
import com.streamsets.datacollector.config.ConnectionConfiguration;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.creation.PipelineBeanCreator;
import com.streamsets.datacollector.event.dto.PipelineStartEvent;
import com.streamsets.datacollector.lineage.LineagePublisherTask;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.runner.Observer;
import com.streamsets.datacollector.runner.Pipeline;
import com.streamsets.datacollector.runner.PipelineRuntimeException;
import com.streamsets.datacollector.runner.SourceOffsetTracker;
import com.streamsets.datacollector.runner.UserContext;
import com.streamsets.datacollector.runner.production.ProductionSourceOffsetCommitterOffsetTracker;
import com.streamsets.datacollector.runner.production.ProductionSourceOffsetTracker;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.usagestats.StatsCollector;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.validation.PipelineConfigurationValidator;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.StageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProductionPipelineBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(ProductionPipelineBuilder.class);
  private static final String PRODUCTION_PIPELINE_SUFFIX = ":production";

  private final StageLibraryTask stageLib;
  private final String name;
  private final String rev;
  private final Configuration configuration;
  private final RuntimeInfo runtimeInfo;
  private final BuildInfo buildInfo;
  private final BlobStoreTask blobStoreTask;
  private final LineagePublisherTask lineagePublisherTask;
  private final StatsCollector statsCollector;

  private final ProductionPipelineRunner runner;
  private final Observer observer;

  public ProductionPipelineBuilder(
      @Named("name") String name,
      @Named("rev") String rev,
      Configuration configuration,
      RuntimeInfo runtimeInfo,
      BuildInfo buildInfo,
      StageLibraryTask stageLib,
      ProductionPipelineRunner runner,
      Observer observer,
      BlobStoreTask blobStoreTask,
      LineagePublisherTask lineagePublisherTask,
      StatsCollector statsCollector
  ) {
    this.name = name;
    this.rev = rev;
    this.configuration = configuration;
    this.runtimeInfo = runtimeInfo;
    this.buildInfo = buildInfo;
    this.stageLib = stageLib;
    this.runner = runner;
    this.observer = observer;
    this.blobStoreTask = blobStoreTask;
    this.lineagePublisherTask = lineagePublisherTask;
    this.statsCollector = statsCollector;
    PipelineBeanCreator.prepareForConnections(configuration, runtimeInfo);
  }

  public ProductionPipeline build(
      UserContext userContext,
      PipelineConfiguration pipelineConf,
      long startTime
  ) throws PipelineRuntimeException, StageException {
    return build(userContext, pipelineConf, startTime, Collections.emptyList(), null, new HashMap<>());
  }

  public ProductionPipeline build(
      UserContext userContext,
      PipelineConfiguration pipelineConf,
      long startTime,
      List<PipelineStartEvent.InterceptorConfiguration> interceptorConfs,
      Map<String, Object> runtimeParameters,
      Map<String, ConnectionConfiguration> connections
  ) throws PipelineRuntimeException, StageException {
    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(
        stageLib,
        buildInfo,
        name,
        pipelineConf,
        userContext.getUser(),
        connections
    );
    pipelineConf = validator.validate();
    if (validator.getIssues().hasIssues()) {
      throw new PipelineRuntimeException(ContainerError.CONTAINER_0158, validator.getIssues().getIssues().size());
    }
    Pipeline pipeline = new Pipeline.Builder(
        stageLib,
        configuration,
        runtimeInfo,
        name + PRODUCTION_PIPELINE_SUFFIX,
        name,
        rev,
        userContext,
        pipelineConf,
        startTime,
        blobStoreTask,
        lineagePublisherTask,
        statsCollector,
        interceptorConfs,
        connections
    ).setObserver(observer).build(runner, runtimeParameters);

    SourceOffsetTracker sourceOffsetTracker;
    if (pipeline.getSource() instanceof OffsetCommitter) {
      sourceOffsetTracker = new ProductionSourceOffsetCommitterOffsetTracker(name, rev, runtimeInfo,
        (OffsetCommitter) pipeline.getSource());
    } else {
      sourceOffsetTracker = new ProductionSourceOffsetTracker(name, rev, runtimeInfo);
    }
    runner.setOffsetTracker(sourceOffsetTracker);
    runner.setPipelineStartTime(startTime);
    runner.setParameters(pipeline.getPipelineConfig().constants);
    return new ProductionPipeline(
        name,
        rev,
        pipelineConf,
        configuration,
        pipeline,
        pipeline.getPipelineConfig().shouldRetry
    );
  }

}
