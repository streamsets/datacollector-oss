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
package com.streamsets.datacollector.runner.preview;

import com.streamsets.datacollector.blobstore.BlobStoreTask;
import com.streamsets.datacollector.config.ConnectionConfiguration;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.creation.PipelineBeanCreator;
import com.streamsets.datacollector.event.dto.PipelineStartEvent;
import com.streamsets.datacollector.lineage.LineagePublisherTask;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.runner.Pipeline;
import com.streamsets.datacollector.runner.PipelineRunner;
import com.streamsets.datacollector.runner.PipelineRuntimeException;
import com.streamsets.datacollector.runner.UserContext;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.usagestats.StatsCollector;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.ValidationUtil;
import com.streamsets.datacollector.validation.Issues;
import com.streamsets.datacollector.validation.PipelineConfigurationValidator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class PreviewPipelineBuilder {

  @SuppressWarnings("unchecked")
  private StageConfiguration createPlugStage(List<String> lanes) {
    StageConfiguration stageConf = new StageConfiguration(
      PreviewStageLibraryTask.NAME + UUID.randomUUID().toString(),
      PreviewStageLibraryTask.LIBRARY,
      PreviewStageLibraryTask.NAME,
      PreviewStageLibraryTask.VERSION,
      Collections.emptyList(),
      Collections.emptyMap(),
      Collections.emptyList(),
      lanes,
      Collections.emptyList(),
      Collections.emptyList()
    );
    stageConf.setSystemGenerated();
    return stageConf;
  }

  private final StageLibraryTask stageLib;
  private final BuildInfo buildInfo;
  private final Configuration configuration;
  private final RuntimeInfo runtimeInfo;
  private final String name;
  private final String rev;
  private PipelineConfiguration pipelineConf;
  private final String endStageInstanceName;
  private final BlobStoreTask blobStoreTask;
  private final LineagePublisherTask lineagePublisherTask;
  private final StatsCollector statsCollector;
  private final boolean testOrigin;
  private final List<PipelineStartEvent.InterceptorConfiguration> interceptorConfs;
  private Map<String, ConnectionConfiguration> connections;

  /**
   * Constructor
   *
   * @param stageLib Stage Library Task
   * @param name Name of pipeline
   * @param pipelineConf Pipeline Configuration
   * @param endStageInstanceName Optional parameter, if passed builder will generate a partial pipeline and
   *                             endStage is exclusive
   */
  public PreviewPipelineBuilder(
    StageLibraryTask stageLib,
    BuildInfo buildInfo,
    Configuration configuration,
    RuntimeInfo runtimeInfo,
    String name,
    String rev,
    PipelineConfiguration pipelineConf,
    String endStageInstanceName,
    BlobStoreTask blobStoreTask,
    LineagePublisherTask lineagePublisherTask,
    StatsCollector statsCollector,
    boolean testOrigin,
    List<PipelineStartEvent.InterceptorConfiguration> interceptorConfs,
    Map<String, ConnectionConfiguration> connections
  ) {
    this.stageLib = new PreviewStageLibraryTask(stageLib);
    this.buildInfo = buildInfo;
    this.configuration = configuration;
    this.runtimeInfo = runtimeInfo;
    this.name = name;
    this.rev = rev;
    this.pipelineConf = pipelineConf;
    this.endStageInstanceName = endStageInstanceName;
    this.blobStoreTask = blobStoreTask;
    this.lineagePublisherTask = lineagePublisherTask;
    this.statsCollector = statsCollector;
    this.testOrigin = testOrigin;
    this.interceptorConfs = interceptorConfs;
    this.connections = connections;
    PipelineBeanCreator.prepareForConnections(configuration, runtimeInfo);
  }

  public PreviewPipeline build(UserContext userContext, PipelineRunner runner) throws PipelineRuntimeException {
    if (testOrigin) {
      // Validate that the test origin can indeed be inserted & executed in this pipeline
      StageConfiguration testOrigin = pipelineConf.getTestOriginStage();
      StageDefinition testOriginDef = stageLib.getStage(testOrigin.getLibrary(), testOrigin.getStageName(), false);
      if(!pipelineConf.getStages().get(0).getEventLanes().isEmpty() && !testOriginDef.isProducingEvents()) {
        throw new PipelineRuntimeException(ContainerError.CONTAINER_0167, testOriginDef.getLabel());
      }

      // Replace origin with test origin
      StageConfiguration origin = pipelineConf.getStages().remove(0);
      testOrigin.setOutputLanes(origin.getOutputLanes());
      testOrigin.setEventLanes(origin.getEventLanes());
      pipelineConf.getStages().add(0, pipelineConf.getTestOriginStage());
    }

    if(endStageInstanceName != null && endStageInstanceName.trim().length() > 0) {
      List<StageConfiguration> stages = new ArrayList<>();
      Set<String> allowedOutputLanes = new HashSet<>();
      for(StageConfiguration stageConfiguration: pipelineConf.getStages()) {
        if (stageConfiguration.getInstanceName().equals(endStageInstanceName)) {
          allowedOutputLanes.addAll(stageConfiguration.getInputLanes());
          break;
        }
        stages.add(stageConfiguration);
      }

      ListIterator li = stages.listIterator(stages.size());

      while(li.hasPrevious()) {
        StageConfiguration stageConfiguration = (StageConfiguration) li.previous();
        boolean contains = false;
        List<String> outputLanes = stageConfiguration.getOutputLanes();
        for(String outputLane :outputLanes) {
          if(allowedOutputLanes.contains(outputLane)){
            contains = true;
            break;
          }
        }

        if(contains) {
          allowedOutputLanes.addAll(stageConfiguration.getInputLanes());
        } else {
          li.remove();
        }
      }

      pipelineConf.setStages(stages);
    }

    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(
        stageLib,
        buildInfo,
        name,
        pipelineConf,
        userContext.getUser(),
        connections
    );
    pipelineConf = validator.validate();
    if (!validator.getIssues().hasIssues() || validator.canPreview()) {
      List<String> openLanes = validator.getOpenLanes();
      if (!openLanes.isEmpty()) {
        pipelineConf.getStages().add(createPlugStage(openLanes));
      }
    } else {
      throw new PipelineRuntimeException(ContainerError.CONTAINER_0154, ValidationUtil.getFirstIssueAsString(name,
        validator.getIssues()));
    }
     Pipeline.Builder builder = new Pipeline.Builder(
       stageLib,
       configuration,
       runtimeInfo,
       name + ":preview",
       name,
       rev,
       userContext,
       pipelineConf,
       System.currentTimeMillis(),
       blobStoreTask,
       lineagePublisherTask,
       statsCollector,
       interceptorConfs,
       connections
     );
     Pipeline pipeline = builder.build(runner);
     if (pipeline != null) {
       return new PreviewPipeline(name, rev, pipeline, validator.getIssues());
     } else {
       Issues issues = new Issues(builder.getIssues());
       throw new PipelineRuntimeException(issues);
    }
  }

  public Map<String, ConnectionConfiguration> getConnections() {
    return connections;
  }

  public void setConnections(HashMap<String, ConnectionConfiguration> connections) {
    this.connections = connections;
  }
}
