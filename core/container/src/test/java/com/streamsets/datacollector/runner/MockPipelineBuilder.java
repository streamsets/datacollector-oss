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

import com.streamsets.datacollector.blobstore.BlobStoreTask;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.event.dto.PipelineStartEvent;
import com.streamsets.datacollector.lineage.LineagePublisherTask;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.util.Configuration;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;

/**
 * Pipeline.Builder test specific builder (yes builder of a builder) that will provide mock defaults and allow user to
 * override them if/when needed.
 */
public class MockPipelineBuilder {
  private StageLibraryTask stageLib;
  private Configuration configuration;
  private String name;
  private String pipelineName;
  private String rev;
  private UserContext userContext;
  private PipelineConfiguration pipelineConf;
  private long startTime;
  private BlobStoreTask blobStoreTask;
  private LineagePublisherTask lineagePublisherTask;
  private List<PipelineStartEvent.InterceptorConfiguration> interceptorConfs;
  private Observer observer;

  public MockPipelineBuilder() {
    this.stageLib = MockStages.createStageLibrary();
    this.configuration = new Configuration();
    this.name = "name";
    this.pipelineName = "myPipeline";
    this.rev = "0";
    this.userContext = MockStages.userContext();
    this.pipelineConf = MockStages.createPipelineConfigurationSourceTarget();
    this.startTime = System.currentTimeMillis();
    this.blobStoreTask = Mockito.mock(BlobStoreTask.class);
    this.lineagePublisherTask = Mockito.mock(LineagePublisherTask.class);
    this.interceptorConfs = Collections.emptyList();
    this.observer = null;
  }

  public MockPipelineBuilder withStageLib(StageLibraryTask stageLib) {
    this.stageLib = stageLib;
    return this;
  }

  public MockPipelineBuilder withConfiguration(Configuration configuration) {
    this.configuration = configuration;
    return this;
  }

  public MockPipelineBuilder withName(String name) {
    this.name = name;
    return this;
  }

  public MockPipelineBuilder withPipelineName(String name) {
    this.pipelineName = name;
    return this;
  }

  public MockPipelineBuilder withRev(String rev) {
    this.rev = rev;
    return this;
  }

  public MockPipelineBuilder withUserContext(UserContext userContext) {
    this.userContext = userContext;
    return this;
  }

  public MockPipelineBuilder withPipelineConf(PipelineConfiguration conf) {
    this.pipelineConf = conf;
    return this;
  }

  public MockPipelineBuilder withStartTime(long startTime) {
    this.startTime = startTime;
    return this;
  }

  public MockPipelineBuilder withBlobStoreTask(BlobStoreTask blobStoreTask) {
    this.blobStoreTask = blobStoreTask;
    return this;
  }

  public MockPipelineBuilder withLineagePublisherTask(LineagePublisherTask lineagePublisherTask) {
    this.lineagePublisherTask = lineagePublisherTask;
    return this;
  }

  public MockPipelineBuilder withObserver(Observer observer) {
    this.observer = observer;
    return this;
  }

  public MockPipelineBuilder withInterceptorConfigurations(List<PipelineStartEvent.InterceptorConfiguration> confs) {
    this.interceptorConfs = confs;
    return this;
  }

  public Pipeline.Builder build() {
    return new Pipeline.Builder(
      stageLib,
      configuration,
      name,
      pipelineName,
      rev,
      userContext,
      pipelineConf,
      startTime,
      blobStoreTask,
      lineagePublisherTask,
      interceptorConfs
    ).setObserver(observer);
  }

  public Pipeline build(PipelineRunner runner) throws PipelineRuntimeException {
    return build()      // Build pipeline builder
      .build(runner);   // And build final pipeline
  }
}
