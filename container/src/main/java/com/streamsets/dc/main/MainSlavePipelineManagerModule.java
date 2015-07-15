/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.main;

import com.streamsets.dc.execution.Manager;
import com.streamsets.dc.execution.manager.slave.SlavePipelineManager;
import com.streamsets.dc.execution.manager.slave.dagger.SlavePipelineManagerModule;
import com.streamsets.dc.http.WebServerModule;
import com.streamsets.pipeline.main.BuildInfo;
import com.streamsets.pipeline.main.LogConfigurator;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.task.Task;
import com.streamsets.pipeline.task.TaskWrapper;
import com.streamsets.pipeline.util.Configuration;

import dagger.Module;
import dagger.ObjectGraph;
import dagger.Provides;

import javax.inject.Singleton;

@Module(injects = { TaskWrapper.class, LogConfigurator.class, RuntimeInfo.class, BuildInfo.class, Configuration.class,
    PipelineStoreTask.class }, library = true, complete = false)
public class MainSlavePipelineManagerModule { //Need better name

  private final ObjectGraph objectGraph;

  public MainSlavePipelineManagerModule() {
    ObjectGraph objectGraph = ObjectGraph.create(SlavePipelineManagerModule.class);
    Manager m = new SlavePipelineManager(objectGraph);
    this.objectGraph = objectGraph.plus(new WebServerModule(m), PipelineTaskModule.class);
  }

  @Provides @Singleton
  public Task providePipelineTask(PipelineTask agent) {
    return agent;
  }

  @Provides @Singleton
  public PipelineTask providePipelineTask() {
    return objectGraph.get(PipelineTask.class);
  }

  @Provides @Singleton
  public RuntimeInfo provideRuntimeInfo() {
    return objectGraph.get(RuntimeInfo.class);
  }

  @Provides @Singleton
  public PipelineStoreTask providePipelineStoreTask() {
    return objectGraph.get(PipelineStoreTask.class);
  }

  @Provides @Singleton
  public BuildInfo provideBuildInfo() {
    return objectGraph.get(BuildInfo.class);
  }

  @Provides @Singleton
  public Configuration provideConfiguration() {
    return objectGraph.get(Configuration.class);
  }
}
