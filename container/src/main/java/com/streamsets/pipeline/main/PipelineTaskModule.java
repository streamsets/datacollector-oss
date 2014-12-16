/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.main;

import com.streamsets.pipeline.http.WebServerModule;
import com.streamsets.pipeline.stagelibrary.StageLibraryModule;
import com.streamsets.pipeline.store.PipelineStoreModule;
import com.streamsets.pipeline.task.Task;
import com.streamsets.pipeline.task.TaskWrapper;
import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;

@Module(injects = {TaskWrapper.class, LogConfigurator.class, BuildInfo.class, RuntimeInfo.class},
        includes = {RuntimeModule.class, WebServerModule.class, StageLibraryModule.class, PipelineStoreModule.class})
public class PipelineTaskModule {

  @Provides @Singleton
  public Task providePipelineTask(PipelineTask agent) {
    return agent;
  }

}
