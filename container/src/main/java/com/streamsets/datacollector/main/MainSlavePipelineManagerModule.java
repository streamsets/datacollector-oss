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
package com.streamsets.datacollector.main;

import com.streamsets.datacollector.activation.ActivationOverrideModule;
import com.streamsets.datacollector.aster.AsterModule;
import com.streamsets.datacollector.aster.EntitlementSyncModule;
import com.streamsets.datacollector.event.handler.dagger.EventHandlerModule;
import com.streamsets.datacollector.execution.Manager;
import com.streamsets.datacollector.execution.manager.slave.SlavePipelineManager;
import com.streamsets.datacollector.execution.manager.slave.dagger.SlavePipelineManagerModule;
import com.streamsets.datacollector.http.AsterContext;
import com.streamsets.datacollector.http.WebServerModule;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.task.Task;
import com.streamsets.datacollector.task.TaskWrapper;
import com.streamsets.datacollector.util.Configuration;
import dagger.Module;
import dagger.ObjectGraph;
import dagger.Provides;

import javax.inject.Singleton;

@Module(
    injects = {
      TaskWrapper.class,
      LogConfigurator.class,
      RuntimeInfo.class,
      BuildInfo.class,
      Configuration.class,
      PipelineStoreTask.class,
      AsterContext.class
    },
    library = true,
    complete = false
)
public class MainSlavePipelineManagerModule { //Need better name

  private final ObjectGraph objectGraph;

  // We cannot use the original AsterModule for testing as it does classloader tricks.
  public static MainSlavePipelineManagerModule createForTest(Object asterModule) {
    return new MainSlavePipelineManagerModule(asterModule);
  }

  public MainSlavePipelineManagerModule() {
    this(AsterModule.class);
  }

  private MainSlavePipelineManagerModule(Object asterModule) {
    ObjectGraph objectGraph = ObjectGraph.create(SlavePipelineManagerModule.class);
    Manager m = new SlavePipelineManager(objectGraph);
    // We add ActivationOverrideModule first to the list to ensure that we load the singleton Activation from the shared
    // graph instead of creating a new copy. This technique is necessary for any shared dependencies.
    this.objectGraph = objectGraph.plus(
            new ActivationOverrideModule(objectGraph),
            new WebServerModule(m),
            EventHandlerModule.class,
            EntitlementSyncModule.class,
            SlavePipelineTaskModule.class,
            asterModule
    );
  }

  @Provides @Singleton
  public Task providePipelineTask(SlavePipelineTask agent) {
    return agent;
  }

  @Provides @Singleton
  public SlavePipelineTask providePipelineTask() {
    return objectGraph.get(SlavePipelineTask.class);
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
