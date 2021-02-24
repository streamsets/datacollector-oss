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
package com.streamsets.datacollector.event.handler.dagger;

import com.streamsets.datacollector.event.handler.EventHandlerTask;
import com.streamsets.datacollector.event.handler.NoOpEventHandlerTask;
import com.streamsets.datacollector.event.handler.remote.ColonCompatibleRemoteDataCollector;
import com.streamsets.datacollector.event.handler.remote.PipelineIdEncodedRemoteDatacollector;
import com.streamsets.datacollector.event.handler.remote.RemoteDataCollector;
import com.streamsets.datacollector.event.handler.remote.RemoteEventHandlerTask;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import dagger.Module;
import dagger.Provides;

import javax.inject.Named;
import javax.inject.Singleton;

/**
 * Provides a singleton instance of EventHandlerTask.
 */
@Module(
  library = true,
  complete = false,
  injects = { EventHandlerTask.class, RemoteDataCollector.class })
public class EventHandlerModule {

  @Provides
  @Singleton
  public EventHandlerTask provideEventHandler(
      @Named("eventHandlerExecutor") SafeScheduledExecutorService eventHandlerExecutor,
      @Named("syncEventsHandlerExecutor") SafeScheduledExecutorService syncEventsHandlerExecutor,
      Configuration conf,
      RemoteDataCollector remoteDataCollector,
      BuildInfo buildInfo,
      RuntimeInfo runtimeInfo,
      StageLibraryTask stageLibraryTask
  ) {
    EventHandlerTask eventHandlerTask;
    boolean isDPMEnabled = runtimeInfo.isDPMEnabled();
    String applicationToken = runtimeInfo.getAppAuthToken();
    if (isDPMEnabled && applicationToken != null && applicationToken.trim()
        .length() > 0 && !runtimeInfo.isClusterSlave()) {
      eventHandlerTask = new RemoteEventHandlerTask(
          new PipelineIdEncodedRemoteDatacollector(new ColonCompatibleRemoteDataCollector(remoteDataCollector)),
          eventHandlerExecutor,
          syncEventsHandlerExecutor,
          stageLibraryTask,
          buildInfo,
          runtimeInfo,
          conf
      );
    } else {
      eventHandlerTask = new NoOpEventHandlerTask();
    }
    return eventHandlerTask;
  }
}
