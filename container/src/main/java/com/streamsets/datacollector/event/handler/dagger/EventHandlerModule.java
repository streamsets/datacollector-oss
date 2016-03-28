/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import javax.inject.Named;
import javax.inject.Singleton;

import com.streamsets.datacollector.event.EventClientImpl;
import com.streamsets.datacollector.event.handler.EventHandlerTask;
import com.streamsets.datacollector.event.handler.NoOpEventHandlerTask;
import com.streamsets.datacollector.event.handler.remote.RemoteDataCollector;
import com.streamsets.datacollector.event.handler.remote.RemoteEventHandlerTask;
import com.streamsets.datacollector.execution.executor.ExecutorModule;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.stagelibrary.StageLibraryModule;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;

import dagger.Module;
import dagger.Provides;

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
    Configuration conf,
    RemoteDataCollector remoteDataCollector,
    RuntimeInfo runtimeInfo,
    StageLibraryTask stageLibraryTask) {
    String targetURL = conf.get(RemoteEventHandlerTask.REMOTE_CONTROL_URL, null);
    EventHandlerTask eventHandlerTask;
    if (targetURL != null) {
      eventHandlerTask =
        new RemoteEventHandlerTask(remoteDataCollector, new EventClientImpl(targetURL), eventHandlerExecutor,
          stageLibraryTask, runtimeInfo, conf);
    } else {
      eventHandlerTask = new NoOpEventHandlerTask();
    }
    return eventHandlerTask;
  }
}
