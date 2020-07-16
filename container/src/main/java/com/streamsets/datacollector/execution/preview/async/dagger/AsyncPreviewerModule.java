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
package com.streamsets.datacollector.execution.preview.async.dagger;

import com.streamsets.datacollector.config.ConnectionConfiguration;
import com.streamsets.datacollector.event.dto.PipelineStartEvent;
import com.streamsets.datacollector.execution.Previewer;
import com.streamsets.datacollector.execution.PreviewerListener;
import com.streamsets.datacollector.execution.preview.async.AsyncPreviewer;
import com.streamsets.datacollector.execution.preview.sync.SyncPreviewer;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;

import dagger.Module;
import dagger.ObjectGraph;
import dagger.Provides;

import javax.inject.Named;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Provides instances of SyncPreviewer.
 */
@Module(injects = Previewer.class, library = true, complete = false)
public class AsyncPreviewerModule {

  private final String id;
  private final String user;
  private final String name;
  private final String rev;
  private final ObjectGraph objectGraph;
  private final PreviewerListener previewerListener;
  private final List<PipelineStartEvent.InterceptorConfiguration> interceptorConfs;
  private final Function<Object, Void> afterActionsFunction;
  private final Map<String, ConnectionConfiguration> connections;

  public AsyncPreviewerModule(
      String id,
      String user,
      String name,
      String rev,
      PreviewerListener previewerListener,
      ObjectGraph objectGraph,
      List<PipelineStartEvent.InterceptorConfiguration> interceptorConfs,
      Function<Object, Void> afterActionsFunction,
      Map<String, ConnectionConfiguration> connections
  ) {
    this.id = id;
    this.user = user;
    this.name = name;
    this.rev = rev;
    this.objectGraph = objectGraph;
    this.previewerListener = previewerListener;
    this.interceptorConfs = interceptorConfs;
    this.afterActionsFunction = afterActionsFunction;
    this.connections = connections;
  }


  @Provides
  public SyncPreviewer providePreviewer() {
    return new SyncPreviewer(
        id,
        user,
        name,
        rev,
        previewerListener,
        objectGraph,
        interceptorConfs,
        afterActionsFunction,
        connections
    );
  }

  @Provides
  public Previewer provideAsyncPreviewer(SyncPreviewer previewer,
                                         @Named("previewExecutor") SafeScheduledExecutorService asyncExecutor) {
    return new AsyncPreviewer(previewer, asyncExecutor);
  }
}
