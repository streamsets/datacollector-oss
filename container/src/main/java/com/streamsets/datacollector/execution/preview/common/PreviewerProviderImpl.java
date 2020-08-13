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
package com.streamsets.datacollector.execution.preview.common;

import com.streamsets.datacollector.config.ConnectionConfiguration;
import com.streamsets.datacollector.event.dto.PipelineStartEvent;
import com.streamsets.datacollector.execution.Previewer;
import com.streamsets.datacollector.execution.PreviewerListener;
import com.streamsets.datacollector.execution.manager.PreviewerProvider;
import com.streamsets.datacollector.execution.preview.async.dagger.AsyncPreviewerModule;
import com.streamsets.datacollector.execution.preview.sync.dagger.SyncPreviewerInjectorModule;

import dagger.ObjectGraph;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

public class PreviewerProviderImpl implements PreviewerProvider {

  @Inject
  public PreviewerProviderImpl() {
  }

  @Override
  public Previewer createPreviewer(
      String user,
      String name,
      String rev,
      PreviewerListener listener,
      ObjectGraph objectGraph,
      List<PipelineStartEvent.InterceptorConfiguration> interceptorConfs,
      Function<Object, Void> afterActionsFunction,
      boolean remote,
      Map<String, ConnectionConfiguration> connections
  ) {

    objectGraph = objectGraph.plus(SyncPreviewerInjectorModule.class);
    objectGraph = objectGraph.plus(new AsyncPreviewerModule(
        UUID.randomUUID().toString(),
        user,
        name,
        rev,
        listener,
        objectGraph,
        interceptorConfs,
        afterActionsFunction,
        connections
    ));
    return objectGraph.get(Previewer.class);
  }
}
