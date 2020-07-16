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
package com.streamsets.datacollector.execution.manager;

import com.streamsets.datacollector.config.ConnectionConfiguration;
import com.streamsets.datacollector.event.dto.PipelineStartEvent;
import com.streamsets.datacollector.execution.Previewer;
import com.streamsets.datacollector.execution.PreviewerListener;

import dagger.ObjectGraph;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Implementation of this interface provides instances of Previewer.
 */
public interface PreviewerProvider {

  Previewer createPreviewer(
      String user,
      String name,
      String rev,
      PreviewerListener listener,
      ObjectGraph objectGraph,
      List<PipelineStartEvent.InterceptorConfiguration> interceptorConfs,
      Function<Object, Void> afterActionsFunction,
      boolean remote,
      Map<String, ConnectionConfiguration> connections
  );

}
