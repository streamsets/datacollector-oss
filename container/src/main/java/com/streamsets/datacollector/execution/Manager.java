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
package com.streamsets.datacollector.execution;

import com.streamsets.datacollector.config.ConnectionConfiguration;
import com.streamsets.datacollector.event.dto.PipelineStartEvent;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.task.Task;
import com.streamsets.datacollector.util.PipelineException;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

// one per SDC
public interface Manager extends Task {
  //the Manager receives a PipelineStore and a PipelineState instance at <init> time
  //the Manager has a threadpool used exclusively for previews and runners
  //it should have configurations for maximum concurrent previews and maximum running pipelines and based on that
  //size the threadpool

  // creates a new previewer which is keep in a cache using its ID. if the pipeline is in a running state no
  // previewer is created.
  // the Previewer is cached for X amount of time after the preview finishes and discarded afterwards
  // (using a last-access cache). the previewer is given a PreviewerListener at <init> time which will be used
  // by the previewer to signal the PreviewOutput has been given back to the client and the Previewer could be
  // eagerly removed from the cache.
  Previewer createPreviewer(
      String user,
      String name,
      String rev,
      List<PipelineStartEvent.InterceptorConfiguration> interceptorConfs,
      Function<Object, Void> afterActionsFunction,
      boolean remote,
      Map<String, ConnectionConfiguration> connections
  ) throws PipelineException;

  // returns the previewer from the cache with the specified ID
  Previewer getPreviewer(String previewerId);

  // creates a runner for a given pipeline, the runner will have the current state of the pipeline.
  Runner getRunner(String name, String rev) throws PipelineException;

  List<PipelineState> getPipelines() throws PipelineException;

  PipelineState getPipelineState(String name, String rev) throws PipelineException;

  // returns if the pipeline is in a 'running' state (starting, stopping, running)
  boolean isPipelineActive(String name, String rev) throws PipelineException;

  boolean isRemotePipeline(String name, String rev) throws PipelineStoreException;

  void addStateEventListener(StateEventListener listener);
}
