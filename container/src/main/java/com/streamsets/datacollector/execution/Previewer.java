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

import com.streamsets.datacollector.callback.CallbackInfo;
import com.streamsets.datacollector.config.ConnectionConfiguration;
import com.streamsets.datacollector.event.dto.PipelineStartEvent;
import com.streamsets.datacollector.runner.StageOutput;
import com.streamsets.datacollector.util.PipelineException;

import javax.ws.rs.core.MultivaluedMap;
import java.util.EventListener;
import java.util.List;
import java.util.Map;

public interface Previewer {

  // a Previewer lives for duration of a preview pipeline operation (validateConfig or start)

  // we should have a implementation that has a synchronous start() (by the time it finishes the status is final)

  // we should have a proxy implementation that uses the synchronous one and calls start() within a separate thread
  // making it the start() asynchronous. The manager should cache this one.

  // Implementations receive a PreviewerListener at <init> time, the listener is owned by the Manager

  String getId();

  String getName();

  String getRev();

  Map<String, ConnectionConfiguration> getConnections();

  List<PipelineStartEvent.InterceptorConfiguration> getInterceptorConfs();

  void validateConfigs(long timeoutMillis) throws PipelineException;

  RawPreview getRawSource(int maxLength, MultivaluedMap<String, String> previewParams) throws PipelineException;

  // Start preview
  void start(
      int batches,
      int batchSize,
      boolean skipTargets,
      boolean skipLifecycleEvents,
      String stopStage,
      List<StageOutput> stagesOverride,
      long timeoutMillis,
      boolean testOrigin
  ) throws PipelineException;

  void stop();

  // in the case of the synchronous one the only acceptable value is -1 (wait until it finishes)
  // in the case of the asynchronous one acceptable values are 0 (dispatch and wait) and greater (dispatch and block for millis)
  boolean waitForCompletion(long timeoutMillis) throws PipelineException;

  PreviewStatus getStatus();

  PreviewOutput getOutput();

  default Map<String, Object> getAttributes() {
    return null;
  }

  default Map<String, Object> updateCallbackInfo(CallbackInfo callbackInfo) {
    return null;
  }

  default void addStateEventListener(StateEventListener listener) {

  }

  interface StateEventListener extends EventListener {

    void onPreviewerStateChange(PreviewStatus fromState, PreviewStatus toState);

  }

}
