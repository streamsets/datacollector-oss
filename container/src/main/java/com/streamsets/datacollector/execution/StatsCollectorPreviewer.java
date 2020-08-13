  /*
 * Copyright 2020 StreamSets Inc.
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
import com.streamsets.datacollector.usagestats.StatsCollector;
import com.streamsets.datacollector.util.PipelineException;

import javax.ws.rs.core.MultivaluedMap;
import java.util.List;
import java.util.Map;

public class StatsCollectorPreviewer implements Previewer {
  private final Previewer previewer;
  private final StatsCollector statsCollector;

  public StatsCollectorPreviewer(
      Previewer previewer,
      StatsCollector statsCollector
  ) {
    this.previewer = previewer;
    this.statsCollector = statsCollector;
  }

  @Override
  public String getId() {
    return previewer.getId();
  }

  @Override
  public String getName() {
    return previewer.getName();
  }

  @Override
  public String getRev() {
    return previewer.getRev();
  }

  @Override
  public Map<String, ConnectionConfiguration> getConnections() {
    return previewer.getConnections();
  }

  @Override
  public List<PipelineStartEvent.InterceptorConfiguration> getInterceptorConfs() {
    return previewer.getInterceptorConfs();
  }

  @Override
  public void validateConfigs(long timeoutMillis) throws PipelineException {
    previewer.validateConfigs(timeoutMillis);
  }

  @Override
  public RawPreview getRawSource(
      int maxLength,
      MultivaluedMap<String, String> previewParams
  ) throws PipelineException {
    return previewer.getRawSource(maxLength, previewParams);
  }

  @Override
  public void start(
      int batches,
      int batchSize,
      boolean skipTargets,
      boolean skipLifecycleEvents,
      String stopStage,
      List<StageOutput> stagesOverride,
      long timeoutMillis,
      boolean testOrigin
  ) throws PipelineException {
    statsCollector.previewPipeline(getName());
    previewer.start(
        batches,
        batchSize,
        skipTargets,
        skipLifecycleEvents,
        stopStage,
        stagesOverride,
        timeoutMillis,
        testOrigin
    );
  }

  @Override
  public void stop() {
    previewer.stop();
  }

  @Override
  public boolean waitForCompletion(long timeoutMillis) throws PipelineException {
    return previewer.waitForCompletion(timeoutMillis);
  }

  @Override
  public PreviewStatus getStatus() {
    return previewer.getStatus();
  }

  @Override
  public PreviewOutput getOutput() {
    return previewer.getOutput();
  }

  @Override
  public Map<String, Object> getAttributes() {
    return previewer.getAttributes();
  }

  @Override
  public Map<String, Object> updateCallbackInfo(CallbackInfo callbackInfo) {
    return previewer.updateCallbackInfo(callbackInfo);
  }

  @Override
  public void addStateEventListener(StateEventListener listener) {
    previewer.addStateEventListener(listener);
  }
}
