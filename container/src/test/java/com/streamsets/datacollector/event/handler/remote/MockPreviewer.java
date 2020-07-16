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

package com.streamsets.datacollector.event.handler.remote;

import com.streamsets.datacollector.config.ConnectionConfiguration;
import com.streamsets.datacollector.event.dto.PipelineStartEvent;
import com.streamsets.datacollector.execution.PreviewOutput;
import com.streamsets.datacollector.execution.PreviewStatus;
import com.streamsets.datacollector.execution.Previewer;
import com.streamsets.datacollector.execution.RawPreview;
import com.streamsets.datacollector.execution.preview.common.PreviewOutputImpl;
import com.streamsets.datacollector.runner.StageOutput;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.datacollector.validation.Issues;

import javax.ws.rs.core.MultivaluedMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class MockPreviewer implements Previewer {

  private String user;
  private String name;
  private String rev;
  public static int validateConfigsCalled;
  public boolean isValid;
  private final List<PipelineStartEvent.InterceptorConfiguration> interceptorConfs;
  public boolean previewStarted;
  public boolean previewStopped;
  private final Function afterActionsFunction;
  private final Map<String, ConnectionConfiguration> connections;

  public MockPreviewer(
      String user,
      String name,
      String rev,
      List<PipelineStartEvent.InterceptorConfiguration> interceptorConfs,
      Function<Object, Void> afterActionsFunction
  ) {
    this.user = user;
    this.name = name;
    this.rev = rev;
    this.interceptorConfs = interceptorConfs;
    this.afterActionsFunction = afterActionsFunction;
    this.connections = new HashMap<>();
  }

  @Override
  public String getId() {
    return user + name + rev;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getRev() {
    // TODO Auto-generated method stub
    return rev;
  }

  @Override
  public List<PipelineStartEvent.InterceptorConfiguration> getInterceptorConfs() {
    return interceptorConfs;
  }

  @Override
  public void validateConfigs(long timeoutMillis) {
    if (name.equals("ns:name")) {
      isValid = true;
    } else {
      isValid = false;
    }
    validateConfigsCalled++;
  }

  @Override
  public RawPreview getRawSource(
      int maxLength,
      MultivaluedMap<String, String> previewParams
  ) {
    // TODO Auto-generated method stub
    return null;
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
  ) {
    previewStarted = true;
  }

  @Override
  @SuppressWarnings("ReturnValueIgnored") // needed for lambda call, which always returns null
  public void stop() {
    previewStopped = true;
    if (afterActionsFunction != null) {
      afterActionsFunction.apply(this);
    }
  }

  @Override
  public boolean waitForCompletion(long timeoutMillis) throws PipelineException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public PreviewStatus getStatus() {
    if (isValid) {
      return PreviewStatus.VALID;
    } else {
      return PreviewStatus.INVALID;
    }
  }

  @Override
  public PreviewOutput getOutput() {
    if (isValid) {
      return new PreviewOutputImpl(PreviewStatus.VALID, null, (List)null);
    } else {
      Issues issues = new Issues();
      return new PreviewOutputImpl(PreviewStatus.INVALID, issues, (List)null);
    }
  }

  @Override
  public Map<String, ConnectionConfiguration> getConnections() {
    return null;
  }
}
