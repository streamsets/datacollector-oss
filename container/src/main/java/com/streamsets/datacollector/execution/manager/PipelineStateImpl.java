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

import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.HashMap;
import java.util.Map;

public class PipelineStateImpl implements PipelineState {
  private final String name;
  private final String rev;
  private final PipelineStatus status;
  private final String message;
  private final long timeStamp;
  private final Map<String, Object> attributes;
  private final String user;
  private final ExecutionMode executionMode;
  private final String metrics;
  private final int retryAttempt;
  private final long nextRetryTimeStamp;

  @SuppressWarnings("unchecked")
  public PipelineStateImpl(String user, String name, String rev, PipelineStatus status, String message, long timeStamp,
    Map<String, Object> attributes, ExecutionMode executionMode, String metrics, int retryAttempt, long nextRetryTimeStamp) {
    this.name = name;
    this.rev = rev;
    this.user = user;
    this.status = status;
    this.message = message;
    this.timeStamp = timeStamp;
    this.attributes = (Map) ((attributes != null) ? new HashMap<>(attributes) : new HashMap<>());
    this.executionMode = executionMode;
    this.metrics = metrics;
    this.retryAttempt = retryAttempt;
    this.nextRetryTimeStamp = nextRetryTimeStamp;
  }

  @Override
  public String getRev() {
    return rev;
  }

  @Override
  public PipelineStatus getStatus() {
    return this.status;
  }

  @Override
  public String getMessage() {
    return this.message;
  }

  @Override
  public long getTimeStamp() {
    return timeStamp;
  }

  @Override
  public String getPipelineId() {
    return name;
  }

  @Override
  public Map<String, Object> getAttributes() {
    return attributes;
  }

  @Override
  public String toString() {
    return Utils.format("PipelineState[name='{}' rev='{}' state='{}' message='{}' timeStamp='{}' attributes='{}']",
      getPipelineId(), getRev(), getStatus().name(), getMessage(), getTimeStamp(), getAttributes());
  }

  @Override
  public String getUser() {
    return user;
  }

  @Override
  public ExecutionMode getExecutionMode() {
    return executionMode;
  }

  @Override
  public String getMetrics() {
    return metrics;
  }

  @Override
  public int getRetryAttempt() {
    return retryAttempt;
  }

  @Override
  public long getNextRetryTimeStamp() {
    return nextRetryTimeStamp;
  }
}
