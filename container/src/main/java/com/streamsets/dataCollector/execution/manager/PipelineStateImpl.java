/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.manager;

import com.streamsets.dataCollector.execution.PipelineState;
import com.streamsets.dataCollector.execution.PipelineStatus;
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
  private ExecutionMode executionMode;

  @SuppressWarnings("unchecked")
  public PipelineStateImpl(String user, String name, String rev, PipelineStatus status, String message, long timeStamp,
                        Map<String, Object> attributes, ExecutionMode executionMode) {
    this.name = name;
    this.rev = rev;
    this.user = user;
    this.status = status;
    this.message = message;
    this.timeStamp = timeStamp;
    this.attributes = (Map) ((attributes != null) ? new HashMap<>(attributes) : new HashMap<>());
    this.executionMode = executionMode;
  }

  @Override
  public String getRev() {
    return rev;
  }

  @Override
  public PipelineStatus getState() {
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
  public String getName() {
    return name;
  }

  @Override
  public Map<String, Object> getAttributes() {
    return attributes;
  }

  @Override
  public String toString() {
    return Utils.format("PipelineState[name='{}' rev='{}' state='{}' message='{}' timeStamp='{}' attributes='{}']",
      getName(), getRev(), getState().name(), getMessage(), getTimeStamp(), getAttributes());
  }

  @Override
  public String getUser() {
    return user;
  }

  @Override
  public ExecutionMode getExecutionMode() {
    return executionMode;
  }
}
