/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.prodmanager;

import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.Collections;
import java.util.Map;

public class PipelineState {
  private final String name;
  private final String rev;
  private final State state;
  private final String message;
  private final long lastStatusChange;
  private final String metrics;
  private final Map<String, Object> attributes;

  @SuppressWarnings("unchecked")
  public PipelineState(String name, String rev, State state, String message, long lastStatusChange,
                       String metrics, Map<String, Object> attributes) {
    this.name = name;
    this.rev = rev;
    this.state = state;
    this.message = message;
    this.lastStatusChange = lastStatusChange;
    this.metrics = metrics;
    this.attributes = (Map) ((attributes != null) ? ImmutableMap.copyOf(attributes) : Collections.emptyMap());
  }

  public String getRev() {
    return rev;
  }

  public State getState() {
    return this.state;
  }

  public String getMessage() {
    return this.message;
  }

  public long getLastStatusChange() {
    return lastStatusChange;
  }

  public String getName() {
    return name;
  }

  public String getMetrics() {
    return metrics;
  }

  public Map<String, Object> getAttributes() {
    return attributes;
  }

  @Override
  public String toString() {
    return Utils.format("PipelineState[name='{}' rev='{}' state='{}' message='{}' lastStatusChange='{}' attributes='{}']",
      getName(), getRev(), getState().name(), getMessage(), getLastStatusChange(), getAttributes());
  }
}
