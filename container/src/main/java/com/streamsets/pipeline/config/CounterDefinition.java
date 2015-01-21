/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.Map;

public class CounterDefinition {

  private final String name;
  private final String label;
  private final String lane;
  private final String predicate;
  private final String counterGroup;
  private final boolean enabled;
  //The possible set of keys are time, count
  private final Map<String, String> decay;

  @JsonCreator
  public CounterDefinition(@JsonProperty("name") String name,
                           @JsonProperty("label") String label,
                           @JsonProperty("lane") String lane,
                           @JsonProperty("predicate") String predicate,
                           @JsonProperty("counterGroup") String counterGroup,
                           @JsonProperty("enabled") boolean enabled,
                           @JsonProperty("decay") Map<String, String> decay) {
    this.name = name;
    this.label = label;
    this.lane = lane;
    this.predicate = predicate;
    this.counterGroup = counterGroup;
    this.enabled = enabled;
    this.decay = decay;
  }

  public String getName() {
    return name;
  }

  public String getLabel() {
    return label;
  }

  public String getLane() {
    return lane;
  }

  public String getPredicate() {
    return predicate;
  }

  public String getCounterGroup() {
    return counterGroup;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public Map<String, String> getDecay() {
    return decay;
  }

  @Override
  public String toString() {
    return Utils.format(
      "CounterDefinition[name='{}' label='{}' lane='{}' predicate='{}' counterGroup='{}' isEnabled='{}']",
      getName(), getLabel(), getLane(), getPredicate(), getCounterGroup(), isEnabled());
  }
}
