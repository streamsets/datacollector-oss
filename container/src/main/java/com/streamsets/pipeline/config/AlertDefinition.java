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

public class AlertDefinition {

  private final String name;
  private final String label;
  private final String lane;
  private final String predicate;
  //The possible set of keys are
  //1. count
  //2. percentage and minVolume
  private final Map<String, String> threshold;
  private final boolean enabled;

  @JsonCreator
  public AlertDefinition(@JsonProperty("name")String name,
                         @JsonProperty("label")String label,
                         @JsonProperty("lane")String lane,
                         @JsonProperty("predicate")String predicate,
                         @JsonProperty("threshold")Map<String, String> threshold,
                         @JsonProperty("enabled")boolean enabled) {
    this.name = name;
    this.label = label;
    this.lane = lane;
    this.predicate = predicate;
    this.threshold = threshold;
    this.enabled = enabled;
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

  public Map<String, String> getThreshold() {
    return threshold;
  }

  public boolean isEnabled() {
    return enabled;
  }

  @Override
  public String toString() {
    return Utils.format("AlertDefinition[name='{}' label='{}' lane='{}' predicate='{}' enabled='{}']",
      getName(), getLabel(), getLane(), getPredicate(), isEnabled());
  }

}
