/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.pipeline.api.impl.Utils;

public class AlertDefinition {

  private final String id;
  private final String label;
  private final String lane;
  private final String predicate;
  private final ThresholdType thresholdType;
  private final String thresholdValue;
  private final long minVolume;
  private final boolean enabled;

  @JsonCreator
  public AlertDefinition(@JsonProperty("id")String id,
                         @JsonProperty("label")String label,
                         @JsonProperty("lane")String lane,
                         @JsonProperty("predicate")String predicate,
                         @JsonProperty("thresholdType") ThresholdType thresholdType,
                         @JsonProperty("thresholdValue") String thresholdValue,
                         @JsonProperty("minVolume") long minVolume,
                         @JsonProperty("enabled")boolean enabled) {
    this.id = id;
    this.label = label;
    this.lane = lane;
    this.predicate = predicate;
    this.thresholdType = thresholdType;
    this.thresholdValue = thresholdValue;
    this.minVolume = minVolume;
    this.enabled = enabled;
  }

  public String getId() {
    return id;
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

  public ThresholdType getThresholdType() {
    return thresholdType;
  }

  public String getThresholdValue() {
    return thresholdValue;
  }

  public long getMinVolume() {
    return minVolume;
  }

  public boolean isEnabled() {
    return enabled;
  }

  @Override
  public String toString() {
    return Utils.format("AlertDefinition[id='{}' label='{}' lane='{}' predicate='{}' enabled='{}']",
      getId(), getLabel(), getLane(), getPredicate(), isEnabled());
  }

}
