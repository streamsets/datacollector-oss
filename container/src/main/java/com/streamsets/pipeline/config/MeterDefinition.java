/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.pipeline.api.impl.Utils;

public class MeterDefinition {

  private final String id;
  private final String label;
  private final String lane;
  private final String predicate;
  private final String meterGroup;
  private final boolean enabled;

  @JsonCreator
  public MeterDefinition(@JsonProperty("id") String id,
                         @JsonProperty("label") String label,
                         @JsonProperty("lane") String lane,
                         @JsonProperty("predicate") String predicate,
                         @JsonProperty("meterGroup") String meterGroup,
                         @JsonProperty("enabled") boolean enabled) {
    this.id = id;
    this.label = label;
    this.lane = lane;
    this.predicate = predicate;
    this.meterGroup = meterGroup;
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

  public String getMeterGroup() {
    return meterGroup;
  }

  public boolean isEnabled() {
    return enabled;
  }

  @Override
  public String toString() {
    return Utils.format(
      "MeterDefinition[id='{}' label='{}' lane='{}' predicate='{}' meterGroup='{}' isEnabled='{}']",
      getId(), getLabel(), getLane(), getPredicate(), getMeterGroup(), isEnabled());
  }
}