/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.pipeline.api.impl.Utils;

public class SamplingDefinition {

  private final String name;
  private final String label;
  private final String lane;
  private final String predicate;
  private final String samplingPercentage;
  private final boolean enabled;

  @JsonCreator
  public SamplingDefinition(@JsonProperty("name") String name,
                            @JsonProperty("label") String label,
                            @JsonProperty("lane") String lane,
                            @JsonProperty("predicate") String predicate,
                            @JsonProperty("samplingPercentage") String samplingPercentage,
                            @JsonProperty("enabled") boolean enabled) {
    this.name = name;
    this.label = label;
    this.lane = lane;
    this.predicate = predicate;
    this.samplingPercentage = samplingPercentage;
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

  public String getSamplingPercentage() {
    return samplingPercentage;
  }

  public boolean isEnabled() {
    return enabled;
  }

  @Override
  public String toString() {
    return Utils.format(
      "SamplingDefinition[name='{}' label='{}' lane='{}' predicate='{}' samplingPercentage='{}', enabled='{}']",
      getName(), getLabel(), getLane(), getPredicate(), getSamplingPercentage(), isEnabled());
  }
}
