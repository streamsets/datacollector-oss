/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

public class SamplingDefinition {

  private final String name;
  private final String label;
  private final String lane;
  private final String predicate;
  private final String samplingPercentage;
  private final boolean enabled;

  public SamplingDefinition(String name, String label, String lane, String predicate, String samplingPercentage, boolean enabled) {
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
}
