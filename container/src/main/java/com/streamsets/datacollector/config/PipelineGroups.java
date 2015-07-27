/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.config;

import com.streamsets.pipeline.api.Label;

public enum PipelineGroups implements Label {
  CONSTANTS("Constants"),
  BAD_RECORDS("Error Records"),
  CLUSTER("Cluster"),
  ;

  private final String label;

  PipelineGroups(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }
}
