/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.cluster;

public enum ClusterPipelineStatus {
  RUNNING("RUNNING"),
  SUCCEEDED("SUCCEEDED"),
  FAILED("FAILED"),
  KILLED("KILLED");

  private final String state;

  ClusterPipelineStatus(String state) {
    this.state = state;
  }

  public String getState() {
    return state;
  }
}
