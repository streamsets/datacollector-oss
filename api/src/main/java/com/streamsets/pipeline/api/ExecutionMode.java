/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api;

public enum ExecutionMode implements Label {
  STANDALONE("Standalone"),
  CLUSTER("Cluster"),
  ;

  private final String label;

  ExecutionMode(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }
}
