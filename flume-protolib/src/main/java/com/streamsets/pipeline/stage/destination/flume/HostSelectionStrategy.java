/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.flume;

import com.streamsets.pipeline.api.Label;

public enum HostSelectionStrategy implements Label {
  ROUND_ROBIN("Round Robin"),
  RANDOM("Random"),

  ;

  private final String label;

  HostSelectionStrategy(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }


}