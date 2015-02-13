/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.kafka;

import com.streamsets.pipeline.api.Label;

public enum PartitionStrategy implements Label {
  RANDOM("Random"),
  ROUND_ROBIN("Round Robin"),
  EXPRESSION("Expression"),

  ;

  private final String label;

  PartitionStrategy(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }


}
