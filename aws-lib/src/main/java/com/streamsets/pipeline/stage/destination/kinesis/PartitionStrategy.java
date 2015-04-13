/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.kinesis;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum PartitionStrategy implements Label {
  ROUND_ROBIN("Round Robin"),
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



