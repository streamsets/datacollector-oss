/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.hdfs.cluster;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum Groups implements Label {
  HADOOP_FS("Hadoop FS"),
  TEXT("Text"),
  JSON("JSON"),
  LOG("Log")
  ;

  private final String label;

  private Groups(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return this.label;
  }
}
