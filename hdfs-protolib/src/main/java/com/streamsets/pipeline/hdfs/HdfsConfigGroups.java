/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.hdfs;

import com.streamsets.pipeline.api.ConfigGroups;

public enum HdfsConfigGroups implements ConfigGroups.Groups {
  HADOOP_FS("Hadoop FS"),
  FILES("Files"),
  DATA("Data"),
  LATE_RECORDS("Late Records"),
  ;

  private final String label;
  HdfsConfigGroups(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }
}
