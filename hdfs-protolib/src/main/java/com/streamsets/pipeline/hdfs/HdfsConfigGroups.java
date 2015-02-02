/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.hdfs;

import com.streamsets.pipeline.api.Label;

public enum HdfsConfigGroups implements Label {
  HADOOP_FS("Hadoop FS"),
  OUTPUT_FILES("Output Files"),
  LATE_RECORDS("Late Records"),
  CSV("CSV Data"),
  TSV("Tab Separated Data"),
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
