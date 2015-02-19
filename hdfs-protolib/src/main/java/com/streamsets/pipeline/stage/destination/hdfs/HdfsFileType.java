/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.hdfs;

import com.streamsets.pipeline.api.Label;

public enum HdfsFileType implements Label {
  TEXT("Text files"), SEQUENCE_FILE("Sequence files");

  private String label;
  HdfsFileType(String label) {
    this.label = label;
  }
  @Override
  public String getLabel() {
    return label;
  }

}
