/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.hdfs;

import com.streamsets.pipeline.api.Label;
import org.apache.hadoop.io.SequenceFile;

public enum HdfsSequenceFileCompressionType implements Label {
  BLOCK("Block Compression", SequenceFile.CompressionType.BLOCK),
  RECORD("Record Compression", SequenceFile.CompressionType.RECORD);

  private final String label;
  private final SequenceFile.CompressionType type;

  HdfsSequenceFileCompressionType(String label, SequenceFile.CompressionType type) {
    this.label = label;
    this.type = type;
  }
  @Override
  public String getLabel() {
    return label;
  }

  public SequenceFile.CompressionType getType() {
    return type;
  }

}
