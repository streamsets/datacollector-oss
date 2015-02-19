/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.hdfs;

import com.streamsets.pipeline.api.Label;
import com.streamsets.pipeline.api.StageException;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;

public enum CompressionMode implements Label {
  NONE("None", null),
  GZIP("Gzip", GzipCodec.class),
  BZIP2("Bzip2", BZip2Codec.class),
  SNAPPY("Snappy", SnappyCodec.class),

  ;

  private final String label;
  private final Class<? extends CompressionCodec> codec;

  CompressionMode(String label, Class<? extends CompressionCodec> codec) {
    this.label = label;
    this.codec = codec;
  }

  public  Class<? extends CompressionCodec> getCodec() {
    return codec;
  }


  @SuppressWarnings("unchecked")
  public static Class<? extends CompressionCodec> getCodec(String codecName) throws StageException {
    try {
      return CompressionMode.valueOf(codecName).getCodec();
    } catch (IllegalArgumentException ex) {
      try {
        Class klass = Thread.currentThread().getContextClassLoader().loadClass(codecName);
        if (CompressionCodec.class.isAssignableFrom(klass)) {
          return (Class<? extends CompressionCodec> ) klass;
        } else {
          throw new StageException(Errors.HADOOPFS_04, codecName);
        }
      } catch (Exception ex1) {
        throw new StageException(Errors.HADOOPFS_05, codecName, ex1.getMessage(), ex1);
      }
    }
  }


  @Override
  public String getLabel() {
    return label;
  }
}
