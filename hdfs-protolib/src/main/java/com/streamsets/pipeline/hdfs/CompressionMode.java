/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.hdfs;

import com.streamsets.pipeline.api.StageException;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;

public enum CompressionMode {
  NONE(null),
  GZIP(GzipCodec.class),
  BZIP2(BZip2Codec.class),
  SNAPPY(SnappyCodec.class),

  ;

  private final Class<? extends CompressionCodec> codec;

  CompressionMode(Class<? extends CompressionCodec> codec) {
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
          throw new StageException(HdfsLibError.HDFS_0006, codecName);
        }
      } catch (Exception ex1) {
        throw new StageException(HdfsLibError.HDFS_0007, codecName, ex1.getMessage(), ex1);
      }
    }
  }


}
