/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.io.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.compress.snappy.SnappyCompressor;
import org.apache.hadoop.io.compress.snappy.SnappyDecompressor;
import org.slf4j.LoggerFactory;

public class SnappyCodec extends AbstractCodec {

  static {
    LoggerFactory.getLogger(SnappyCodec.class).info("Using SDC Hadoop SnappyCodec");
  }


  public SnappyCodec() {
    super(SnappyCompressor.class, SnappyDecompressor.class, ".snappy");
  }

  @Override
  protected void init(Configuration conf) {
    int bufferSize = conf.getInt(CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_KEY,
                             CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_DEFAULT);
    setBufferSize(bufferSize);
  }

  @Override
  protected int getCompressionOverhead() {
    return getBufferSize() / 6 + 32;
  }

  @Override
  public Compressor createCompressor() {
    return new SnappyCompressor(getBufferSize());
  }

  @Override
  public Decompressor createDecompressor() {
    return new SnappyDecompressor(getBufferSize());
  }

  public static boolean isNativeCodeLoaded() {
    return false;
  }

}
