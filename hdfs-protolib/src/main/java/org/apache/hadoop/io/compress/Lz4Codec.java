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
import org.apache.hadoop.io.compress.lz4.Constants;
import org.apache.hadoop.io.compress.lz4.Lz4Compressor;
import org.apache.hadoop.io.compress.lz4.Lz4Decompressor;
import org.slf4j.LoggerFactory;


public class Lz4Codec extends AbstractCodec {

  static {
    LoggerFactory.getLogger(Lz4Codec.class).info("Using SDC Hadoop Lz4Codec");
  }

  private boolean unsafe;
  private boolean useHC;

  public Lz4Codec() {
    super(Lz4Compressor.class, Lz4Decompressor.class, ".lz4");
  }

  @Override
  protected void init(Configuration conf) {
    int bufferSize = conf.getInt(CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZ4_BUFFERSIZE_KEY,
                                 CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZ4_BUFFERSIZE_DEFAULT);
    setBufferSize(bufferSize);
    unsafe = conf.getBoolean(Constants.IO_COMPRESSION_CODEC_LZ4_UNSAFE_KEY,
                             Constants.IO_COMPRESSION_CODEC_LZ4_UNSAFE_DEFAULT);
    useHC = conf.getBoolean(CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZ4_USELZ4HC_KEY,
                            CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZ4_USELZ4HC_DEFAULT);
  }

  @Override
  protected int getCompressionOverhead() {
    return getBufferSize() / 255 + 16;
  }

  @Override
  public Compressor createCompressor() {
    return new Lz4Compressor(unsafe, getBufferSize(), useHC);
  }

  @Override
  public Decompressor createDecompressor() {
    return new Lz4Decompressor(unsafe, getBufferSize());
  }

  public static boolean isNativeCodeLoaded() {
    return false;
  }
}
