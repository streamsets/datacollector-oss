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
package org.apache.hadoop.io.compress.lz4;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.apache.hadoop.io.compress.AbstractCompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Lz4Compressor extends AbstractCompressor {
  private final static Logger LOG = LoggerFactory.getLogger(Lz4Compressor.class);

  private final LZ4Compressor compressor;

  public Lz4Compressor() {
    this(Constants.DEFAULT_BUFFER_SIZE);
  }

  public Lz4Compressor(int bufferSize) {
    this(true, bufferSize, false);
  }

  public Lz4Compressor(boolean unsafe, int bufferSize, boolean useHC) {
    super(bufferSize);
    LZ4Factory factory = (unsafe) ? LZ4Factory.unsafeInstance() : LZ4Factory.safeInstance();
    compressor = (useHC) ? factory.highCompressor() : factory.fastCompressor();
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }

  @Override
  protected int compressBuffer(byte[] uncompressedBuf, int uncompressedBufLen, byte[] compressedBuf) {
    return compressor.compress(uncompressedBuf, 0, uncompressedBufLen, compressedBuf, 0);
  }

}
