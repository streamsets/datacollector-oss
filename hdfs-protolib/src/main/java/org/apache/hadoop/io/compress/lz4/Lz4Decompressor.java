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

import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;
import org.apache.hadoop.io.compress.AbstractDecompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Lz4Decompressor extends AbstractDecompressor {

  private final static Logger LOG = LoggerFactory.getLogger(Lz4Decompressor.class);

  private final LZ4SafeDecompressor decompressor;

  public Lz4Decompressor() {
    this(Constants.DEFAULT_BUFFER_SIZE);
  }

  public Lz4Decompressor(int bufferSize) {
    this(true, bufferSize);
  }

  public Lz4Decompressor(boolean unsafe, int bufferSize) {
    super(bufferSize);
    LZ4Factory factory = (unsafe) ? LZ4Factory.unsafeInstance() : LZ4Factory.safeInstance();
    decompressor = factory.safeDecompressor();
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }

  @Override
  protected int decompressBuffer(byte[] compressedBuf, int compressedBufLen, byte[] uncompressedBuf){
        return decompressor.decompress(compressedBuf, 0, compressedBufLen, uncompressedBuf, 0);
  }

}
