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
package org.apache.hadoop.io.compress.snappy;

import org.apache.hadoop.io.compress.AbstractDecompressor;
import org.iq80.snappy.Snappy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnappyDecompressor extends AbstractDecompressor {

  private final static Logger LOG = LoggerFactory.getLogger(SnappyDecompressor.class);

  public SnappyDecompressor() {
    this(Constants.DEFAULT_BUFFER_SIZE);
  }

  public SnappyDecompressor(int bufferSize) {
    super(bufferSize);
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }

  @Override
  protected int decompressBuffer(byte[] compressedBuf, int compressedBufLen, byte[] uncompressedBuf) {
    return Snappy.uncompress(compressedBuf, 0, compressedBufLen, uncompressedBuf, 0);
  }

}
