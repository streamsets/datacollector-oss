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

import org.apache.hadoop.io.compress.AbstractCompressor;
import org.iq80.snappy.Snappy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnappyCompressor extends AbstractCompressor {
  private final static Logger LOG = LoggerFactory.getLogger(SnappyCompressor.class);

  public SnappyCompressor() {
    this(Constants.DEFAULT_BUFFER_SIZE);
  }

  public SnappyCompressor(int bufferSize) {
    super(bufferSize);
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }

  @Override
  protected int compressBuffer(byte[] uncompressedBuf, int uncompressedBufLen, byte[] compressedBuf) {
    return Snappy.compress(uncompressedBuf, 0, uncompressedBufLen, compressedBuf, 0);
  }

}
