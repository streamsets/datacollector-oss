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

import org.apache.hadoop.io.compress.AbstractTestCompressorDecompressor;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;

public class TestSnappyCompressorDecompressor extends AbstractTestCompressorDecompressor {

  @Override
  protected int getDefaultBufferSize() {
    return Constants.DEFAULT_BUFFER_SIZE;
  }

  @Override
  protected Compressor createCompressor() {
    return new SnappyCompressor();
  }

  @Override
  protected Compressor createCompressor(int bufferSize) {
    return new SnappyCompressor(bufferSize);
  }

  @Override
  protected Decompressor createDecompressor() {
    return new SnappyDecompressor();
  }

  @Override
  protected Decompressor createDecompressor(int bufferSize) {
    return new SnappyDecompressor(bufferSize);
  }
}
