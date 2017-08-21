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
package com.streamsets.pipeline.lib.io.fileref;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * The Abstract Implementation of {@link AbstractWrapperStream} which can perform
 * pre/post operations during read
 * (For Ex: updating metrics, controlling rate limits) for bytes being read.<br>
 *
 * NOTE: Implementations of this class should implement
 * {@link #performPreReadOperation(int)} and {@link #performPostReadOperation(int)}
 * to perform any pre / post processing steps on stream read.
 * @param <T> Stream implementation of {@link AutoCloseable}
 */
abstract class AbstractPrePostReadOperationPerformingStream<T extends AutoCloseable> extends AbstractWrapperStream<T> {

  AbstractPrePostReadOperationPerformingStream(T stream) {
    super(stream);
  }

  @Override
  public int read() throws IOException {
    performPreReadOperation(1);
    int readByte = super.read();
    performPostReadOperation((readByte != -1)? 1 : 0);
    return readByte;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    performPreReadOperation(dst.remaining());
    int bytesRead = super.read(dst);
    performPostReadOperation(bytesRead);
    return bytesRead;
  }

  @Override
  public int read(byte[] b) throws IOException {
    performPreReadOperation(b.length);
    int bytesRead = super.read(b);
    performPostReadOperation(bytesRead);
    return bytesRead;
  }

  @Override
  public int read(byte[] b, int offset, int len) throws IOException {
    performPreReadOperation(len - offset);
    int bytesRead = super.read(b, offset, len);
    performPostReadOperation(bytesRead);
    return bytesRead;
  }

  protected abstract void performPreReadOperation(int bytesToBeRead);
  protected abstract void performPostReadOperation(int bytesRead);
}
