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

import com.streamsets.pipeline.api.impl.Utils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

public abstract class AbstractWrapperStream<T extends AutoCloseable> extends InputStream implements ReadableByteChannel {
  private final T stream;

  AbstractWrapperStream(T stream) {
    this.stream = stream;
  }

  private void checkState(Class<? extends AutoCloseable> baseClass) {
    Utils.checkState(
        baseClass.isAssignableFrom(stream.getClass()),
        Utils.format("Unsupported read method for : {}", stream.getClass().getName())
    );
  }

  //InputStream
  @Override
  public int read() throws IOException {
    checkState(InputStream.class);
    return  ((InputStream)stream).read();
  }

  //InputStream
  @Override
  public int read(byte[] b) throws IOException {
    checkState(InputStream.class);
    return ((InputStream)stream).read(b);
  }

  //InputStream
  @Override
  public int read(byte[] b, int offset, int len) throws IOException {
    checkState(InputStream.class);
    return  ((InputStream)stream).read(b, offset, len);
  }

  //InputStream
  @Override
  public int available() throws IOException {
    checkState(InputStream.class);
    return ((InputStream)stream).available();
  }

  //InputStream
  @Override
  public boolean markSupported() {
    checkState(InputStream.class);
    return ((InputStream)stream).markSupported();
  }

  //InputStream
  @Override
  public void mark(int readLimit) {
    checkState(InputStream.class);
    ((InputStream)stream).mark(readLimit);
  }

  //InputStream
  @Override
  public void reset() throws IOException {
    checkState(InputStream.class);
    ((InputStream)stream).reset();
  }

  //InputStream
  @Override
  public long skip(long n) throws IOException {
    checkState(InputStream.class);
    return ((InputStream)stream).skip(n);
  }

  //ReadableByteChannel
  @Override
  public int read(ByteBuffer dst) throws IOException {
    checkState(ReadableByteChannel.class);
    return  ((ReadableByteChannel)stream).read(dst);
  }

  //ReadableByteChannel
  @Override
  public boolean isOpen() {
    checkState(ReadableByteChannel.class);
    return ((ReadableByteChannel)stream).isOpen();
  }

  @Override
  public void close() throws IOException {
    try {
      stream.close();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
