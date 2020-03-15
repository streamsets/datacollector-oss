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
package com.streamsets.pipeline.lib.io;

import com.streamsets.pipeline.api.impl.Utils;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Copied from Google Guava for compatibility with versions <14.0
 * Added an addition capability to take action on when read is called after limit is reached
 *
 *
 * {@link https://github.com/google/guava/blob/master/guava/src/com/google/common/io/ByteStreams.java}
 */
public class LimitedInputStream extends FilterInputStream {

  private long left;
  private long mark = -1;

  public LimitedInputStream(InputStream in, long limit) {
    super(in);
    Utils.checkNotNull(in, "Input Stream cannot be null");
    Utils.checkArgument(limit >= 0, "limit must be non-negative");
    left = limit;
  }

  @Override
  public int available() throws IOException {
    return (int) Math.min(in.available(), left);
  }

  // it's okay to mark even if mark isn't supported, as reset won't work
  @Override
  public synchronized void mark(int readLimit) {
    in.mark(readLimit);
    mark = left;
  }

  @Override
  public int read() throws IOException {
    if (left == 0) {
      handleReadOnLimitExceeded();
      return -1;
    }

    int result = in.read();
    if (result != -1) {
      --left;
    }
    return result;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (left == 0) {
      handleReadOnLimitExceeded();
      return -1;
    }

    len = (int) Math.min(len, left);
    int result = in.read(b, off, len);
    if (result != -1) {
      left -= result;
    }
    return result;
  }

  @Override
  public synchronized void reset() throws IOException {
    if (!in.markSupported()) {
      throw new IOException("Mark not supported");
    }
    if (mark == -1) {
      throw new IOException("Mark not set");
    }

    in.reset();
    left = mark;
  }

  @Override
  public long skip(long n) throws IOException {
    n = Math.min(n, left);
    long skipped = in.skip(n);
    left -= skipped;
    return skipped;
  }

  /**
   * Determines the action to be taken on when {@link #read()} or {@link #read(byte[])} or {@link #read(byte[])}
   * is called after reaching the read limit from the {@link InputStream}
   * @throws IOException Exception to be thrown condition if method is overriden
   */
  protected void handleReadOnLimitExceeded() throws IOException {
    //NOOP by default
  }
}
