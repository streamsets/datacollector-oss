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
import org.slf4j.Logger;

import java.io.IOException;

public abstract class AbstractCompressor implements Compressor {
  public static final String SDC= "SDC";


  private byte[] uncompressedBuf;
  private int uncompressedBufLen;

  private byte[] userBuf;
  private int userBufOff;
  private int userBufLen;

  private byte[] compressedBuf;
  private int compressedBufOff;
  private int compressedBufLen;


  private boolean finish;
  private boolean finished;
  private long bytesRead;
  private long bytesWritten;

  protected AbstractCompressor(int bufferSize) {
    uncompressedBuf = new byte[bufferSize];
    compressedBuf = new byte[bufferSize];
    getLogger().debug("SDC <init> buffer={}", bufferSize);
  }

  protected abstract Logger getLogger();

  @Override
  public void setInput(byte[] b, int off, int len) {
    getLogger().debug("SDC setInput() len={}", len);
    if (b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }
    finished = false;

    if (len > (uncompressedBuf.length - uncompressedBufLen)) {
      // save data; now !needsInput
      this.userBuf = b;
      this.userBufOff = off;
      this.userBufLen = len;
    } else {
      System.arraycopy(b, off, uncompressedBuf, uncompressedBufLen, len);
      uncompressedBufLen += len;
    }

    bytesRead += len;
  }

  @Override
  public boolean needsInput() {
    return !(compressedBufLen > 0 || (uncompressedBuf.length - uncompressedBufLen) == 0 || userBufLen > 0);
  }

  @Override
  public void setDictionary(byte[] b, int off, int len) {
    //nop
  }

  @Override
  public long getBytesRead() {
    return bytesRead;
  }

  @Override
  public long getBytesWritten() {
    return bytesWritten;
  }

  @Override
  public void finish() {
    getLogger().debug("SDC finish()");
    finish = true;
  }

  @Override
  public boolean finished() {
    boolean b = (finish && finished && compressedBufLen == 0);
    getLogger().debug("SDC finished(): {}", b);
    return b;
  }

  protected abstract int compressBuffer(byte[] uncompressedBuf, int uncompressedBufLen, byte[] compressedBuf);

  @Override
  public int compress(byte[] b, int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }

    // Check if there is compressed data
    int n = compressedBufLen;
    if (n > 0) {
      n = Math.min(n, len);
      System.arraycopy(compressedBuf, compressedBufOff, b, off, n);
      compressedBufOff += n;
      compressedBufLen -= n;
      bytesWritten += n;
      getLogger().debug("SDC compress() len={}", n);
      return n;
    }

    // reset compressed buffer
    compressedBufOff = 0;
    compressedBufLen = 0;


    if (uncompressedBufLen == 0) { // reinitialize uncompressed buffer
      if (userBufLen > 0) { // there is carry over from user buffer, inject it in the uncompressed buffer
        n = Math.min(userBufLen, uncompressedBuf.length);
        System.arraycopy(userBuf, userBufOff, uncompressedBuf, uncompressedBufLen, n);
        uncompressedBufLen += n;
        userBufOff += n;
        userBufLen -= n;
      }
      if (uncompressedBufLen == 0) { // Called without data; write nothing
        finished = true;
        getLogger().debug("SDC compress() len=0");
        return 0;
      }
    }

    // Compress data
    n = compressBuffer(uncompressedBuf, uncompressedBufLen, compressedBuf);
    compressedBufLen += n;

    // snappy consumes all buffer input
    uncompressedBufLen = 0;

    // Set 'finished' if snappy has consumed all user-data
    if (userBufLen == 0) {
      finished = true;
    }

    // Get at most 'len' bytes
    n = Math.min(compressedBufLen, len);
    System.arraycopy(compressedBuf, 0, b, off, n);
    compressedBufOff = n;
    compressedBufLen -= n;

    bytesWritten += n;

    getLogger().debug("SDC compress() len={}", n);
    return n;
  }

  private void clear() {
    finish = false;
    finished = false;

    uncompressedBufLen = 0;

    userBufOff = 0;
    userBufLen = 0;

    compressedBufOff = 0;
    compressedBufLen = 0;

    bytesRead = 0;
    bytesWritten = 0;
  }

  @Override
  public void reset() {
    getLogger().debug("SDC reset()");
    clear();
  }

  @Override
  public void end() {
    getLogger().debug("SDC end()");
    //NOP
  }

  @Override
  public void reinit(Configuration conf) {
    getLogger().debug("SDC reinit()");
    clear();
  }
}
