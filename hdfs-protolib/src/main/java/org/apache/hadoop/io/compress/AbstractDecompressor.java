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

import org.slf4j.Logger;

import java.io.IOException;

public abstract class AbstractDecompressor implements Decompressor {
  public static final String SDC= "SDC";

  private byte[] compressedBuf = null;
  private int compressedBufLen;

  private byte[] uncompressedBuf = null;
  private int uncompressedBufOff;
  private int uncompressedBufLen;

  private byte[] userBuf = null;
  private int userBufOff = 0;
  private int userBufLen = 0;

  private boolean finished;

  protected AbstractDecompressor(int bufferSize) {
    compressedBuf = new byte[bufferSize];
    uncompressedBuf = new byte[bufferSize];
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

    userBuf = b;
    userBufOff = off;
    userBufLen = len;

    setInputFromSavedData();

    // Reinitialize snappy's output direct-buffer
    uncompressedBufOff = 0;
    uncompressedBufLen = 0;

  }

  private void setInputFromSavedData() {
    getLogger().debug("SDC setInputFromSavedData()");

    // Reinitialize snappy's input direct buffer
    compressedBufLen = Math.min(userBufLen, compressedBuf.length);
    System.arraycopy(userBuf, userBufOff, compressedBuf, 0, compressedBufLen);

    // Note how much data is being fed to snappy
    userBufOff += compressedBufLen;
    userBufLen -= compressedBufLen;
  }

  @Override
  public boolean needsInput() {
    getLogger().debug("SDC needsInput()");

    // Consume remaining compressed data?
    if (uncompressedBufLen > 0) {
      return false;
    }

    // Check if snappy has consumed all input
    if (compressedBufLen <= 0) {
      // Check if we have consumed all user-input
      if (userBufLen <= 0) {
        return true;
      } else {
        setInputFromSavedData();
      }
    }

    return false;
  }

  @Override
  public void setDictionary(byte[] b, int off, int len) {
    //NOP
  }

  @Override
  public boolean needsDictionary() {
    return false;
  }

  @Override
  public boolean finished() {
    boolean b = (finished && uncompressedBufLen == 0);
    getLogger().debug("SDC finished(): {}", b);
    return b;
  }

  protected abstract int decompressBuffer(byte[] compressedBuf, int compressedBufLen, byte[] uncompressedBuf);

  @Override
  public int decompress(byte[] b, int off, int len) throws IOException {
    getLogger().debug("SDC decompress() len={}", len);

    if (b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }

    int n = 0;
    // Check if there is uncompressed data

    if (uncompressedBufLen > 0) {
      n = Math.min(uncompressedBufLen, len);
      System.arraycopy(uncompressedBuf, uncompressedBufOff, b, off, n);
      uncompressedBufOff += n;
      uncompressedBufLen -= n;
      return n;
    }

    if (compressedBufLen > 0) {
      // Re-initialize the snappy's output direct buffer
      uncompressedBufOff = 0;
      uncompressedBufLen = 0;

      // Decompress data
      n = decompressBuffer(compressedBuf, compressedBufLen, uncompressedBuf);
//      n = Snappy.uncompress(compressedBuf, 0, compressedBufLen, uncompressedBuf, 0);
      uncompressedBufLen = n;

      // the compressed buffer is always processed in full, thus we don't have a leftover
      compressedBufLen = 0;

      if (userBufLen <= 0) {
        finished = true;
      }

      // Get at most 'len' bytes
      n = Math.min(n, len);
      System.arraycopy(uncompressedBuf, 0, b, off, n);
      uncompressedBufOff += n;
      uncompressedBufLen -= n;
    }

    return n;
  }

  @Override
  public int getRemaining() {
    getLogger().debug("SDC getRemaining()");
    // Never use this function in BlockDecompressorStream.
    return 0;
  }

  @Override
  public void reset() {
    getLogger().debug("SDC reset()");
    finished = false;

    compressedBufLen = 0;

    userBufOff = 0;
    userBufLen = 0;

    uncompressedBufOff = 0;
    uncompressedBufLen = 0;
  }

  @Override
  public void end() {
    getLogger().debug("SDC end()");
    //NOP
  }

}
