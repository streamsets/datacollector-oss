/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import com.streamsets.pipeline.lib.util.ExceptionUtils;

import java.io.IOException;
import java.io.Reader;
import java.nio.CharBuffer;

/**
 * Caps amount of data read to avoid OOM issues, max size should be 64K or more ot avoid issues with implicit
 * stream buffers by JDK and libraries.
 */
public class OverrunReader extends CountingReader {

  public static final String READ_LIMIT_SYS_PROP = "overrun.reader.read.limit";

  public static int getDefaultReadLimit() {
    return Integer.parseInt(System.getProperty(READ_LIMIT_SYS_PROP, "102400"));
  }

  private final int readLimit;
  private final boolean removeCtrlChars;
  private boolean enabled;

  public OverrunReader(Reader in, int readLimit, boolean overrunCheckEnabled, boolean removeCtrlChars) {
    super(in);
    this.removeCtrlChars = removeCtrlChars;
    this.readLimit = readLimit;
    setEnabled(overrunCheckEnabled);
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled && readLimit > 0;
    if (enabled) {
      resetCount();
    }
  }

  public boolean isEnabled() {
    return enabled;
  }

  @Override
  protected synchronized void afterRead(int n) {
    super.afterRead(n);
    if (isEnabled() && getCount() > readLimit) {
      ExceptionUtils.throwUndeclared(new OverrunException(Utils.format(
          "Reader exceeded the read limit '{}'", readLimit), getPos()));
    }
  }

  private char[] oneCharBuf = new char[1];

  @Override
  public int read() throws IOException {
    if (removeCtrlChars) {
      int r = read(oneCharBuf, 0, 1);
      while (r > -1 && r == 0) {
        r = read(oneCharBuf, 0, 1);
      }
      return (r == -1) ? -1 : oneCharBuf[0];
    } else {
      return super.read();
    }
  }

  @Override
  public int read(char[] buffer) throws IOException {
    if (removeCtrlChars) {
      return read(buffer, 0, buffer.length);
    } else {
      return super.read(buffer);
    }
  }

  @Override
  public int read(char[] buffer, int offset, int len) throws IOException {
    if (removeCtrlChars) {
      char[] internalBuffer = new char[len];
      int r = super.read(internalBuffer, 0, len);
      return (r == -1) ? -1 : removeControlChars(internalBuffer, r, buffer, offset);
    } else {
      return super.read(buffer, offset, len);
    }
  }

  @Override
  public int read(CharBuffer target) throws IOException {
    if (removeCtrlChars) {
      char[] buffer = new char[target.limit()];
      int r = read(buffer);
      if (r > 0) {
        target.put(buffer, 0, r);
      }
      return r;
    } else {
      return super.read(target);
    }
  }

  static int removeControlChars(char[] intBuffer, int len, char[] extBuffer, int offset) {
    int removed = 0;
    int extPos = 0;
    int pos = 0;

    int controlPos = findFirstControlIdx(intBuffer, pos, len);
    while (controlPos > -1) {
      int lenToCopy = controlPos - pos;
      if (lenToCopy > 0) {
        System.arraycopy(intBuffer, pos, extBuffer, offset + extPos, lenToCopy);
        extPos += lenToCopy;
      }
      pos = controlPos + 1;
      removed++;
      controlPos = findFirstControlIdx(intBuffer, pos, len);
    }
    int lenToCopy = len - pos;
    if (lenToCopy > 0) {
      System.arraycopy(intBuffer, pos, extBuffer, offset + extPos, lenToCopy);
    }
    return len - removed;
  }

  static int findFirstControlIdx(char[] buffer, int start, int bufferLen) {
    int pos = start;
    while (pos < bufferLen && !isControl(buffer[pos])) {
      pos++;
    }
    return (pos == bufferLen) ? -1 : pos;
  }

  static boolean isControl(char c) {
    return c == 127 || (c < 32 && c != '\t' && c !='\n' && c != '\r');
   }

}
