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

import org.apache.commons.io.input.ProxyReader;

import java.io.IOException;
import java.io.Reader;

/**
 * Reader that reads a line up to a maximum length and then discards the rest of line
 */
public class OverrunLineReader extends ProxyReader {
  private final CountingReader countingReader;
  private final int maxLine;

  public OverrunLineReader(Reader reader, int maxLine) {
    super(reader);
    countingReader = (reader instanceof CountingReader) ? (CountingReader) reader : null;
    this.maxLine = maxLine;
  }

  public long getPos() {
    return (countingReader == null) ? -1 : countingReader.getPos() - (nChars - nextChar);
  }

  public long getCount() {
    if (countingReader != null) {
      return countingReader.getCount();
    } else {
      throw new UnsupportedOperationException("Underlying reader does not implement Countable");
    }
  }

  public long resetCount() {
    if (countingReader != null) {
      return countingReader.resetCount();
    } else {
      throw new UnsupportedOperationException("Underlying reader does not implement Countable");
    }
  }

  // mimicking logic from Java BufferedReader to read lines


  private char cb[] = new char[8192 * 2];
  private int nChars, nextChar;

  private static final int INVALIDATED = -2;
  private static final int UNMARKED = -1;
  private int markedChar = UNMARKED;
  private int readAheadLimit = 0; /* Valid only when markedChar > 0 */

  /**
   * If the next character is a line feed, skip it
   */
  private boolean skipLF = false;

  /**
   * The skipLF flag when the mark was set
   */
  private boolean markedSkipLF = false;

  @Override
  public void mark(int readAheadLimit) throws IOException {
    if (readAheadLimit < 0) {
      throw new IllegalArgumentException("Read-ahead limit < 0");
    }
    this.readAheadLimit = readAheadLimit;
    markedChar = nextChar;
    markedSkipLF = skipLF;
  }

  @Override
  public void reset() throws IOException {
    if (markedChar < 0) {
      throw new IOException((markedChar == INVALIDATED) ? "Mark invalid" : "Stream not marked");
    }
    nextChar = markedChar;
    skipLF = markedSkipLF;
  }

  private void fill() throws IOException {
    int dst;
    if (markedChar <= UNMARKED) {
            /* No mark */
      dst = 0;
    } else {
            /* Marked */
      int delta = nextChar - markedChar;
      if (delta >= readAheadLimit) {
                /* Gone past read-ahead limit: Invalidate mark */
        markedChar = INVALIDATED;
        readAheadLimit = 0;
        dst = 0;
      } else {
        if (readAheadLimit <= cb.length) {
                    /* Shuffle in the current buffer */
          System.arraycopy(cb, markedChar, cb, 0, delta);
          markedChar = 0;
          dst = delta;
        } else {
                    /* Reallocate buffer to accommodate read-ahead limit */
          char ncb[] = new char[readAheadLimit];
          System.arraycopy(cb, markedChar, ncb, 0, delta);
          cb = ncb;
          markedChar = 0;
          dst = delta;
        }
        nextChar = nChars = delta;
      }
    }

    int n;
    do {
      int max = (maxLine == -1) ? cb.length - dst : Math.min(maxLine, cb.length - dst);
      n = read(cb, dst, max);
    } while (n == 0);
    if (n > 0) {
      nChars = dst + n;
      nextChar = dst;
    }
  }

  public int readLine(StringBuilder s) throws IOException {
    int initialLen = s.length();
    int overrun = 0;
    int startChar;

    boolean omitLF = skipLF;

    for (; ; ) {

      if (nextChar >= nChars) {
        fill();
      }
      if (nextChar >= nChars) { /* EOF */
        int read = s.length() - initialLen;
        if (read > 0) {
          return read + overrun;
        } else {
          return -1;
        }
      }
      boolean eol = false;
      char c = 0;
      int i;

      /* Skip a leftover '\n', if necessary */
      if (omitLF && (cb[nextChar] == '\n')) {
        nextChar++;
      }
      skipLF = false;
      omitLF = false;

      for (i = nextChar; i < nChars; i++) {
        c = cb[i];
        if ((c == '\n') || (c == '\r')) {
          eol = true;
          break;
        }
      }

      startChar = nextChar;
      nextChar = i;

      if (eol) {
        overrun += copyToBuffer(s, initialLen, startChar, i);
        nextChar++;
        if (c == '\r') {
          skipLF = true;
          if (nextChar >= nChars) {
            fill(); //we force filling a new buffer to see if we have a '\n' in it
          }
          if (nextChar < nChars && cb[nextChar] == '\n') {
            nextChar++;
            skipLF = false;
          }
        }
        int read = s.length() - initialLen + overrun;
        return read;
      }

      overrun += copyToBuffer(s, initialLen, startChar, i);
    }
  }

  // we use this to trim the output in case of overruns
  private int copyToBuffer(StringBuilder s, int initialLen, int startChar, int currentChar) {
    int overrun = 0;
    int currentSize = s.length() - initialLen;
    int readSize = currentChar - startChar;
    if (maxLine > -1 && currentSize + readSize > maxLine) {
      int adjustedReadSize = maxLine - currentSize;
      if (adjustedReadSize > 0) {
        s.append(cb, startChar, adjustedReadSize);
        overrun = readSize - adjustedReadSize;
      } else {
        overrun = readSize;
      }
    } else {
      s.append(cb, startChar, readSize);
    }
    return overrun;
  }

}
