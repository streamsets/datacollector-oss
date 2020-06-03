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

import com.streamsets.pipeline.api.ext.io.CountingReader;
import org.apache.commons.io.input.ProxyReader;

import java.io.IOException;
import java.io.Reader;

public abstract class AbstractOverrunDelimitedReader extends ProxyReader {
  private static final int INVALIDATED = -2;
  private static final int UNMARKED = -1;

  private final CountingReader countingReader;

  private int markedChar = UNMARKED;
  private int readAheadLimit = 0; /* Valid only when markedChar > 0 */

  protected final int maxLine;

  protected char[] cb;
  protected int nChars;
  protected int nextChar;
  protected Character quoteChar;
  protected Character escapeChar;

  AbstractOverrunDelimitedReader(Reader reader, int maxLine, int bufferSize) {
    super(reader);
    this.countingReader = (reader instanceof CountingReader) ? (CountingReader) reader : null;
    this.maxLine = maxLine;
    this.cb = new char[bufferSize];
    this.quoteChar = null;
    this.escapeChar = null;
  }

  AbstractOverrunDelimitedReader(Reader reader, int maxLine, int bufferSize, char quoteChar, char escapeChar) {
    super(reader);
    this.countingReader = (reader instanceof CountingReader) ? (CountingReader) reader : null;
    this.maxLine = maxLine;
    this.cb = new char[bufferSize];
    this.quoteChar = quoteChar;
    this.escapeChar = escapeChar;
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


  @Override
  public void mark(int readAheadLimit) throws IOException {
    if (readAheadLimit < 0) {
      throw new IllegalArgumentException("Read-ahead limit < 0");
    }
    this.readAheadLimit = readAheadLimit;
    markedChar = nextChar;
  }

  @Override
  public void reset() throws IOException {
    if (markedChar < 0) {
      throw new IOException((markedChar == INVALIDATED) ? "Mark invalid" : "Stream not marked");
    }
    nextChar = markedChar;
  }

  protected void fill() throws IOException {
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
      n = read(cb, dst , max);
    } while (n == 0);
    if (n > 0) {
      nChars = dst + n;
      nextChar = dst;
    }
  }

  // we use this to trim the output in case of overruns
  protected int copyToBuffer(StringBuilder s, int initialLen, int startChar, int currentChar) {
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


  public abstract int readLine(StringBuilder s) throws IOException ;
}
