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

import java.io.IOException;
import java.io.Reader;

/**
 * Reader that reads a line up to a maximum length and then discards the rest of line
 */
public class OverrunLineReader extends AbstractOverrunDelimitedReader {

  //For Testing purposes
  OverrunLineReader(Reader reader, int maxLine, int bufferSize) {
    super(reader, maxLine, bufferSize);
  }

  public OverrunLineReader(Reader reader, int maxLine) {
    super(reader, maxLine, 8192 * 2);
  }

  public OverrunLineReader(Reader reader, int maxLine, char quoteChar, char escapeChar) {
    super(reader, maxLine, 8192 * 2, quoteChar, escapeChar);
  }

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
    super.mark(readAheadLimit);
    markedSkipLF = skipLF;
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    skipLF = markedSkipLF;
  }

  @Override
  public int readLine(StringBuilder s) throws IOException {
    int initialLen = s.length();
    int overrun = 0;
    int startChar;

    boolean omitLF = skipLF, quote = false;

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
      if (!quote && omitLF && (cb[nextChar] == '\n')) {
        nextChar++;
      }
      skipLF = false;
      omitLF = false;

      for (i = nextChar; i < nChars; i++) {
        c = cb[i];
        boolean escaped = escapeChar != null && i != 0 && cb[i - 1] == escapeChar;
        if (!escaped && quoteChar != null && c == quoteChar) {
          quote = !quote;
        }
        if (!quote && ((c == '\n') || (c == '\r'))) {
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

}
