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

import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.io.Reader;

public class OverrunCustomDelimiterReader extends AbstractOverrunDelimitedReader {

  private final String customDelimiter;
  private final boolean includeDelimiterInTheText;

 //For Testing purposes
  @VisibleForTesting
  OverrunCustomDelimiterReader(Reader reader, int maxLine, int bufferSize, String customDelimiter, boolean includeDelimiterInTheText) {
    super(reader, maxLine, bufferSize);
    this.customDelimiter = customDelimiter;
    this.includeDelimiterInTheText = includeDelimiterInTheText;
  }

  public OverrunCustomDelimiterReader(Reader reader, int maxLine, String customDelimiter, boolean includeDelimiterInTheText) {
    super(reader, maxLine, 8192 * 2);
    this.customDelimiter = customDelimiter;
    this.includeDelimiterInTheText = includeDelimiterInTheText;
  }

  public OverrunCustomDelimiterReader(
      Reader reader,
      int maxLine,
      String customDelimiter,
      boolean includeDelimiterInTheText,
      char quoteChar,
      char escapeChar
  ) {
    super(reader, maxLine, 8192 * 2, quoteChar, escapeChar);
    this.customDelimiter = customDelimiter;
    this.includeDelimiterInTheText = includeDelimiterInTheText;
  }

  @Override
  public int readLine(StringBuilder s) throws IOException {
    int initialLen = s.length(), overrun = 0, delimiterIndexToBeMatched = 0, startChar;
    boolean quoted = false;

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
      int searchIdx = nextChar, nextPossibleMatch = -1;

      while (searchIdx < nChars) {
        c = cb[searchIdx];

        boolean escaped = escapeChar != null && searchIdx != 0 && cb[searchIdx - 1] == escapeChar;

        if (quoteChar != null && !escaped && c == quoteChar) {
          quoted = !quoted;
        }

        if (!quoted) {
          //Optimization for starting the search again.
          //In stream if we see a match for first character in delimiter, we can start the search from there,
          //if the current search for delimiter is not successful
          if (nextPossibleMatch == -1 && searchIdx != 0 && c == customDelimiter.charAt(0)) {
            nextPossibleMatch = searchIdx;
          }

          if (c == customDelimiter.charAt(delimiterIndexToBeMatched)) {
            delimiterIndexToBeMatched++;
          } else if (delimiterIndexToBeMatched > 0) {
            delimiterIndexToBeMatched = 0;
            if (nextPossibleMatch > 0) {
              searchIdx = nextPossibleMatch;
            }
            nextPossibleMatch = -1;
          }
        }

        searchIdx++;

        if (!quoted && delimiterIndexToBeMatched == customDelimiter.length()) {
          eol = true;
          delimiterIndexToBeMatched = 0;
          nextPossibleMatch = -1;
          break;
        }
      }

      startChar = nextChar;
      nextChar = searchIdx;

      if (eol) {
        //Strip off the delimiter if needed.
        overrun += copyToBuffer(s, initialLen, startChar, searchIdx);
        if (!includeDelimiterInTheText) {
          s.setLength(s.length() - customDelimiter.length());
        }
        return s.length() - initialLen + overrun;
      }
      overrun += copyToBuffer(s, initialLen, startChar, searchIdx);
    }
  }
}
