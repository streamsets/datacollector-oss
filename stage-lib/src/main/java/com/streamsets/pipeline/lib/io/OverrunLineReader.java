/**
 * Licensed to the Apache Software Foundation (ASF) under one
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;

/**
 * BufferedReader that reads a line up to a maximum length and then discards the rest of line
 */
public class OverrunLineReader extends BufferedReader {
  private final CountingReader countingReader;
  private final int maxLine;
  private StringBuilder readLineSb;

  public OverrunLineReader(Reader reader, int bufferSize, int maxLine) {
    super(reader, bufferSize);
    countingReader = (reader instanceof CountingReader) ? (CountingReader) reader : null;
    this.maxLine = maxLine;
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
  public String readLine() throws IOException {
    if (readLineSb == null) {
      readLineSb = new StringBuilder(maxLine);
    }
    readLineSb.setLength(0);
    int charsRead = readLine(readLineSb);
    return (charsRead == -1) ? null : readLineSb.toString();
  }

  // returns the stream line length, the StringBuilder has up to maxLine chars
  public int readLine(StringBuilder sb) throws IOException {
    int c = read();
    int count = (c == -1) ? -1 : 0;
    while (c > -1 && count < maxLine && !checkEolAndAdjust(c)) {
      count++;
      sb.append((char) c);
      c = read();
    }
    if (count >= maxLine) {
      while (c > -1 && c != '\n' && c != '\r') {
        count++;
        c = read();
      }
      checkEolAndAdjust(c);
    }
    return count;
  }

  private boolean checkEolAndAdjust(int c) throws IOException {
    boolean eol = false;
    if (c == '\n') {
      eol = true;
    } else if (c == '\r') {
      eol = true;
      mark(1);
      c = read();
      if (c != '\n') {
        reset();
      }
    }
    return eol;
  }

}
