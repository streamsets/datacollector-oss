/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
