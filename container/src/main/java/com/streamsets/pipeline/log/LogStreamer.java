/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.log;

import com.streamsets.pipeline.api.impl.Utils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;

public class LogStreamer implements Closeable {
  private RandomAccessFile raf;
  private long startingOffset;
  private long len;
  private final boolean atBeginningAlready;

  public LogStreamer(String log, long endingOffset, long len) throws IOException {
    atBeginningAlready = endingOffset == 0;
    if (!atBeginningAlready) {
      File logFile = new File(log);
      if (logFile.exists()) {
        raf = new RandomAccessFile(logFile, "r");
        endingOffset = (endingOffset != -1) ? endingOffset : raf.length();
        startingOffset = Math.max(0, endingOffset - len);
        this.len = (startingOffset == 0) ? endingOffset : endingOffset - startingOffset;
      } else {
        throw new IOException(Utils.format("Log file '{}' does not exist", logFile.getAbsolutePath()));
      }
    }
  }

  public long getNewEndingOffset() {
    return startingOffset;
  }

  public void close() throws IOException {
    if (raf != null) {
      raf.close();
    }
  }

  public void stream(OutputStream outputStream) throws IOException {
    if (!atBeginningAlready) {
      stream(startingOffset, len, outputStream);
    }
  }

  private void stream(long offset, long len, OutputStream outputStream) throws IOException {
    raf.seek(offset);
    boolean eof = false;
    byte[] buff = new byte[4096];
    while (!eof && len > 0) {
      int askedLen = (int) Math.min(buff.length, len);
      int readLen = raf.read(buff, 0, askedLen);
      if (readLen > 0) {
        outputStream.write(buff, 0, readLen);
        len -= readLen;
      }
      eof = readLen == -1;
    }
  }

}
