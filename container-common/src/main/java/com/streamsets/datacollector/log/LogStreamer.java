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
package com.streamsets.datacollector.log;

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

  @Override
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
