/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.pipeline.lib.remote;

import net.schmizz.sshj.sftp.RemoteFile;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Provides methods for creating Input and Output Streams for {@link RemoteFile} which will close the file when the
 * Stream is closed.  SSHJ normally does not do this when closing the Stream so that you can continue to use the
 * {@link RemoteFile} for other tasks; but in our case, we we'll only be doing the one thing, and don't want to have
 * special handling later on to take care of closing the file.  This ensures that we don't leave anything open in the
 * SFTP Server.
 */
public class SFTPStreamFactory {

  private static final int MAX_UNCONFIRMED_READ_WRITES = 64;

  public static InputStream createReadAheadInputStream(RemoteFile remoteFile) {
    return createReadAheadInputStream(remoteFile, -1);
  }

  public static InputStream createReadAheadInputStream(RemoteFile remoteFile, int bufferSizeWholeFile) {

    return remoteFile.new ReadAheadRemoteFileInputStream(MAX_UNCONFIRMED_READ_WRITES) {

      private static final int DEFAULT_SIZE_READ_AHEAD = 80 * 1024;
      private int size = bufferSizeWholeFile;
      private byte[] data;
      private int currentPos;
      private int last;
      private boolean eof;

      private boolean isClosed = false;
      @Override
      public synchronized void close() throws IOException {
        if (!isClosed) {
          remoteFile.close();
          isClosed = true;
        }
      }

      @Override
      public int read(@NotNull byte[] b) throws IOException {
        return super.read(b, 0, b.length);
      }

      /**
       * Implementation of an intermediate buffer in order to allow the SFTP + S3 pipeline work with the
       * the Read Ahead Stream option. Because of the problem addressed on the SDC-13025 ticket we implemented
       * a solution that calls the read(byte[] into, int off, int len) always on a fixed size and using always the
       * buffer size property form the SFTP config. If this property is not set a DEFAULT_SIZE_READ_AHEAD is taken.
       *
       * {@inheritDoc}
       */
      @Override
      public int read(byte[] into, int off, int len) throws IOException {
        if (eof) {
          return -1;
        }
        if (data==null) {
          init();
        }
        int counter = 0;
        for (int i=0; i<len; i++) {
          if (currentPos == 0) {
            fillBuffer();
            if (eof) break;
          }
          into[i+off] = data[currentPos];
          currentPos = (currentPos+1) % last;
          counter++;
        }
        return counter == 0 ? -1 : counter;
      }

      public void fillBuffer() throws IOException {
        int last = super.read(data,0,size);
        if (last == -1) {
          eof=true;
        } else {
          this.last = last;
        }
      }

      private void init(){
        if (size <= 0) {
          size = DEFAULT_SIZE_READ_AHEAD;
        }
        data = new byte[size];
        currentPos = 0;
        eof = false;
      }

    };
  }

  public static InputStream createInputStream(RemoteFile remoteFile) {
    return remoteFile.new RemoteFileInputStream() {
      private boolean isClosed = false;
      @Override
      public synchronized void close() throws IOException {
        if (!isClosed) {
          remoteFile.close();
          isClosed = true;
        }
      }
    };
  }

  public static OutputStream createOutputStream(RemoteFile remoteFile) {
    return remoteFile.new RemoteFileOutputStream(0, MAX_UNCONFIRMED_READ_WRITES) {
      private boolean isClosed = false;
      @Override
      public synchronized void close() throws IOException {
        if (!isClosed) {
          remoteFile.close();
          isClosed = true;
        }
      }
    };
  }
}
