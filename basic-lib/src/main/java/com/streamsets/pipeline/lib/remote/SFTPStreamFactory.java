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
    return remoteFile.new ReadAheadRemoteFileInputStream(MAX_UNCONFIRMED_READ_WRITES) {
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
