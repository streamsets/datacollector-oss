/*
 * Copyright 2018 StreamSets Inc.
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

import org.apache.commons.vfs2.FileNotFoundException;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class FTPRemoteFile extends RemoteFile {
  private static final Logger LOG = LoggerFactory.getLogger(FTPRemoteFile.class);

  private final FileObject fileObject;
  private FileObject tempFileObject;

  public FTPRemoteFile(String filePath, long lastModified, FileObject fileObject) throws IOException {
    super(filePath, lastModified);
    this.fileObject = fileObject;
  }

  @Override
  public boolean exists() throws IOException {
    return fileObject.exists();
  }

  @Override
  public InputStream createInputStream() throws FileSystemException {
    // If a data connection has been unexpectedly closed by the client
    // it's possible (though we haven't found examples of it)
    // that an FTP server maintains the last command in progress.
    // For some FTP servers it means that no more control commands will be
    // accepted, thus it's possible that the client gets blocked here.
    // In case if a customer experiences any issues with the FTP connector
    // which gets stuck, we recommend to analyse TRACE log messages.
    // If there is no corresponding message saying that a stream
    // has been opened, it means the client hit the issue.
    // Unfortunately, there is no known workaround, our recommendation
    // would be to migrate to a different FTP server.
    // Please also notices, we do not know what FTP servers are broken
    // thus this possibility is theoretical and this kind of behaviour
    // of FTP servers (to maintain a command opened while losing a data
    // connection) is not expected at all.
    LOG.trace("Trying to open an input stream on {}", fileObject);
    InputStream inputStream = fileObject.getContent().getInputStream();
    LOG.trace("Opened an input stream on {}", fileObject);
    return inputStream;
  }

  @Override
  public OutputStream createOutputStream() throws IOException {
    tempFileObject = fileObject.getParent().resolveFile(TMP_FILE_PREFIX + fileObject.getName().getBaseName());
    return tempFileObject.getContent().getOutputStream();
  }

  @Override
  public void commitOutputStream() throws IOException {
    if (tempFileObject == null) {
      throw new IOException("Cannot commit " + getFilePath() + " - it must be written first");
    }
    tempFileObject.moveTo(fileObject);
  }

  @Override
  public boolean isReadable() throws IOException {
    InputStream ignored = null;
    try {
      ignored = createInputStream();
      LOG.trace("File is readable");
    } catch (final IOException e) {
      LOG.error("Error reading file", e);
      if (!(e instanceof FileNotFoundException)) {
        throw e;
      }
      // File is not found because its either deleted or we don't have permissions
    }
    if (ignored != null) {
      try {
        ignored.close();
      } catch (final IOException ex) {
        LOG.debug("Failed to close an input stream while checking if we can read a file {}", fileObject, ex);
        // We're ignoring the failure to close the input stream.
        // The servers we tested return an error code which causes an exception to be thrown and caught here.
        // Normally we would need to issue an ABOR command, unfortunately
        // 1) it's not exposed by the FTP client wrapper
        // 2) anyway the current implementation in the library we use doesn't send the command but
        // closes a connection to the FTP server.
        // We do not expect any negative effect here, since in reality the data connection is
        // closed, the error the client throws is because the server returns an error code
        // for the previous command, that is RETR. This is expected since we do not want to
        // download the whole file, we just wanted to see if we could read the content
        // This is the most reliable way to test access permissions.
        // The worst case scenario was already described: server losses a data connection
        // and doesn't finish (why wouldn't it?) the RETR command. In this case we wouldn't see any
        // exception here but the server would ignore any subsequent control commands and the client
        // may get stuck next time we try to open a data connection.
        // As explained above this is just a theoretical possibility which wasn't yet seen in live.
        // Since a workaround requires a lot of effort and it's not clear if any of the customers uses
        // incompatible FTP servers, it was decided not to overcomplicate the solution but only
        // to debug messages to help to identify if we hit the case. If that happens we will
        // get back to this issue to find another solutions.
      }
    }
    return ignored != null;
  }
}
