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

import com.streamsets.pipeline.api.impl.Utils;
import net.schmizz.sshj.sftp.Response;
import net.schmizz.sshj.sftp.SFTPException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;

public class SFTPRemoteFile extends RemoteFile {
  private static final Logger LOG = LoggerFactory.getLogger(SFTPRemoteFile.class);

  private final ChrootSFTPClient sftpClient;
  private String tempFilePath;

  public SFTPRemoteFile(String filePath, long lastModified, ChrootSFTPClient sftpClient) {
    super(filePath, lastModified);
    this.sftpClient = sftpClient;
  }

  @Override
  public boolean exists() throws IOException {
    return sftpClient.exists(getFilePath());
  }

  @Override
  public InputStream createInputStream() throws IOException {
    return sftpClient.openForReading(getFilePath());
  }

  @Override
  public OutputStream createOutputStream() throws IOException {
    Path p = Paths.get(getFilePath());
    tempFilePath = p.resolveSibling(TMP_FILE_PREFIX + p.getFileName().toString()).toString();
    return sftpClient.openForWriting(tempFilePath);
  }

  @Override
  public void commitOutputStream() throws IOException {
    if (tempFilePath == null) {
      throw new IOException("Cannot commit " + getFilePath() + " - it must be written first");
    }
    sftpClient.rename(tempFilePath, getFilePath());
  }

  @Override
  public boolean isReadable() throws IOException {
    try (InputStream ignored = createInputStream()) {
      LOG.trace("File is readable");
      return true;
    } catch (IOException e) {
      if (e instanceof SFTPException) {
        SFTPException ex = (SFTPException)e;
        LOG.error(Utils.format("Error checking isReadable {}", ex.getStatusCode()), ex);
        if (ex.getStatusCode() == Response.StatusCode.PERMISSION_DENIED || ex.getStatusCode() == Response.StatusCode.NO_SUCH_FILE) {
          return false;
        }
      }
      throw e;
    }
  }
}
