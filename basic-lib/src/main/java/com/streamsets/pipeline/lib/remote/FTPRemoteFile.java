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
    return fileObject.getContent().getInputStream();
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
    try (InputStream ignored = createInputStream()){
      LOG.trace("File is readable");
      return true;
    } catch (IOException e) {
      LOG.error("Error reading file", e);
      if (e instanceof FileNotFoundException) {
        // File is not found because its either deleted or don't have permissions
        return false;
      }
     throw e;
    }
  }
}
