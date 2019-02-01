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

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class FTPRemoteFile extends RemoteFile {
  private final FileObject fileObject;

  public FTPRemoteFile(String filePath, long lastModified, FileObject fileObject) {
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
    return fileObject.getContent().getOutputStream();
  }
}
