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
package com.streamsets.pipeline.lib.remote;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Holder for necessary info about a remote file, as well as a way to create an {@link InputStream} and an
 * {@link OutputStream}. Subclasses should implement the abstract methods based on what the remote source is.
 */
public abstract class RemoteFile {
  private final String filePath;
  private final long lastModified;

  protected RemoteFile(String filePath, long lastModified) {
    this.filePath = filePath;
    this.lastModified = lastModified;
  }

  public String getFilePath() {
    return filePath;
  }

  public long getLastModified() {
    return lastModified;
  }

  public abstract boolean exists() throws IOException;

  public abstract InputStream createInputStream() throws IOException;

  public abstract OutputStream createOutputStream() throws IOException;
}
