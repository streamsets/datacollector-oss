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
package com.streamsets.pipeline.stage.origin.remote;

import java.io.IOException;
import java.io.InputStream;

class SFTPRemoteFile extends RemoteFile {
  private final ChrootSFTPClient sftpClient;

  SFTPRemoteFile(String filePath, long lastModified, ChrootSFTPClient sftpClient) {
    super(filePath, lastModified);
    this.sftpClient = sftpClient;
  }

  @Override
  InputStream createInputStream() throws IOException {
    return sftpClient.open(getFilePath());
  }
}
