/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.pipeline.stage.executor.remote;

import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.remote.RemoteConfigBean;
import com.streamsets.pipeline.stage.destination.remote.SFTPRemoteUploadTargetDelegate;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;

public class SFTPRemoteLocationExecutorDelegate extends SFTPRemoteUploadTargetDelegate implements RemoteLocationExecutorDelegate {

  public SFTPRemoteLocationExecutorDelegate(RemoteConfigBean remoteConfig) {
    super(remoteConfig);
  }

  @Override
  public void delete(String filePath) throws IOException {
    sftpClient.delete(filePath);
  }

  @Override
  public boolean move(String sourceFile, String targetDir, boolean overwriteFile) throws IOException {
    // Make sure to prepend/append leading/trailing slashes to form the full target filename
    String targetFile = StringUtils.appendIfMissing(slashify(targetDir), "/")
        + StringUtils.substringAfterLast(sourceFile, "/");

    return sftpClient.rename(sourceFile, targetFile, overwriteFile);
  }
}
