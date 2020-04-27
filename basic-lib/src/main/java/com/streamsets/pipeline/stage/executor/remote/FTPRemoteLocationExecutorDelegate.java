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

import com.streamsets.pipeline.lib.remote.RemoteConfigBean;
import com.streamsets.pipeline.stage.destination.remote.FTPRemoteUploadTargetDelegate;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.NameScope;

import java.io.IOException;
import java.nio.file.Paths;

public class FTPRemoteLocationExecutorDelegate extends FTPRemoteUploadTargetDelegate implements  RemoteLocationExecutorDelegate {

  public FTPRemoteLocationExecutorDelegate(RemoteConfigBean remoteConfig) {
    super(remoteConfig);
  }

  @Override
  public void delete(String filePath) throws IOException {
    FileObject fileObject = resolveChild(filePath);
    fileObject.delete();
  }

  @Override
  public boolean move(String sourceFile, String targetDir, boolean overwriteFile) throws IOException {
    FileObject targetFolder = resolveChild(targetDir);
    targetFolder.createFolder();

    String targetFileName = StringUtils.appendIfMissing(targetFolder.getName().getPath(), "/")
        + StringUtils.substringAfterLast(sourceFile, "/");

    FileObject targetFile = targetFolder.resolveFile(targetFileName, NameScope.DESCENDENT);

    if (!overwriteFile && targetFile.exists()) {
      return false;
    }

    resolveChild(sourceFile).moveTo(targetFile);
    targetFile.close();


    return true;
  }
}
