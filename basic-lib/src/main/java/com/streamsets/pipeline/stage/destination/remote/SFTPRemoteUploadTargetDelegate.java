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
package com.streamsets.pipeline.stage.destination.remote;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.lib.remote.RemoteConfigBean;
import com.streamsets.pipeline.lib.remote.RemoteFile;
import com.streamsets.pipeline.lib.remote.SFTPRemoteConnector;
import com.streamsets.pipeline.lib.remote.SFTPRemoteFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.List;

public class SFTPRemoteUploadTargetDelegate extends SFTPRemoteConnector implements RemoteUploadTargetDelegate {

  private static final Logger LOG = LoggerFactory.getLogger(SFTPRemoteUploadTargetDelegate.class);

  public SFTPRemoteUploadTargetDelegate(RemoteConfigBean remoteConfig) {
    super(remoteConfig);
  }

  @Override
  public void initAndConnect(List<Stage.ConfigIssue> issues, Target.Context context, URI remoteURI) {
    super.initAndConnect(issues, context, remoteURI, Groups.REMOTE, Groups.CREDENTIALS);
  }

  @Override
  public RemoteFile getFile(String remotePath) throws IOException {
    return new SFTPRemoteFile(slashify(remotePath), 0, sftpClient);
  }
}
