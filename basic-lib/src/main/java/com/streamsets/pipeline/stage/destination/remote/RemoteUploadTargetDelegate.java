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
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.lib.remote.RemoteFile;

import java.io.IOException;
import java.net.URI;
import java.util.List;

/**
 * Encapsulates all unique behavior of a remote upload target so that {@link RemoteUploadTarget} doesn't have to
 * be cluttered with different cases depending on the remote target.  There are two implementations:
 * {@link SFTPRemoteUploadTargetDelegate} and {@link FTPRemoteUploadTargetDelegate} to handle SFTP and FTP,
 * respectively.
 *
 * {@link #initAndConnect(List, Target.Context, URI)} should be called once and only once, before calling
 * anything else. {@link #close()} should be called when finished to clean up.
 */
public interface RemoteUploadTargetDelegate {

  void initAndConnect(List<Stage.ConfigIssue> issues, Target.Context context, URI remoteURI);

  RemoteFile getFile(String remotePath) throws IOException;

  void verifyAndReconnect() throws StageException;

  void close() throws IOException;
}
