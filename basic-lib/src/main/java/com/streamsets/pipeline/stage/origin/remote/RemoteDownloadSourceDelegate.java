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

import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialValue;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;

/**
 * Encapsulates all unique behavior of a remote download source so that {@link RemoteDownloadSource} doesn't have to
 * be cluttered with different cases depending on the remote source.  There are two implementations:
 * {@link SFTPRemoteDownloadSourceDelegate} and {@link FTPRemoteDownloadSourceDelegate} to handle SFTP and FTP,
 * respectively.
 *
 * {@link #initAndConnect(List, Source.Context, URI, String)} should be called once and only once, before calling
 * anything else. {@link #close()} should be called when finished to clean up.
 */
abstract class RemoteDownloadSourceDelegate {
  protected static final String CONF_PREFIX = "conf.";
  protected static final String ZERO = "0";
  protected static final int MAX_RETRIES = 2;

  protected RemoteDownloadConfigBean conf;

  protected RemoteDownloadSourceDelegate(RemoteDownloadConfigBean conf) {
    this.conf = conf;
  }

  protected String resolveCredential(CredentialValue credentialValue, String config, List<Stage.ConfigIssue> issues,
      Source.Context context) {
    try {
      return credentialValue.get();
    } catch (StageException e) {
      issues.add(context.createConfigIssue(
          Groups.CREDENTIALS.getLabel(),
          config,
          Errors.REMOTE_17,
          e.toString()
      ));
    }
    return null;
  }

  protected String resolveUsername(URI remoteURI, List<Stage.ConfigIssue> issues, Source.Context context) {
    String userInfo = remoteURI.getUserInfo();
    if (userInfo != null) {
      if (userInfo.contains(":")) {
        return userInfo.substring(0, userInfo.indexOf(":"));
      }
      return userInfo;
    }
    return resolveCredential(conf.username, CONF_PREFIX + "username", issues, context);
  }

  protected String resolvePassword(URI remoteURI, List<Stage.ConfigIssue> issues, Source.Context context) {
    String userInfo = remoteURI.getUserInfo();
    if (userInfo != null && userInfo.contains(":")) {
      return userInfo.substring(userInfo.indexOf(":") + 1);
    }
    return resolveCredential(conf.password, CONF_PREFIX + "password", issues, context);
  }

  void initAndConnect(
      List<Stage.ConfigIssue> issues, Source.Context context, URI remoteURI, String archiveDir
  ) throws IOException {
    synchronized (this) {
      initAndConnectInternal(issues, context, remoteURI, archiveDir);
    }
  }

  protected abstract void initAndConnectInternal(
      List<Stage.ConfigIssue> issues, Source.Context context, URI remoteURI, String archiveDir
  ) throws IOException;

  abstract Offset createOffset(String file) throws IOException;

  abstract long populateMetadata(String remotePath, Map<String, Object> metadata) throws IOException;

  abstract void queueFiles(FileQueueChecker fqc, NavigableSet<RemoteFile> fileQueue, FileFilter fileFilter) throws
      IOException, StageException;

  abstract void close() throws IOException;

  abstract void delete(String remotePath) throws IOException;

  abstract String archive(String fromPath) throws IOException;
}
