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
import com.streamsets.pipeline.lib.remote.RemoteFile;

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
public interface RemoteDownloadSourceDelegate {
  static final String ZERO = "0";

  void initAndConnect(
      List<Stage.ConfigIssue> issues, Source.Context context, URI remoteURI, String archiveDir
  );

  Offset createOffset(String file) throws IOException;

  long populateMetadata(String remotePath, Map<String, Object> metadata) throws IOException;

  void queueFiles(FileQueueChecker fqc, NavigableSet<RemoteFile> fileQueue, FileFilter fileFilter) throws
      IOException, StageException;

  void close() throws IOException;

  void delete(String remotePath) throws IOException;

  String archive(String fromPath) throws IOException;
}
