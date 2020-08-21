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
import com.streamsets.pipeline.lib.remote.ChrootSFTPClient;
import com.streamsets.pipeline.lib.remote.RemoteFile;
import com.streamsets.pipeline.lib.remote.SFTPRemoteConnector;
import com.streamsets.pipeline.lib.remote.SFTPRemoteFile;
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;
import net.schmizz.sshj.sftp.FileAttributes;
import net.schmizz.sshj.sftp.FileMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.FileNameMap;
import java.net.URI;
import java.net.URLConnection;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;

class SFTPRemoteDownloadSourceDelegate extends SFTPRemoteConnector implements RemoteDownloadSourceDelegate {

  private static final Logger LOG = LoggerFactory.getLogger(SFTPRemoteDownloadSourceDelegate.class);

  private RemoteDownloadConfigBean conf;

  SFTPRemoteDownloadSourceDelegate(RemoteDownloadConfigBean conf) {
    super(conf.remoteConfig);
    this.conf = conf;
  }

  @Override
  public void initAndConnect(
      List<Stage.ConfigIssue> issues, Source.Context context, URI remoteURI, String archiveDir
  ) {
    super.initAndConnect(issues, context, remoteURI, Groups.REMOTE, Groups.CREDENTIALS);

    if (issues.isEmpty() && archiveDir != null) {
      try {
        sftpClient.setArchiveDir(archiveDir, conf.archiveDirUserDirIsRoot);
      } catch (IOException ioe) {
        issues.add(context.createConfigIssue(
            Groups.POST_PROCESSING.name(),
            CONF_PREFIX + "archiveDir",
            Errors.REMOTE_DOWNLOAD_08,
            ioe.getMessage(),
            ioe
        ));
      }
    }

    if (issues.isEmpty() && !remoteConfig.disableReadAheadStream) {
      int wholeFileMaxObjectLen = conf.dataFormatConfig.wholeFileMaxObjectLen;
      if (wholeFileMaxObjectLen>0) {
        sftpClient.setBufferSizeReadAheadStream(wholeFileMaxObjectLen);
      } else {
        sftpClient.setBufferSizeReadAheadStream(-1);
      }
    }
  }

  @Override
  public Offset createOffset(String file) throws IOException {
    FileAttributes attributes = sftpClient.stat(file);
    return new Offset(slashify(file), convertSecsToMillis(attributes.getMtime()), ZERO);
  }

  @Override
  public long populateMetadata(String remotePath, Map<String, Object> metadata) throws IOException {
    FileAttributes remoteAttributes = sftpClient.stat(remotePath);
    long size = remoteAttributes.getSize();
    metadata.put(HeaderAttributeConstants.SIZE, size);
    metadata.put(HeaderAttributeConstants.LAST_MODIFIED_TIME, convertSecsToMillis(remoteAttributes.getMtime()));
    metadata.put(RemoteDownloadSource.CONTENT_TYPE, determineContentType(remotePath));
    metadata.put(RemoteDownloadSource.CONTENT_ENCODING, null);  // VFS hardcodes this in FileContentInfoFilenameFactory
    return size;
  }

  private String determineContentType(String remotePath) {
    // This is based on VFS's FileContentInfoFilenameFactory and just looks at the filename extension
    String name = Paths.get(remotePath).getFileName().toString();
    FileNameMap fileNameMap = URLConnection.getFileNameMap();
    return fileNameMap.getContentTypeFor(name);
  }

  @Override
  public void queueFiles(FileQueueChecker fqc, NavigableSet<RemoteFile> fileQueue, FileFilter fileFilter)
      throws IOException {
    verifyAndReconnect();
    queueFiles(fqc, fileQueue, fileFilter, null);
  }

  private void queueFiles(
      FileQueueChecker queueChecker, NavigableSet<RemoteFile> fileQueue, FileFilter fileFilter, String remoteDirPath
  ) throws IOException {
    List<ChrootSFTPClient.SimplifiedRemoteResourceInfo> theFiles = remoteDirPath == null
                                                                   ? sftpClient.ls(fileFilter)
                                                                   : sftpClient.ls(remoteDirPath, fileFilter);

    for (ChrootSFTPClient.SimplifiedRemoteResourceInfo remoteFileInfo : theFiles) {
      LOG.debug("Checking {}", remoteFileInfo.getPath());
      if (conf.processSubDirectories && remoteFileInfo.getType() == FileMode.Type.DIRECTORY) {
        LOG.trace("Recursively looking for subdirectories under: {}", remoteFileInfo.getPath());
        queueFiles(queueChecker, fileQueue, fileFilter, remoteFileInfo.getPath());
      } else if (remoteFileInfo.getType() != FileMode.Type.REGULAR) {
        LOG.trace("Skipping {} because it is not a file", remoteFileInfo.getPath());
      } else {
        LOG.trace("Found candidate file to be queued: {}", remoteFileInfo.getPath());
        RemoteFile tempFile = new SFTPRemoteFile(slashify(remoteFileInfo.getPath()),
            convertSecsToMillis(remoteFileInfo.getModifiedTime()),
            sftpClient
        );
        if (queueChecker.shouldQueue(tempFile)) {
          LOG.debug("Queuing file {} with modtime {}", tempFile.getFilePath(), tempFile.getLastModified());
          // If we are done with all files, the files with the final mtime might get re-ingested over and over.
          // So if it is the one of those, don't pull it in.
          fileQueue.add(tempFile);
        }
      }
    }
  }

  @Override
  public void delete(String remotePath) throws IOException {
    sftpClient.delete(remotePath);
  }

  @Override
  public String archive(String fromPath) throws IOException {
    return sftpClient.archive(fromPath);
  }

  private static long convertSecsToMillis(long secs) {
    return secs * 1000L;
  }
}
