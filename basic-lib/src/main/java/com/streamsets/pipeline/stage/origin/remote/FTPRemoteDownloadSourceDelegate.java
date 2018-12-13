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
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPCmd;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.NameScope;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.auth.StaticUserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.ftp.FtpClient;
import org.apache.commons.vfs2.provider.ftp.FtpFileSystem;
import org.apache.commons.vfs2.provider.ftp.FtpFileSystemConfigBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.regex.Matcher;

class FTPRemoteDownloadSourceDelegate extends RemoteDownloadSourceDelegate {

  private static final Logger LOG = LoggerFactory.getLogger(FTPRemoteDownloadSourceDelegate.class);

  private URI remoteURI;
  private FileSystemOptions options;
  private FileObject remoteDir;
  private Method getFtpClient;
  private boolean supportsMDTM;

  FTPRemoteDownloadSourceDelegate(RemoteDownloadConfigBean conf) {
    super(conf);
  }

  protected void initAndConnectInternal(List<Stage.ConfigIssue> issues, Source.Context context, URI remoteURI) throws
      IOException {
    options = new FileSystemOptions();
    this.remoteURI = remoteURI;
    if (conf.strictHostChecking) {
      issues.add(
          context.createConfigIssue(Groups.CREDENTIALS.getLabel(),
          CONF_PREFIX + "strictHostChecking",
          Errors.REMOTE_12
      ));
    }
    switch (conf.auth) {
      case PRIVATE_KEY:
        issues.add(
            context.createConfigIssue(Groups.CREDENTIALS.getLabel(),
            CONF_PREFIX + "privateKey",
            Errors.REMOTE_11
        ));
        break;
      case PASSWORD:
        String username = resolveUsername(remoteURI, issues, context);
        String password = resolvePassword(remoteURI, issues, context);
        if (remoteURI.getUserInfo() != null) {
          remoteURI = UriBuilder.fromUri(remoteURI).userInfo(null).build();
        }
        StaticUserAuthenticator authenticator = new StaticUserAuthenticator(remoteURI.getHost(), username, password);
        DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(options, authenticator);
        break;
      case NONE:
        break;
      default:
        break;
    }
    FtpFileSystemConfigBuilder.getInstance().setPassiveMode(options, true);
    FtpFileSystemConfigBuilder.getInstance().setUserDirIsRoot(options, conf.userDirIsRoot);

    // Only actually try to connect and authenticate if there were no issues
    if (issues.isEmpty()) {
      LOG.info("Connecting to {}", remoteURI.toString());
      // To ensure we can connect, else we fail validation.
      remoteDir = VFS.getManager().resolveFile(remoteURI.toString(), options);
      // Ensure we can assess the remote directory...
      remoteDir.refresh();
      // throw away the results.
      remoteDir.getChildren();

      setupModTime();
    }
  }

  Offset createOffset(String file) throws IOException {
    FileObject fileObject = remoteDir.resolveFile(file, NameScope.DESCENDENT);
    Offset offset = new Offset(
        fileObject.getName().getPath(),
        getModTime(fileObject),
        ZERO
    );
    return offset;
  }

  long populateMetadata(String remotePath, Map<String, Object> metadata) throws IOException {
    FileObject fileObject = remoteDir.resolveFile(remotePath);
    long size = fileObject.getContent().getSize();
    metadata.put(HeaderAttributeConstants.SIZE, size);
    metadata.put(HeaderAttributeConstants.LAST_MODIFIED_TIME, getModTime(fileObject));
    metadata.put(RemoteDownloadSource.CONTENT_TYPE, fileObject.getContent().getContentInfo().getContentType());
    metadata.put(RemoteDownloadSource.CONTENT_ENCODING, fileObject.getContent().getContentInfo().getContentEncoding());
    return size;
  }

  void queueFiles(FileQueueChecker fqc, NavigableSet<RemoteFile> fileQueue, FileFilter fileFilter) throws
      IOException, StageException {
    boolean done = false;
    int retryCounter = 0;
    while (!done && retryCounter < MAX_RETRIES) {
      try {
        remoteDir.refresh();
        // A call to getChildren() and then refresh() is needed in order to properly refresh if files were updated
        // A possible bug in VFS?
        remoteDir.getChildren();
        remoteDir.refresh();
        done = true;
      } catch (FileSystemException fse) {
        // Refresh can fail due to session is down, a timeout, etc; so try getting a new connection
        if (retryCounter < MAX_RETRIES - 1) {
          LOG.info("Got FileSystemException when trying to refresh remote directory. '{}'", fse.getMessage(), fse);
          LOG.warn("Retrying connection to remote directory");
          remoteDir = VFS.getManager().resolveFile(remoteURI.toString(), options);
        } else {
          throw new StageException(Errors.REMOTE_18, fse.getMessage(), fse);
        }
      }
      retryCounter++;
    }

    FileObject[] theFiles = remoteDir.getChildren();
    if (conf.processSubDirectories) {
      theFiles = ArrayUtils.addAll(theFiles, remoteDir.findFiles(fileFilter));
    }

    for (FileObject file : theFiles) {
      LOG.debug("Checking {}", file.getName().getPath());
      if (file.getType() != FileType.FILE) {
        LOG.trace("Skipping {} because it is not a file", file.getName().getPath());
        continue;
      }

      //check if base name matches - not full path.
      Matcher matcher = fileFilter.getRegex().matcher(file.getName().getBaseName());
      if (!matcher.matches()) {
        LOG.trace("Skipping {} because it does not match the regex", file.getName().getPath());
        continue;
      }

      RemoteFile tempFile = new FTPRemoteFile(file.getName().getPath(), getModTime(file), file);
      if (fqc.shouldQueue(tempFile)) {
        LOG.debug("Queuing file {} with modtime {}", tempFile.getFilePath(), tempFile.getLastModified());
        // If we are done with all files, the files with the final mtime might get re-ingested over and over.
        // So if it is the one of those, don't pull it in.
        fileQueue.add(tempFile);
      }
    }
  }

  private void setupModTime() {
    // The FTP protocol's default way to list files gives very inaccurate/ambiguous/inconsistent timestamps (e.g. it's
    // common for many FTP servers to drop the HH:mm on files older than 6 months).  Some FTP servers support the
    // MDTM command, which returns an accurate/correct timestamp, but not all servers support it.  Here, we'll check if
    // MDTM is supported so we can use it later to get proper timestamps.  Unfortunately, VFS does not expose a nice way
    // to use MDTM or to even get to the underlying FTPClient (VFS-257).  We have to use reflection.
    supportsMDTM = false;
    FtpClient ftpClient = null;
    FtpFileSystem ftpFileSystem = (FtpFileSystem) remoteDir.getFileSystem();
    try {
      ftpClient = ftpFileSystem.getClient();
      getFtpClient = ftpClient.getClass().getDeclaredMethod("getFtpClient");
      getFtpClient.setAccessible(true);
      FTPClient rawFtpClient = (FTPClient) getFtpClient.invoke(ftpClient);
      rawFtpClient.features();
      supportsMDTM = rawFtpClient.getReplyString().contains(FTPCmd.MDTM.getCommand());
    } catch (Exception e) {
      LOG.trace("Ignoring Exception when determining MDTM support", e);
    } finally {
      if (ftpClient != null) {
        ftpFileSystem.putClient(ftpClient);
      }
    }
    LOG.info("Using MDTM for more accurate timestamps: {}", supportsMDTM);
  }

  private long getModTime(FileObject fileObject) throws FileSystemException {
    long modTime = fileObject.getContent().getLastModifiedTime();
    if (supportsMDTM) {
      FtpClient ftpClient = null;
      FtpFileSystem ftpFileSystem = (FtpFileSystem) remoteDir.getFileSystem();
      try {
        ftpClient = ftpFileSystem.getClient();
        FTPClient rawFtpClient = (FTPClient) getFtpClient.invoke(ftpClient);
        String path = fileObject.getName().getPath();
        if (conf.userDirIsRoot && path.startsWith("/")) {
          // Remove the leading slash to turn it into a proper relative path
          path = path.substring(1);
        }
        FTPFile ftpFile = rawFtpClient.mdtmFile(path);
        if (ftpFile != null) {
          modTime = ftpFile.getTimestamp().getTimeInMillis();
        }
      } catch (Exception e) {
        LOG.trace("Ignoring Exception from MDTM command and falling back to basic timestamp", e);
      } finally {
        if (ftpClient != null) {
          ftpFileSystem.putClient(ftpClient);
        }
      }
    }
    return modTime;
  }

  void close() throws IOException {
    if (remoteDir != null) {
      remoteDir.close();
      FileSystem fs = remoteDir.getFileSystem();
      remoteDir.getFileSystem().getFileSystemManager().closeFileSystem(fs);
    }
  }
}
