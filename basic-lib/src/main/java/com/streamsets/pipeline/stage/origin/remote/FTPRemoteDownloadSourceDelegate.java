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
import com.streamsets.pipeline.lib.remote.FTPRemoteConnector;
import com.streamsets.pipeline.lib.remote.FTPRemoteFile;
import com.streamsets.pipeline.lib.remote.RemoteFile;
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPCmd;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.provider.ftp.FTPClientWrapper;
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

class FTPRemoteDownloadSourceDelegate extends FTPRemoteConnector implements RemoteDownloadSourceDelegate {

  private static final Logger LOG = LoggerFactory.getLogger(FTPRemoteDownloadSourceDelegate.class);

  private RemoteDownloadConfigBean conf;
  private URI archiveURI;
  private FileSystemOptions archiveOptions;
  private Method getFtpClient;
  private boolean supportsMDTM;

  FTPRemoteDownloadSourceDelegate(RemoteDownloadConfigBean conf) {
    super(conf.remoteConfig);
    this.conf = conf;
  }

  @Override
  public void initAndConnect(
      List<Stage.ConfigIssue> issues, Source.Context context, URI remoteURI, String archiveDir
  ) {
    super.initAndConnect(issues, context, remoteURI, Groups.REMOTE, Groups.CREDENTIALS);

    if (issues.isEmpty()) {
      setupModTime();

      if (archiveDir != null) {
        archiveURI = UriBuilder.fromUri(remoteURI).replacePath(archiveDir).build();
        archiveOptions = (FileSystemOptions) options.clone();
        FtpFileSystemConfigBuilder.getInstance().setUserDirIsRoot(archiveOptions, conf.archiveDirUserDirIsRoot);
      }
    }
  }

  @Override
  public Offset createOffset(String file) throws IOException {
    FileObject fileObject = resolveChild(file);
    return new Offset(relativizeToRoot(fileObject.getName().getPath()), getModTime(fileObject), ZERO);
  }

  @Override
  public long populateMetadata(String remotePath, Map<String, Object> metadata) throws IOException {
    FileObject fileObject = resolveChild(remotePath);
    long size = fileObject.getContent().getSize();
    metadata.put(HeaderAttributeConstants.SIZE, size);
    metadata.put(HeaderAttributeConstants.LAST_MODIFIED_TIME, getModTime(fileObject));
    metadata.put(RemoteDownloadSource.CONTENT_TYPE, fileObject.getContent().getContentInfo().getContentType());
    metadata.put(RemoteDownloadSource.CONTENT_ENCODING, fileObject.getContent().getContentInfo().getContentEncoding());
    return size;
  }

  @Override
  public void queueFiles(FileQueueChecker fqc, NavigableSet<RemoteFile> fileQueue, FileFilter fileFilter)
      throws IOException {
    verifyAndReconnect();

    FileObject[] theFiles = remoteDir.getChildren();
    if (conf.processSubDirectories) {
      theFiles = ArrayUtils.addAll(theFiles, remoteDir.findFiles(fileFilter));
    }

    for (FileObject file : theFiles) {
      String path = relativizeToRoot(file.getName().getPath());
      LOG.debug("Checking {}", path);
      if (file.getType() != FileType.FILE) {
        LOG.trace("Skipping {} because it is not a file", path);
      } else if (!fileFilter.getRegex().matcher(file.getName().getBaseName()).matches()) {
        //check if base name matches - not full path.
        LOG.trace("Skipping {} because it does not match the regex", path);
      } else {
        RemoteFile tempFile = new FTPRemoteFile(path, getModTime(file), file);
        if (fqc.shouldQueue(tempFile)) {
          LOG.debug("Queuing file {} with modtime {}", tempFile.getFilePath(), tempFile.getLastModified());
          // If we are done with all files, the files with the final mtime might get re-ingested over and over.
          // So if it is the one of those, don't pull it in.
          fileQueue.add(tempFile);
        }
      }
    }
  }

  public void delete(String remotePath) throws IOException {
    FileObject fileObject = resolveChild(remotePath);
    fileObject.delete();
  }

  @Override
  public String archive(String fromPath) throws IOException {
    if (archiveURI == null) {
      throw new IOException("No archive directory defined - cannot archive");
    }

    String toPath = archiveURI.toString() + (fromPath.startsWith("/") ? fromPath.substring(1) : fromPath);

    FileObject toFile = VFS.getManager().resolveFile(toPath, archiveOptions);
    toFile.refresh();
    // Create the toPath's parent dir(s) if they don't exist
    toFile.getParent().createFolder();
    resolveChild(fromPath).moveTo(toFile);
    toFile.close();
    return toPath;
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
      getFtpClient = FTPClientWrapper.class.getDeclaredMethod("getFtpClient");
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
        if (conf.remoteConfig.userDirIsRoot && path.startsWith("/")) {
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
}
