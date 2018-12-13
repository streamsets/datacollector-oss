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
import net.schmizz.sshj.DefaultConfig;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.FileAttributes;
import net.schmizz.sshj.sftp.FileMode;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;
import net.schmizz.sshj.userauth.keyprovider.KeyProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.FileNameMap;
import java.net.URI;
import java.net.URLConnection;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;

class SFTPRemoteDownloadSourceDelegate extends RemoteDownloadSourceDelegate {

  private static final Logger LOG = LoggerFactory.getLogger(SFTPRemoteDownloadSourceDelegate.class);

  private final File knownHostsFile;
  private SSHClient sshClient;
  private ChrootSFTPClient sftpClient;
  private SSHClientRebuilder sshClientRebuilder;

  SFTPRemoteDownloadSourceDelegate(RemoteDownloadConfigBean conf) {
    super(conf);
    if (conf.knownHosts != null && !conf.knownHosts.isEmpty()) {
      this.knownHostsFile = new File(conf.knownHosts);
    } else {
      this.knownHostsFile = null;
    }
  }

  protected void initAndConnectInternal(List<Stage.ConfigIssue> issues, Source.Context context, URI remoteURI) throws
      IOException {
    sshClientRebuilder = new SSHClientRebuilder();
    DefaultConfig sshConfig = new DefaultConfig();
    sshClient = new SSHClient(sshConfig);
    sshClientRebuilder.setConfig(sshConfig);

    if (conf.strictHostChecking) {
      if (knownHostsFile != null) {
        if (knownHostsFile.exists() && knownHostsFile.isFile() && knownHostsFile.canRead()) {
          try {
            sshClient.loadKnownHosts(knownHostsFile);
            sshClientRebuilder.setKnownHosts(knownHostsFile);
          } catch (IOException ex) {
            issues.add(
                context.createConfigIssue(Groups.CREDENTIALS.getLabel(),
                CONF_PREFIX + "knownHosts",
                Errors.REMOTE_06,
                knownHostsFile
            ));
          }
        } else {
          issues.add(
              context.createConfigIssue(Groups.CREDENTIALS.getLabel(),
              CONF_PREFIX + "knownHosts",
              Errors.REMOTE_06,
              knownHostsFile
          ));
        }
      } else {
        issues.add(
            context.createConfigIssue(Groups.CREDENTIALS.getLabel(),
            CONF_PREFIX + "strictHostChecking",
            Errors.REMOTE_07
        ));
      }
    } else {
      // Strict host checking off
      sshClient.addHostKeyVerifier(new PromiscuousVerifier());
    }

    String username = resolveUsername(remoteURI, issues, context);
    sshClientRebuilder.setUsername(username);
    KeyProvider keyProvider = null;
    String password = null;
    switch (conf.auth) {
      case PRIVATE_KEY:
        File privateKeyFile = new File(conf.privateKey);
        if (!privateKeyFile.exists() || !privateKeyFile.isFile() || !privateKeyFile.canRead()) {
          issues.add(
              context.createConfigIssue(Groups.CREDENTIALS.getLabel(),
              CONF_PREFIX + "privateKey",
              Errors.REMOTE_10,
              conf.privateKey
          ));
        } else {
          String privateKeyPassphrase = resolveCredential(
              conf.privateKeyPassphrase,
              CONF_PREFIX + "privateKeyPassphrase",
              issues, context
          );
          if (privateKeyPassphrase != null && !privateKeyPassphrase.isEmpty()) {
            keyProvider = sshClient.loadKeys(privateKeyFile.getAbsolutePath(), privateKeyPassphrase);
            sshClientRebuilder.setKeyPassphrase(privateKeyPassphrase);
            try {
              // Verify the passphrase works
              keyProvider.getPrivate();
            } catch (IOException e) {
              issues.add(
                  context.createConfigIssue(Groups.CREDENTIALS.getLabel(),
                  CONF_PREFIX + "privateKeyPassphrase",
                  Errors.REMOTE_19,
                  e.getMessage()
            ));
            }
          } else {
            keyProvider = sshClient.loadKeys(privateKeyFile.getAbsolutePath());
          }
          sshClientRebuilder.setKeyLocation(privateKeyFile.getAbsolutePath());
        }
        break;
      case PASSWORD:
        password = resolvePassword(remoteURI, issues, context);
        sshClientRebuilder.setPassword(password);
        break;
      default:
        break;
    }

    // Only actually try to connect and authenticate if there were no issues
    if (issues.isEmpty()) {
      LOG.info("Connecting to {}", remoteURI.toString());
      sshClient.connect(remoteURI.getHost(), remoteURI.getPort());
      sshClientRebuilder.setHostPort(remoteURI.getHost(), remoteURI.getPort());
      switch (conf.auth) {
        case PRIVATE_KEY:
          sshClient.authPublickey(username, keyProvider);
          break;
        case PASSWORD:
          sshClient.authPassword(username, password);
          break;
        default:
          break;
      }
      String remotePath = remoteURI.getPath().trim();
      if (remotePath.isEmpty()) {
        remotePath = "/";
      }
      sftpClient = new ChrootSFTPClient(sshClient.newSFTPClient(), remotePath, conf.userDirIsRoot);
      // To ensure we have access, else we fail validation.
      sftpClient.ls();
    }
  }

  Offset createOffset(String file) throws IOException {
    FileAttributes attributes = sftpClient.stat(file);
    Offset offset =
        // For backwards compatibility/consistency we need to add a leading /
        new Offset(file.startsWith("/") ? file : "/" + file,
        convertSecsToMillis(attributes.getMtime()),
        ZERO
    );
    return offset;
  }

  long populateMetadata(String remotePath, Map<String, Object> metadata) throws IOException {
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

  void queueFiles(FileQueueChecker fqc, NavigableSet<RemoteFile> fileQueue, FileFilter fileFilter) throws
      IOException, StageException {
    boolean done = false;
    int retryCounter = 0;
    while (!done && retryCounter < MAX_RETRIES) {
      try {
        sftpClient.ls();
        done = true;
      } catch (IOException ioe) {
        // ls can fail due to session is down, a timeout, etc; so try getting a new connection
        if (retryCounter < MAX_RETRIES - 1) {
          LOG.info("Got IOException when trying to ls remote directory. '{}'", ioe.getMessage(), ioe);
          LOG.warn("Retrying connection to remote directory");
          sshClient = sshClientRebuilder.build();
          sftpClient.setSFTPClient(sshClient.newSFTPClient());
        } else {
          throw new StageException(Errors.REMOTE_18, ioe.getMessage(), ioe);
        }
      }
      retryCounter++;
    }
    queueFiles(fqc, fileQueue, fileFilter, null);
  }

  void queueFiles(FileQueueChecker fqc, NavigableSet<RemoteFile> fileQueue, FileFilter fileFilter, String remoteDirPath)
      throws IOException {
    List<ChrootSFTPClient.SimplifiedRemoteResourceInfo> theFiles = (remoteDirPath == null)
      ? sftpClient.ls(fileFilter)
      : sftpClient.ls(remoteDirPath, fileFilter);

    for (ChrootSFTPClient.SimplifiedRemoteResourceInfo remoteFileInfo : theFiles) {
      LOG.debug("Checking {}", remoteFileInfo.getPath());
      if (conf.processSubDirectories && remoteFileInfo.getType() == FileMode.Type.DIRECTORY) {
        queueFiles(fqc, fileQueue, fileFilter, remoteFileInfo.getPath());
        continue;
      }

      if (remoteFileInfo.getType() != FileMode.Type.REGULAR) {
        LOG.trace("Skipping {} because it is not a file", remoteFileInfo.getPath());
        continue;
      }

      RemoteFile tempFile = new SFTPRemoteFile(
          remoteFileInfo.getPath(),
          convertSecsToMillis(remoteFileInfo.getModifiedTime()),
          sftpClient);
      if (fqc.shouldQueue(tempFile)) {
        LOG.debug("Queuing file {} with modtime {}", tempFile.getFilePath(), tempFile.getLastModified());
        // If we are done with all files, the files with the final mtime might get re-ingested over and over.
        // So if it is the one of those, don't pull it in.
        fileQueue.add(tempFile);
      }
    }
  }

  void close() throws IOException {
    try {
      if (sftpClient != null) {
        sftpClient.close();
      }
    } finally {
      if (sshClient != null) {
        sshClient.close();
      }
    }
  }

  private static long convertSecsToMillis(long secs) {
    return secs * 1000L;
  }
}
