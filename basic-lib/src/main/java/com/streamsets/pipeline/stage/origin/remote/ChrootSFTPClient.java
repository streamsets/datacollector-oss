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

import net.schmizz.sshj.sftp.FileAttributes;
import net.schmizz.sshj.sftp.FileMode;
import net.schmizz.sshj.sftp.RemoteDirectory;
import net.schmizz.sshj.sftp.RemoteResourceFilter;
import net.schmizz.sshj.sftp.RemoteResourceInfo;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.sftp.SFTPException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * A wrapper around {@link SFTPClient} that acts as if the root of the remote filesystem is at the specified root.
 * In other words, to the outside world it's similar as if you did a 'chroot'.  This is useful for being compatible with
 * {@link org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder} and consistent with
 * {@link org.apache.commons.vfs2.provider.ftp.FtpFileSystemConfigBuilder}.
 */
class ChrootSFTPClient {

  private final String root;
  private SFTPClient sftpClient;
  private final String pathSeparator;

  /**
   * Wraps the provided {@link SFTPClient} at the given root.  The given root can either be an absolute path or a path
   * relative to the user's home directory.
   *
   * @param sftpClient The {@link SFTPClient} to wrap
   * @param root The root directory to use
   * @param rootRelativeToUserDir true if the given root is relative to the user's home dir, false if not
   * @throws IOException
   */
  public ChrootSFTPClient(SFTPClient sftpClient, String root, boolean rootRelativeToUserDir) throws IOException {
    this.sftpClient = sftpClient;
    this.pathSeparator = sftpClient.getSFTPEngine().getPathHelper().getPathSeparator();
    if (rootRelativeToUserDir) {
      root = sftpClient.canonicalize(".") + (root.startsWith(pathSeparator) ? root : pathSeparator + root);
    }
    if (sftpClient.statExistence(root) == null) {
      throw new SFTPException(root + ": does not exist");
    }
    this.root = root;
  }

  public void setSFTPClient(SFTPClient sftpClient) {
    this.sftpClient = sftpClient;
  }

  private String prependRoot(String path) {
    if (path.startsWith(pathSeparator) && !path.isEmpty()) {
      path = path.substring(1);
    }
    return sftpClient.getSFTPEngine().getPathHelper().adjustForParent(root, path);
  }

  private String removeRoot(String path) {
    return pathSeparator + Paths.get(root).relativize(Paths.get(path)).toString();
  }

  public List<SimplifiedRemoteResourceInfo> ls() throws IOException {
    return ls("/", null);
  }

  public List<SimplifiedRemoteResourceInfo> ls(String path) throws IOException {
    return ls(path, null);
  }

  public List<SimplifiedRemoteResourceInfo> ls(RemoteResourceFilter filter) throws IOException {
    return ls("/", filter);
  }

  public List<SimplifiedRemoteResourceInfo> ls(String path, RemoteResourceFilter filter) throws IOException {
    final RemoteDirectory dir = sftpClient.getSFTPEngine().openDir(prependRoot(path));
    try {
      List<RemoteResourceInfo> dirScan = dir.scan(filter);
      List<SimplifiedRemoteResourceInfo> results = new ArrayList<>(dirScan.size());
      for (RemoteResourceInfo remoteResourceInfo : dirScan) {
        // This is needed in order to remove the root from the paths (RemoteResourceInfo is unfortunately immutable)
        results.add(
            new SimplifiedRemoteResourceInfo(
                removeRoot(remoteResourceInfo.getPath()),
                remoteResourceInfo.getAttributes().getMtime(),
                remoteResourceInfo.getAttributes().getType())
        );
      }
      return results;
    } finally {
      dir.close();
    }
  }

  public InputStream open(String filename) throws IOException {
    return sftpClient.open(prependRoot(filename)).new RemoteFileInputStream();
  }

  public FileAttributes stat(String path) throws IOException {
    return sftpClient.stat(prependRoot(path));
  }

  public void close() throws IOException {
    sftpClient.close();
  }

  static class SimplifiedRemoteResourceInfo {
    private String path;
    private long modifiedTime;
    private FileMode.Type type;

    public SimplifiedRemoteResourceInfo(String path, long modifiedTime, FileMode.Type type) {
      this.path = path;
      this.modifiedTime = modifiedTime;
      this.type = type;
    }

    public String getPath() {
      return path;
    }

    public long getModifiedTime() {
      return modifiedTime;
    }

    public FileMode.Type getType() {
      return type;
    }
  }
}
