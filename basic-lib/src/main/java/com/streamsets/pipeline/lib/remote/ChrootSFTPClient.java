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
package com.streamsets.pipeline.lib.remote;

import net.schmizz.sshj.sftp.FileAttributes;
import net.schmizz.sshj.sftp.FileMode;
import net.schmizz.sshj.sftp.OpenMode;
import net.schmizz.sshj.sftp.RemoteDirectory;
import net.schmizz.sshj.sftp.RemoteFile;
import net.schmizz.sshj.sftp.RemoteResourceFilter;
import net.schmizz.sshj.sftp.RemoteResourceInfo;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.sftp.SFTPException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

/**
 * A wrapper around {@link SFTPClient} that acts as if the root of the remote filesystem is at the specified root.
 * In other words, to the outside world it's similar as if you did a 'chroot'.  This is useful for being compatible with
 * {@link org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder} and consistent with
 * {@link org.apache.commons.vfs2.provider.ftp.FtpFileSystemConfigBuilder}.  It also adds an archive functionality and
 * handles massaging some paths.
 */
public class ChrootSFTPClient {

  private final String root;
  private String archiveDir;
  private SFTPClient sftpClient;
  private final boolean disableReadAheadStream;

  private int bufferSizeReadAheadStream;

  /**
   * Wraps the provided {@link SFTPClient} at the given root.  The given root can either be an absolute path or a path
   * relative to the user's home directory.
   *
   * @param sftpClient The {@link SFTPClient} to wrap
   * @param root The root directory to use
   * @param rootRelativeToUserDir true if the given root is relative to the user's home dir, false if not
   * @param makeRoot will create the root dir if true and it doesn't already exist
   * @param disableReadAheadStream disables the use of
   *   the {@link net.schmizz.sshj.sftp.RemoteFile.ReadAheadRemoteFileInputStream} class when opening files for reading,
   *   since there appears to be an issue with that class, and large files, when using on conjunction with S3 at least
   *   (see https://github.com/hierynomus/sshj/issues/505).  If this is set to true, then the
   *   {@link net.schmizz.sshj.sftp.RemoteFile.RemoteFileInputStream} will be opened instead, which is far less
   *   performant, but does not seem to trigger the problem.
   *
   * @throws IOException
   */
  public ChrootSFTPClient(
      SFTPClient sftpClient,
      String root,
      boolean rootRelativeToUserDir,
      boolean makeRoot,
      boolean disableReadAheadStream
  ) throws
      IOException {
    this.sftpClient = sftpClient;
    if (rootRelativeToUserDir) {
      String userDir = sftpClient.canonicalize(".");
      root = Paths.get(userDir, root).toString();
    }
    if (sftpClient.statExistence(root) == null) {
      if (makeRoot) {
        sftpClient.mkdirs(root);
      } else {
        throw new SFTPException(root + " does not exist");
      }
    }
    this.root = root;
    this.disableReadAheadStream = disableReadAheadStream;
  }

  public void setSFTPClient(SFTPClient sftpClient) {
    this.sftpClient = sftpClient;
  }

  private String prependRoot(String path) {
    return Paths.get(root, path).toString();
  }

  private String prependArchiveDir(String path) {
    return Paths.get(archiveDir, path).toString();
  }

  private String removeRoot(String path) {
    return "/" + Paths.get(root).relativize(Paths.get(path)).toString();
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

  public InputStream openForReading(String path) throws IOException {
    RemoteFile remoteFile = sftpClient.open(prependRoot(path));
    if (disableReadAheadStream) {
      return SFTPStreamFactory.createInputStream(remoteFile);
    } else {
      return SFTPStreamFactory.createReadAheadInputStream(remoteFile, bufferSizeReadAheadStream);
    }
  }

  public OutputStream openForWriting(String path) throws IOException {
    String toPath = prependRoot(path);
    // Create the toPath's parent dir(s) if they don't exist
    String toDir = Paths.get(toPath).getParent().toString();
    sftpClient.mkdirs(toDir);
    RemoteFile remoteFile = sftpClient.open(
        toPath,
        EnumSet.of(OpenMode.WRITE, OpenMode.CREAT, OpenMode.TRUNC),
        FileAttributes.EMPTY
    );
    return SFTPStreamFactory.createOutputStream(remoteFile);
  }

  public FileAttributes stat(String path) throws IOException {
    return sftpClient.stat(prependRoot(path));
  }

  public boolean exists(String path) throws IOException {
    return sftpClient.statExistence(prependRoot(path)) != null;
  }

  public void delete(String path) throws IOException {
    sftpClient.rm(prependRoot(path));
  }

  public void setArchiveDir(String archiveDir, boolean archiveDirRelativeToUserDir) throws IOException {
    if (archiveDir != null) {
      if (archiveDirRelativeToUserDir) {
        String userDir = sftpClient.canonicalize(".");
        archiveDir = Paths.get(userDir, archiveDir).toString();
      }
    }
    this.archiveDir = archiveDir;
  }

  public String archive(String path) throws IOException {
    if (archiveDir == null) {
      throw new IOException("No archive directory defined - cannot archive");
    }
    String fromPath = prependRoot(path);
    String toPath = prependArchiveDir(path);
    renameInternal(fromPath, toPath, false);
    return toPath;
  }

  public void rename(String fromPath, String toPath) throws IOException {
    fromPath = prependRoot(fromPath);
    toPath = prependRoot(toPath);
    renameInternal(fromPath, toPath, true);
  }

  public boolean rename(String fromPath, String toPath, boolean overwriteFile) throws IOException {
    fromPath = prependRoot(fromPath);
    toPath = prependRoot(toPath);

    if (!overwriteFile && sftpClient.statExistence(toPath) != null) {
      return false;
    }
    renameInternal(fromPath, toPath, true);
    return true;
  }

  private void renameInternal(String fromPath, String toPath, boolean deleteExists) throws IOException {
    // Create the toPath's parent dir(s) if they don't exist
    String toDir = Paths.get(toPath).getParent().toString();
    sftpClient.mkdirs(toDir);
    // Delete the target if it already exists
    if (deleteExists && sftpClient.statExistence(toPath) != null) {
      sftpClient.rm(toPath);
    }
    sftpClient.rename(fromPath, toPath);
  }

  public void close() throws IOException {
    sftpClient.close();
  }

  public static class SimplifiedRemoteResourceInfo {
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

  public int getBufferSizeReadAheadStream() {
    return bufferSizeReadAheadStream;
  }

  public void setBufferSizeReadAheadStream(int bufferSizeReadAheadStream) {
    this.bufferSizeReadAheadStream = bufferSizeReadAheadStream;
  }
}
