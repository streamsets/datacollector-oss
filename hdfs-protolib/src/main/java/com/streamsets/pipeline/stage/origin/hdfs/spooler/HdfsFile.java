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
package com.streamsets.pipeline.stage.origin.hdfs.spooler;

import com.streamsets.pipeline.lib.dirspooler.WrappedFile;
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class HdfsFile implements WrappedFile {
  public static final String PERMISSIONS = "permissions";

  private Map<String, Object> customMetadata = null;
  private Path filePath;
  private FileSystem fs;

  public HdfsFile(FileSystem fs, Path filePath) {
    this.filePath = filePath;
    this.fs = fs;
  }

  public boolean isAbsolute() {
    return filePath.isAbsolute();
  }

  public String getFileName() {
    return filePath.getName();
  }

  public String getAbsolutePath() {
    if (filePath == null) {
      return "";
    }
    return filePath.toUri().getPath();
  }

  public String getParent() {
    if (filePath == null) {
      return "";
    }
    return filePath.getParent().toUri().getPath();
  }

  public long getSize() throws IOException {
    return fs.getFileStatus(filePath).getLen();
  }

  public InputStream getInputStream() throws IOException {
    return fs.open(filePath).getWrappedStream();
  }

  @SuppressWarnings("unchecked")
  public Map<String, Object> getFileMetadata() throws IOException {
    FileStatus file = fs.getFileStatus(filePath);
    Map<String, Object>  metadata = new HashMap<>();
    metadata.put(HeaderAttributeConstants.FILE_NAME, file.getPath().getName());
    metadata.put(HeaderAttributeConstants.FILE, file.getPath().toUri().getPath());
    metadata.put(HeaderAttributeConstants.LAST_MODIFIED_TIME, file.getModificationTime());
    metadata.put(HeaderAttributeConstants.LAST_ACCESS_TIME, file.getAccessTime());
    metadata.put(HeaderAttributeConstants.IS_DIRECTORY, file.isDirectory());
    metadata.put(HeaderAttributeConstants.IS_SYMBOLIC_LINK, file.isSymlink());
    metadata.put(HeaderAttributeConstants.SIZE, file.getLen());
    metadata.put(HeaderAttributeConstants.OWNER, file.getOwner());
    metadata.put(HeaderAttributeConstants.GROUP, file.getGroup());
    metadata.put(HeaderAttributeConstants.BLOCK_SIZE, file.getBlockSize());
    metadata.put(HeaderAttributeConstants.REPLICATION, file.getReplication());
    metadata.put(HeaderAttributeConstants.IS_ENCRYPTED, file.isEncrypted());

    FsPermission permission = file.getPermission();
    if (permission != null) {
      metadata.put(PERMISSIONS, permission.toString());
    }

    return metadata;
  }

  @Override
  public Map<String, Object> getCustomMetadata() {
    if (customMetadata == null) {
      customMetadata = new HashMap<>();
    }
    return customMetadata;
  }

  public String toString() {
    return getAbsolutePath();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HdfsFile hdfsFile = (HdfsFile) o;
    return Objects.equals(filePath, hdfsFile.filePath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(filePath);
  }

  @Override
  public boolean canRead() throws IOException{
    return fs.getFileStatus(filePath).getPermission().getUserAction().implies(FsAction.READ);
  }
}
