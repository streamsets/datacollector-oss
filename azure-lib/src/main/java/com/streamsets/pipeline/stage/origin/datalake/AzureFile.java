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
package com.streamsets.pipeline.stage.origin.datalake;

import com.streamsets.pipeline.lib.dirspooler.WrappedFile;
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class AzureFile implements WrappedFile {
  public static final String PERMISSIONS = "permissions";

  private Map<String, Object> customMetadata;
  private FileStatus status;
  private FileSystem fs;

  public AzureFile(FileSystem fs, FileStatus status) {
    this.status = status;
    this.fs = fs;
    customMetadata = getFileMetadata();
  }

  public boolean isAbsolute() {
    return status.getPath().isAbsolute();
  }

  public String getFileName() {
    return status.getPath().getName();
  }

  public String getAbsolutePath() {
    return status.getPath().toUri().getPath();
  }

  public String getParent() {
    return status.getPath().getParent().toUri().getPath();
  }

  public long getSize() {
    return status.getLen();
  }

  public InputStream getInputStream() throws IOException {
    return fs.open(status.getPath()).getWrappedStream();
  }

  @SuppressWarnings("unchecked")
  public Map<String, Object> getFileMetadata() {
    Map<String, Object>  metadata = new HashMap<>();
    metadata.put(HeaderAttributeConstants.FILE_NAME, status.getPath().getName());
    metadata.put(HeaderAttributeConstants.FILE, status.getPath().toUri().getPath());
    metadata.put(HeaderAttributeConstants.LAST_MODIFIED_TIME, status.getModificationTime());
    metadata.put(HeaderAttributeConstants.LAST_ACCESS_TIME, status.getAccessTime());
    metadata.put(HeaderAttributeConstants.IS_DIRECTORY, status.isDirectory());
    metadata.put(HeaderAttributeConstants.IS_SYMBOLIC_LINK, status.isSymlink());
    metadata.put(HeaderAttributeConstants.SIZE, status.getLen());
    metadata.put(HeaderAttributeConstants.OWNER, status.getOwner());
    metadata.put(HeaderAttributeConstants.GROUP, status.getGroup());
    metadata.put(HeaderAttributeConstants.BLOCK_SIZE, status.getBlockSize());
    metadata.put(HeaderAttributeConstants.REPLICATION, status.getReplication());
    metadata.put(HeaderAttributeConstants.IS_ENCRYPTED, status.isEncrypted());

    FsPermission permission = status.getPermission();
    if (permission != null) {
      metadata.put(PERMISSIONS, permission.toString());
    }

    return metadata;
  }

  /**
   * Returns the custom metadata map for the wrapped file.
   *
   * It is initialized with the same data as getFileMetadata, but in this case the map returned is persistent across
   * invocations and caller is allowed to add new entries or modify the values of the initial entries.
   *
   * @return The {@code Map<String, Object>} file custom metadata.
   */
  public Map<String, Object> getCustomMetadata() {
    return customMetadata;
  }

  @Override
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
    AzureFile file = (AzureFile) o;
    return Objects.equals(status, file.status);
  }

  @Override
  public int hashCode() {
    return Objects.hash(status);
  }

  @Override
  public boolean canRead() {
    return status.getPermission().getUserAction().implies(FsAction.READ);
  }
}
