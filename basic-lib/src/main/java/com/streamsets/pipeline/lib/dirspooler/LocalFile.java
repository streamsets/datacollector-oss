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
package com.streamsets.pipeline.lib.dirspooler;

import com.google.common.base.Preconditions;
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.streamsets.pipeline.lib.dirspooler.LocalFileSystem.PERMISSIONS;

public class LocalFile implements WrappedFile {

  private Path filePath;

  public LocalFile(Path filePath) {
    this.filePath = filePath;
  }

  public boolean isAbsolute() {
    return filePath.isAbsolute();
  }

  public String getFileName() {
    return filePath.getFileName().toString();
  }

  public String getAbsolutePath() {
    return filePath.toAbsolutePath().toString();
  }

  public String getParent() {
    return filePath.getParent().toString();
  }

  public String toString() {
    return getAbsolutePath();
  }

  public InputStream getInputStream() throws IOException {
    File file = new File(filePath.toString());
    return new FileInputStream(file);
  }

  public Map<String, Object> getFileMetadata() throws IOException {
    boolean isPosix = filePath.getFileSystem().supportedFileAttributeViews().contains("posix");
    Map<String, Object>  metadata = new HashMap<>(Files.readAttributes(filePath, isPosix? "posix:*" : "*"));
    metadata.put(HeaderAttributeConstants.FILE_NAME, filePath.getFileName().toString());
    metadata.put(HeaderAttributeConstants.FILE, filePath);
    if (isPosix && metadata.containsKey(PERMISSIONS) && Set.class.isAssignableFrom(metadata.get(PERMISSIONS).getClass())) {
      Set<PosixFilePermission> posixFilePermissions = (Set<PosixFilePermission>)(metadata.get(PERMISSIONS));
      //converts permission to rwx- format and replace it in permissions field.
      // (totally containing 9 characters 3 for user 3 for group and 3 for others)
      metadata.put(PERMISSIONS, PosixFilePermissions.toString(posixFilePermissions));
    }
    return metadata;
  }

  public long getSize() throws IOException {
    return Files.size(filePath);
  }

  @Override
  public int hashCode() {
    return getAbsolutePath().hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }

    if (o == this) {
      return true;
    }

    if (!(o instanceof LocalFile)) {
      return false;
    }

    return filePath.equals(Paths.get(((LocalFile) o).getAbsolutePath()));
  }
}
