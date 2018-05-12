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

import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.lib.io.fileref.AbstractSpoolerFileRef;
import com.streamsets.pipeline.lib.hashing.HashingUtil;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;

public class HdfsFileRef extends AbstractSpoolerFileRef {

  private String filePath;
  private FileSystem fs;

  public HdfsFileRef(
      FileSystem fs,
      String filePath,
      int bufferSize,
      boolean createMetrics,
      long fileSize,
      double rateLimit,
      boolean verifyChecksum,
      String checksum,
      HashingUtil.HashType checksumAlgorithm
  ) {
    super(
        ImmutableSet.<Class<? extends AutoCloseable>>of(InputStream.class, FSDataInputStream.class),
        bufferSize,
        createMetrics,
        fileSize,
        rateLimit,
        verifyChecksum,
        checksum,
        checksumAlgorithm
    );
    this.fs = fs;
    this.filePath = filePath;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected  <T extends AutoCloseable> T createInputStream(Class<T> streamClassType) throws IOException {
    return (T) fs.open(new Path(filePath));
  }

  @Override
  public String toString() {
    return "Hdfs: '" + filePath + "'";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    HdfsFileRef that = (HdfsFileRef) o;

    return filePath != null ? filePath.equals(that.filePath) : that.filePath == null;

  }

  @Override
  public int hashCode() {
    return filePath != null ? filePath.hashCode() : 0;
  }

  /**
   * Builder for building {@link HdfsFileRef}
   */
  public static final class Builder extends AbstractSpoolerFileRef.Builder {
    private FileSystem fs;

    public Builder fileSystem(FileSystem fs) {
      this.fs = fs;
      return this;
    }

    @Override
    public HdfsFileRef build() {
      return new HdfsFileRef(
          fs,
          filePath,
          bufferSize,
          createMetrics,
          totalSizeInBytes,
          rateLimit,
          verifyChecksum,
          checksum,
          checksumAlgorithm
      );
    }
  }
}

