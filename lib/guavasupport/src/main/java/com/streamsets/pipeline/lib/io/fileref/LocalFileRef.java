/*
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.pipeline.lib.io.fileref;


import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.lib.hashing.HashingUtil;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.ReadableByteChannel;

public class LocalFileRef extends AbstractSpoolerFileRef {

  private String filePath;

  public LocalFileRef(
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
        ImmutableSet.<Class<? extends AutoCloseable>>of(InputStream.class, ReadableByteChannel.class),
        bufferSize,
        createMetrics,
        fileSize,
        rateLimit,
        verifyChecksum,
        checksum,
        checksumAlgorithm
    );
    this.filePath = filePath;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected  <T extends AutoCloseable> T createInputStream(Class<T> streamClassType) throws IOException {
    if (streamClassType.equals(ReadableByteChannel.class)) {
      return (T) new FileInputStream(filePath).getChannel();
    } else {
      return (T) new FileInputStream(filePath);
    }
  }

  @Override
  public String toString() {
    return "Local: '" + filePath + "'";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    LocalFileRef that = (LocalFileRef) o;

    return filePath != null ? filePath.equals(that.filePath) : that.filePath == null;

  }

  @Override
  public int hashCode() {
    return filePath != null ? filePath.hashCode() : 0;
  }

  /**
   * Builder for building {@link LocalFileRef}
   */
  public static final class Builder extends AbstractSpoolerFileRef.Builder {

    @Override
    public LocalFileRef build() {
      return new LocalFileRef(
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
