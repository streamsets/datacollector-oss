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
package com.streamsets.pipeline.stage.cloudstorage.origin;

import com.google.cloud.storage.Blob;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.lib.hashing.HashingUtil;
import com.streamsets.pipeline.lib.io.fileref.AbstractFileRef;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

public final class GCSFileRef extends AbstractFileRef {

  private final Blob blob;

  public GCSFileRef(
      Blob blob,
      int bufferSize,
      boolean createMetrics,
      long fileSize,
      double rateLimit,
      boolean verifyChecksum,
      String checksum,
      HashingUtil.HashType checksumAlgorithm
  ) {
    super(
        ImmutableSet.of(InputStream.class, ReadableByteChannel.class),
        bufferSize,
        createMetrics,
        fileSize,
        rateLimit,
        verifyChecksum,
        checksum,
        checksumAlgorithm
    );
    this.blob = blob;
  }

  @SuppressWarnings("unchecked")
  protected  <T extends AutoCloseable> T createInputStream(Class<T> streamClassType) throws IOException {
    ReadableByteChannel readableByteChannel = blob.reader();
    return (streamClassType.equals(ReadableByteChannel.class))?
        (T) readableByteChannel : (T) Channels.newInputStream(readableByteChannel);
  }

  @Override
  public String toString() {
    return "gs://" + blob.getBucket() + "/" + blob.getName();
  }

  /**
   * Builder for building {@link GCSFileRef}
   */
  public static final class Builder extends AbstractFileRef.Builder<GCSFileRef, GCSFileRef.Builder> {
    private Blob blob;

    public Builder blob(Blob blob) {
      this.blob = blob;
      return this;
    }

    @Override
    public GCSFileRef build() {
      return new GCSFileRef(
          blob,
          bufferSize,
          createMetrics,
          blob.getSize(),
          rateLimit,
          verifyChecksum,
          checksum,
          checksumAlgorithm
      );
    }
  }
}
