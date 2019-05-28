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

import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hasher;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.event.WholeFileProcessedEvent;
import com.streamsets.pipeline.lib.generator.StreamCloseEventHandler;
import com.streamsets.pipeline.lib.hashing.HashingUtil;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * The Implementation of {@link AbstractWrapperStream} which uses a checksum algorithm
 * to calculate the checksum of the stream which is being read.
 *
 * The consumers of {@link ChecksumCalculatingWrapperStream} can call {@link #getCalculatedChecksum()}
 * after calling {@link #close()} to get the calculated checksum of the stream.
 *
 * @param <T> Stream implementation of {@link AutoCloseable}
 */
class ChecksumCalculatingWrapperStream<T extends AutoCloseable> extends AbstractWrapperStream<T> {
  private final Hasher hasher;
  private final HashingUtil.HashType checksumAlgorithm;
  private final StreamCloseEventHandler streamCloseEventHandler;

  private boolean isCalculated;
  private String calculatedChecksum;

  ChecksumCalculatingWrapperStream(
      T stream,
      HashingUtil.HashType checksumAlgorithm,
      StreamCloseEventHandler streamCloseEventHandler
  ) {
    super(stream);
    Utils.checkNotNull(checksumAlgorithm, "checksumAlgorithm");
    this.checksumAlgorithm = checksumAlgorithm;
    hasher = HashingUtil.getHasher(checksumAlgorithm).newHasher();
    isCalculated = false;
    this.streamCloseEventHandler = streamCloseEventHandler;
  }

  private void updateChecksum(byte[] b, int offset, int len) {
    if (len > 0) {
      hasher.putBytes(b, offset, len);
    }
  }

  @Override
  public int read() throws IOException {
    int readByte = super.read();
    if (readByte != -1) {
      hasher.putByte((byte) readByte);
    }
    return readByte;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    int bytesRead = super.read(dst);
    if (bytesRead > 0) {
      ByteBuffer readOnlyBuffer = dst.asReadOnlyBuffer();
      readOnlyBuffer.flip();
      readOnlyBuffer = readOnlyBuffer.slice();
      byte[] b;
      if (readOnlyBuffer.hasArray()) {
        b = readOnlyBuffer.array();
      } else {
        b = new byte[bytesRead];
        readOnlyBuffer.get(b);
      }
      updateChecksum(b, 0, bytesRead);
    }
    return bytesRead;
  }

  @Override
  public int read(byte[] b) throws IOException {
    int bytesRead = super.read(b);
    updateChecksum(b, 0, bytesRead);
    return bytesRead;
  }

  @Override
  public int read(byte[] b, int offset, int len) throws IOException {
    int bytesRead = super.read(b, offset, len);
    updateChecksum(b, offset, bytesRead);
    return bytesRead;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void close() throws IOException {
    if (!isCalculated) {
      //toString returns the hex string representation.
      calculatedChecksum = hasher.hash().toString();
      isCalculated = true;
      if (streamCloseEventHandler != null) {
        streamCloseEventHandler.handleCloseEvent(
            new ImmutableMap.Builder<String, Object>()
                .put(WholeFileProcessedEvent.CHECKSUM, getCalculatedChecksum())
                .put(WholeFileProcessedEvent.CHECKSUM_ALGORITHM, checksumAlgorithm)
                .build()
        );
      }
    }
    super.close();
  }

  String getCalculatedChecksum() {
    Utils.checkState(isCalculated, "Checksum not calculated until close() is called.");
    return calculatedChecksum;
  }
}
