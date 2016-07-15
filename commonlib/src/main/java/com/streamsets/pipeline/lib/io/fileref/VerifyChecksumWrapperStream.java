/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.lib.io.fileref;

import com.google.common.base.Enums;
import com.google.common.base.Optional;
import com.google.common.hash.Hasher;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.hashing.HashingUtil;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * The Implementation of {@link AbstractWrapperStream} which uses a checksum algorithm
 * to verify the checksum of the stream which is being read.
 * This function throws an error if there is a checksum mismatch when the stream is closed.
 * @param <T> Stream implementation of {@link AutoCloseable}
 */
final class VerifyChecksumWrapperStream<T extends AutoCloseable> extends AbstractWrapperStream<T> {
  private final String checksum;
  private final Hasher hasher;

  VerifyChecksumWrapperStream(T stream, String checksum, HashingUtil.HashType checksumAlgorithm) {
    super(stream);
    Utils.checkNotNull(checksum, "checksum");
    Utils.checkNotNull(checksumAlgorithm, "checksumAlgorithm");
    this.checksum = checksum;
    hasher = HashingUtil.getHasher(checksumAlgorithm).newHasher();
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
  public void close() throws IOException {
    //toString returns the hex string representation.
    String calculatedChecksum = hasher.hash().toString();
    if (!calculatedChecksum.equals(checksum)) {
      throw new IOException(Utils.format("The checksum did not match. Expected: {}, Actual: {}", checksum, calculatedChecksum));
    }
    super.close();
  }
}
