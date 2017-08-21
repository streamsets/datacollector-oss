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

import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.hashing.HashingUtil;

import java.io.IOException;

/**
 * The Implementation of {@link ChecksumCalculatingWrapperStream} which uses a checksum algorithm
 * verifies the checksum of the stream which is being read.
 * This function {@link #close()} throws an error if there is a checksum mismatch when the stream is closed.
 * @param <T> Stream implementation of {@link AutoCloseable}
 */
final class VerifyChecksumWrapperStream<T extends AutoCloseable> extends ChecksumCalculatingWrapperStream<T> {
  private final String checksum;

  VerifyChecksumWrapperStream(T stream, String checksum, HashingUtil.HashType checksumAlgorithm) {
    super(stream, checksumAlgorithm, null);
    Utils.checkNotNull(checksum, "checksum");
    this.checksum = checksum;
  }

  @Override
  public void close() throws IOException {
    //Calculates the checksum
    super.close();
    String calculatedChecksum = getCalculatedChecksum();
    if (!calculatedChecksum.equals(checksum)) {
      throw new IOException(Utils.format("The checksum did not match. Expected: {}, Actual: {}", checksum, calculatedChecksum));
    }
  }
}
