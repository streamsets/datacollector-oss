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
package com.streamsets.pipeline.stage.processor.transformer;

import com.google.common.collect.ImmutableSet;

import com.streamsets.pipeline.lib.hashing.HashingUtil;
import com.streamsets.pipeline.lib.io.fileref.AbstractFileRef;

import java.io.IOException;
import java.io.InputStream;

public class WholeFileTransformerFileRef extends AbstractFileRef {
  private String filePath;

  public WholeFileTransformerFileRef(
      String filePath,
      int bufferSize,
      boolean createMetrics,
      long totalSizeInBytes,
      double rateLimit,
      boolean verifyChecksum,
      String checksum,
      HashingUtil.HashType checksumAlgorithm
  ) {
    super(
        ImmutableSet.<Class<? extends AutoCloseable>>of(InputStream.class),
        bufferSize,
        createMetrics,
        totalSizeInBytes,
        rateLimit,
        verifyChecksum,
        checksum,
        checksumAlgorithm
    );
    this.filePath = filePath;
  }
  @Override
  @SuppressWarnings("unchecked")
  public <T extends AutoCloseable> T createInputStream(Class<T> streamClassType) throws IOException {
    return (T) new WholeFileTransformerInputStream(filePath);
  }

  @Override
  public String toString() {
    return "Whole File Transformer: Parquet, ";
  }

  /**
   * Builder for building {@link WholeFileTransformerFileRef}
   */
  public static final class Builder extends AbstractFileRef.Builder {
    private String filePath;

    public Builder filePath(String filePath) {
      this.filePath = filePath;
      return this;
    }

    @Override
    public WholeFileTransformerFileRef build() {
      return new WholeFileTransformerFileRef(
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
