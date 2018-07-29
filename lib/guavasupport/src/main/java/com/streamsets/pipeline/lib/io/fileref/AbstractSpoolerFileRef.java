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
package com.streamsets.pipeline.lib.io.fileref;

import com.streamsets.pipeline.lib.hashing.HashingUtil;

import java.util.Set;

public abstract class AbstractSpoolerFileRef extends AbstractFileRef {

  public AbstractSpoolerFileRef(
      Set<Class<? extends AutoCloseable>> supportedStreamClasses,
      int bufferSize,
      boolean createMetrics,
      long fileSize,
      double rateLimit,
      boolean verifyChecksum,
      String checksum,
      HashingUtil.HashType checksumAlgorithm
  ) {
    super(
        supportedStreamClasses,
        bufferSize,
        createMetrics,
        fileSize,
        rateLimit,
        verifyChecksum,
        checksum,
        checksumAlgorithm
    );
  }

  /**
   * Builder for building {@link com.streamsets.pipeline.lib.io.fileref.AbstractSpoolerFileRef}
   */
  public static abstract class Builder extends AbstractFileRef.Builder<AbstractSpoolerFileRef, AbstractSpoolerFileRef.Builder> {
    protected String filePath;

    public AbstractSpoolerFileRef.Builder filePath(String filePath) {
      this.filePath = filePath;
      return this;
    }
  }
}
