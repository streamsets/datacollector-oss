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
package com.streamsets.pipeline.stage.origin.s3;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.lib.hashing.HashingUtil;
import com.streamsets.pipeline.lib.io.fileref.AbstractFileRef;
import com.streamsets.pipeline.lib.io.fileref.AbstractWrapperStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

final class S3FileRef extends AbstractFileRef {
  private final AmazonS3Client s3Client;
  private final S3ObjectSummary s3ObjectSummary;

  @SuppressWarnings("unchecked")
  S3FileRef(
      AmazonS3Client s3Client,
      S3ObjectSummary s3ObjectSummary,
      int bufferSize,
      boolean createMetrics,
      long totalSizeInBytes,
      boolean verifyChecksum,
      String checksum,
      HashingUtil.HashType checksumAlgorithm) {
    super(
        (Set)ImmutableSet.of(
            (Class<? extends AutoCloseable>)InputStream.class
        ),
        bufferSize,
        createMetrics,
        totalSizeInBytes,
        verifyChecksum,
        checksum,
        checksumAlgorithm
    );
    this.s3Client = s3Client;
    this.s3ObjectSummary = s3ObjectSummary;
  }
  @Override
  @SuppressWarnings("unchecked")
  public <T extends AutoCloseable> T createInputStream(Class<T> streamClassType) throws IOException {
    //The object is fetched every time a stream needs to be opened.
    return (T) AmazonS3Util.getObject(s3Client, s3ObjectSummary.getBucketName(), s3ObjectSummary.getKey()).getObjectContent();}


  @Override
  public String toString() {
    return "S3: Bucket='" + s3ObjectSummary.getBucketName()+ ", ObjectKey='" + s3ObjectSummary.getKey() + "'";
  }

  /**
   * Builder for building {@link S3FileRef}
   */
  public static final class Builder extends AbstractFileRef.Builder<S3FileRef, Builder> {
    private S3ObjectSummary s3ObjectSummary;
    private AmazonS3Client s3Client;

    public Builder s3Client(AmazonS3Client s3Client) {
      this.s3Client = s3Client;
      return this;
    }

    public Builder s3ObjectSummary(S3ObjectSummary s3ObjectSummary) {
      this.s3ObjectSummary = s3ObjectSummary;
      return this;
    }

    @Override
    public S3FileRef build() {
      return new S3FileRef(
          s3Client,
          s3ObjectSummary,
          bufferSize,
          createMetrics,
          totalSizeInBytes,
          verifyChecksum,
          checksum,
          checksumAlgorithm
      );
    }
  }
}
