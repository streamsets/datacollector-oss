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

import com.streamsets.pipeline.api.FileRef;
import com.streamsets.pipeline.api.ProtoConfigurableEntity;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.hashing.HashingUtil;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

/**
 * An AbstractFileRef to accommodate the common logic used by the implementations of {@link FileRef}
 *
 */
public abstract class AbstractFileRef extends FileRef {
  private final boolean createMetrics;
  private final long totalSizeInBytes;
  private final double rateLimit;
  private final boolean verifyChecksum;
  private final String checksum;
  private final HashingUtil.HashType checksumAlgorithm;
  private final Set<Class<? extends AutoCloseable>> supportedStreamClasses;

  /**
   * @param bufferSize The buffer size that can be used by the input stream.
   * @param createMetrics if the metrics are needed
   * @param totalSizeInBytes the file size, can be null, if {@link #createMetrics} is false
   */
  public AbstractFileRef(
      Set<Class<? extends AutoCloseable>> supportedStreamClasses,
      int bufferSize,
      boolean createMetrics,
      long totalSizeInBytes,
      double rateLimit,
      boolean verifyChecksum,
      String checksum,
      HashingUtil.HashType checksumAlgorithm
  ) {
    super(bufferSize);
    Utils.checkNotNull(supportedStreamClasses, "supportedStreamClasses");
    Utils.checkArgument(
        supportedStreamClasses.contains(InputStream.class),
        Utils.format("Input Stream should be at least supported by the File Ref {}", this.getClass().getName())
    );
    this.supportedStreamClasses = supportedStreamClasses;
    this.createMetrics = createMetrics;
    this.totalSizeInBytes = totalSizeInBytes;
    this.rateLimit = rateLimit;
    this.verifyChecksum = verifyChecksum;
    this.checksum = checksum;
    this.checksumAlgorithm = checksumAlgorithm;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends AutoCloseable> Set<Class<T>> getSupportedStreamClasses() {
    return (Set)supportedStreamClasses;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends AutoCloseable> T createInputStream(ProtoConfigurableEntity.Context context, Class<T> streamClassType) throws IOException {
    Utils.checkArgument(
        supportedStreamClasses.contains(streamClassType),
        Utils.format("Stream class {} not supported for {}.", streamClassType, this.getClass().getName())
    );
    T stream = createInputStream(streamClassType);
    stream = (createMetrics)?(T)new MetricEnabledWrapperStream<>(toString(), totalSizeInBytes, context, stream) : stream;
    stream = (rateLimit > 0)? (T)new RateLimitingWrapperStream<>(stream, totalSizeInBytes, rateLimit) : stream;
    return (verifyChecksum)?(T)new VerifyChecksumWrapperStream<>(stream, checksum, checksumAlgorithm) : stream;
  }

  /**
   * Creates the Stream instance based on the stream class type.
   * @param streamClassType the stream class type
   * @param <T> Stream Implementation of {@link AutoCloseable}
   * @return the stream
   * @throws IOException if there are issues in creating the stream.
   */
  protected abstract <T extends AutoCloseable> T createInputStream(Class<T> streamClassType) throws IOException ;

  /**
   * Abstract File Ref Builder
   * @param <F> {@link FileRef}
   * @param <B> {@link Builder}
   */
  public static abstract class Builder<F extends AbstractFileRef, B extends Builder<F, B>> {
    protected int bufferSize = Integer.MAX_VALUE;
    protected boolean createMetrics = true;
    protected long totalSizeInBytes;
    protected boolean verifyChecksum = false;
    protected double rateLimit = -1;
    protected String checksum;
    protected HashingUtil.HashType checksumAlgorithm;

    @SuppressWarnings("unchecked")
    public B bufferSize(int bufferSize) {
      this.bufferSize = (bufferSize == -1)? Integer.MAX_VALUE : bufferSize;
      return (B) this;
    }

    @SuppressWarnings("unchecked")
    public B createMetrics(boolean createMetrics) {
      this.createMetrics = createMetrics;
      return (B)this;
    }

    public B rateLimit(double rateLimit) {
      this.rateLimit = rateLimit;
      return (B)this;
    }

    @SuppressWarnings("unchecked")
    public B verifyChecksum(boolean verifyChecksum) {
      this.verifyChecksum = verifyChecksum;
      return (B)this;
    }

    @SuppressWarnings("unchecked")
    public B totalSizeInBytes(long totalSizeInBytes) {
      this.totalSizeInBytes = totalSizeInBytes;
      return (B)this;
    }

    @SuppressWarnings("unchecked")
    public B checksum(String checksum) {
      Utils.checkArgument(verifyChecksum, "Verify Checksum should be true");
      Utils.checkNotNull(checksum, checksum);
      this.checksum = checksum;
      return (B)this;
    }

    @SuppressWarnings("unchecked")
    public B checksumAlgorithm(HashingUtil.HashType checksumAlgorithm) {
      Utils.checkArgument(verifyChecksum, "Verify Checksum should be true");
      Utils.checkNotNull(checksumAlgorithm, "checksumAlgorithm");
      this.checksumAlgorithm = checksumAlgorithm;
      return (B)this;
    }

    public abstract F build();
  }

}
