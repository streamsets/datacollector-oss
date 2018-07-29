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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import com.streamsets.pipeline.api.impl.Utils;

/**
 * The Implementation of {@link AbstractPrePostReadOperationPerformingStream} which limits the rate
 * for the stream read. (EX: Bandwidth rate limit)
 * @param <T> Stream implementation of {@link AutoCloseable}
 */
class RateLimitingWrapperStream<T extends AutoCloseable> extends AbstractPrePostReadOperationPerformingStream<T> {
  private final RateLimiter rateLimiter;
  private long remainingStreamSize;

  RateLimitingWrapperStream(T stream, long totalStreamSize, double rateLimit) {
    super(stream);
    Utils.checkArgument(rateLimit > 0, "Rate limit for this stream should be greater than 0.");
    rateLimiter = RateLimiter.create(rateLimit);
    remainingStreamSize = totalStreamSize;
  }

  @VisibleForTesting
  void acquire(int bytesWishToBeRead) {
    rateLimiter.acquire(bytesWishToBeRead);
  }

  @Override
  protected void performPreReadOperation(int bytesToBeRead) {
    //At the last set of bytes we would actually
    //be reading less than what we wish to read so optimise for that.
    int bytesWishToBeRead = (int) Math.min(bytesToBeRead, remainingStreamSize);
    if (bytesWishToBeRead > 0) {
      acquire(bytesWishToBeRead);
    }
  }

  @Override
  protected void performPostReadOperation(int bytesRead) {
    remainingStreamSize -= bytesRead;
  }
}
