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
package com.streamsets.datacollector.execution.runner;

import com.streamsets.pipeline.api.impl.Utils;

public class RetryUtils {
  // Retry at 15 secs, 225 secs and 300 secs
  private static final int[] RETRY_BACKOFF_SECS = { 15, 30, 60, 120, 240, 300 };

  private RetryUtils() {}

  public static long getNextRetryTimeStamp(int retryAttempt, long currentTime) {
    Utils.checkState(retryAttempt != 0, "Retry attempt cannot be 0");
    if (retryAttempt - 1 >= RETRY_BACKOFF_SECS.length) {
      retryAttempt = RETRY_BACKOFF_SECS.length - 1;
    } else {
      retryAttempt = retryAttempt - 1;
    }
    return currentTime + (RETRY_BACKOFF_SECS[retryAttempt] * 1000);
  }

}
