/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.execution.runner;

import com.streamsets.pipeline.api.impl.Utils;

public class RetryUtils {
  // Retry at 15 secs, 225 secs and 300 secs
  private static final int RETRY_BACKOFF_SECS[] = { 15, 30, 60, 120, 240, 300 };

  public static long getNextRetryTimeStamp(int retryAttempt, long previousRetryEndTimeStamp) {
    Utils.checkState(retryAttempt != 0, "Retry attempt cannot be 0");
    if (retryAttempt - 1 >= RETRY_BACKOFF_SECS.length) {
      retryAttempt = RETRY_BACKOFF_SECS.length - 1;
    } else {
      retryAttempt = retryAttempt - 1;
    }
    long scheduledTime = previousRetryEndTimeStamp + (RETRY_BACKOFF_SECS[retryAttempt] * 1000);
    return scheduledTime;
  }

}
