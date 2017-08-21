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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TestRetryUtils {

  @Test
  public void testRetryInterval() throws Exception {
    long currentTime = System.currentTimeMillis();
   int retryAttempt = 1 ;
    long delay = RetryUtils.getNextRetryTimeStamp(retryAttempt, currentTime);
    // 15 secs - first retry
    assertEquals(currentTime + 15000, delay);
    delay = RetryUtils.getNextRetryTimeStamp(retryAttempt + 1, currentTime);
    // 30 secs  - 2nd retry
    assertEquals(currentTime + 30000, delay);
    delay = RetryUtils.getNextRetryTimeStamp(retryAttempt + 2, currentTime);
    // 60 secs  - 3rd retry
    assertEquals(currentTime + 60000, delay);
    delay = RetryUtils.getNextRetryTimeStamp(retryAttempt + 3, currentTime);
    // 120 secs - 4th retry
    assertEquals(currentTime + 120000, delay);
    delay = RetryUtils.getNextRetryTimeStamp(retryAttempt + 4, currentTime);
    // 240 secs - 5th retry
    assertEquals(currentTime + 240000, delay);
    delay = RetryUtils.getNextRetryTimeStamp(retryAttempt + 5, currentTime);
    // 300 secs - 6th retry
    assertEquals(currentTime + 300000, delay);
    delay = RetryUtils.getNextRetryTimeStamp(retryAttempt + 10, currentTime);
    // 300 secs - 10th retry
    assertEquals(currentTime + 300000, delay);
  }

}
