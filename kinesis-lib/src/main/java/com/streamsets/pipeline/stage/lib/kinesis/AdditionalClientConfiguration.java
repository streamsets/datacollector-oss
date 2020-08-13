/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.pipeline.stage.lib.kinesis;

import java.util.Arrays;

public enum AdditionalClientConfiguration {
  FAIL_OVER_TIME_MILLIS("failOverTimeMillis"),
  TASK_BACKOFF_TIME_MILLIS("taskBackoffTimeMillis"),
  METRICS_BUFFER_TIME_MILLIS("metricsBufferTimeMillis"),
  METRICS_MAX_QUEUE_SIZE("metricsMaxQueueSize"),
  VALIDATE_SEQUENCE_NUMBER_BEFORE_CHECK_POINTING("validateSequenceNumberBeforeCheckpointing"),
  SHUTDOWN_GRACE_MILLIS("shutdownGraceMillis"),
  BILLING_MODE("billingMode"),
  TIME_OUT_IN_SECONDS("timeoutInSeconds"),
  RETRY_GET_RECORDS_IN_SECONDS("retryGetRecordsInSeconds"),
  MAX_GET_RECORDS_THREAD_POOL("maxGetRecordsThreadPool"),
  MAX_LEASE_RENEWAL_THREADS("maxLeaseRenewalThreads"),
  LOG_WARNING_FOR_TASK_AFTER_MILLIS("logWarningForTaskAfterMillis"),
  LIST_SHARDS_BACK_OFF_TIME_IN_MILLIS("listShardsBackoffTimeInMillis"),
  MAX_LIST_SHARDS_RETRY_ATTEMPTS("maxListShardsRetryAttempts"),
  CLEAN_UP_LEASES_UPON_SHARD_COMPLETION("cleanupLeasesUponShardCompletion"),
  USER_AGENT_PREFIX("userAgentPrefix"),
  USER_AGENT_SUFFIX("userAgentSuffix"),
  MAX_CONNECTIONS("maxConnections"),
  REQUEST_TIMEOUT("requestTimeout"),
  CLIENT_EXECUTION_TIMEOUT("clientExecutionTimeout"),
  THROTTLE_RETRIES("throttleRetries"),
  CONNECTION_MAX_IDLE_MILLIS("connectionMaxIdleMillis"),
  VALIDATE_AFTER_INACTIVITY_MILLIS("validateAfterInactivityMillis"),
  USE_EXPECT_CONTINUE("useExpectContinue"),
  MAX_CONSECUTIVE_RETRIES_BEFORE_THROTTLING("maxConsecutiveRetriesBeforeThrottling"),
  RETRY_MODE("retryMode");

  private final String property;

  AdditionalClientConfiguration(String property) { this.property = property; }

  public String getProperty() { return property; }

  public static boolean propertyExists(String property) {
    return Arrays.stream(AdditionalClientConfiguration.values()).anyMatch(value -> value.getProperty()
                                                                                        .equalsIgnoreCase(property));
  }

  public static AdditionalClientConfiguration getName(String property) {
    for (AdditionalClientConfiguration e : AdditionalClientConfiguration.values()){
      if(e.getProperty().equalsIgnoreCase(property)) return e;
    }
    return null;
  }

}
