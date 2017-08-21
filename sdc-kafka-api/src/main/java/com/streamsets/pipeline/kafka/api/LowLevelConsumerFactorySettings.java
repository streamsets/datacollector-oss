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
package com.streamsets.pipeline.kafka.api;

public class LowLevelConsumerFactorySettings {

  private final String topic;
  private final int partition;
  private final String brokerHost;
  private final int brokerPort;
  private final String clientName;
  private final int minFetchSize;
  private final int maxFetchSize;
  private final int maxWaitTime;

  public LowLevelConsumerFactorySettings(
      String topic,
      int partition,
      String brokerHost,
      int brokerPort,
      String clientName,
      int minFetchSize,
      int maxFetchSize,
      int maxWaitTime
  ) {
    this.topic = topic;
    this.partition = partition;
    this.brokerHost = brokerHost;
    this.brokerPort = brokerPort;
    this.clientName = clientName;
    this.minFetchSize = minFetchSize;
    this.maxFetchSize = maxFetchSize;
    this.maxWaitTime = maxWaitTime;
  }

  public String getTopic() {
    return topic;
  }

  public int getPartition() {
    return partition;
  }

  public String getBrokerHost() {
    return brokerHost;
  }

  public int getBrokerPort() {
    return brokerPort;
  }

  public String getClientName() {
    return clientName;
  }

  public int getMinFetchSize() {
    return minFetchSize;
  }

  public int getMaxFetchSize() {
    return maxFetchSize;
  }

  public int getMaxWaitTime() {
    return maxWaitTime;
  }
}
