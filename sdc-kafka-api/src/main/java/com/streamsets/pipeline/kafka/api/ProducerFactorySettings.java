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

import com.streamsets.pipeline.config.DataFormat;

import java.util.Map;

public class ProducerFactorySettings {

  private final Map<String, Object> kafkaProducerConfigs;
  private final PartitionStrategy partitionStrategy;
  private final String metadataBrokerList;
  private final DataFormat dataFormat;
  private final boolean sendWriteResponse;

  public ProducerFactorySettings(
      Map<String, Object> kafkaProducerConfigs,
      PartitionStrategy partitionStrategy,
      String metadataBrokerList,
      DataFormat dataFormat,
      boolean sendWriteResponse
  ) {
    this.kafkaProducerConfigs = kafkaProducerConfigs;
    this.partitionStrategy = partitionStrategy;
    this.metadataBrokerList = metadataBrokerList;
    this.dataFormat = dataFormat;
    this.sendWriteResponse = sendWriteResponse;
  }

  public Map<String, Object> getKafkaProducerConfigs() {
    return kafkaProducerConfigs;
  }

  public PartitionStrategy getPartitionStrategy() {
    return partitionStrategy;
  }

  public String getMetadataBrokerList() {
    return metadataBrokerList;
  }

  public DataFormat getDataFormat() {
    return dataFormat;
  }

  public boolean isSendWriteResponse() {
    return sendWriteResponse;
  }
}
