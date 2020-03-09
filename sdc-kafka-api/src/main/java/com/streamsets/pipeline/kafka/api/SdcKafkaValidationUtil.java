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

import com.google.common.net.HostAndPort;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;

import java.util.List;
import java.util.Map;

public interface SdcKafkaValidationUtil {

  public List<HostAndPort> validateKafkaBrokerConnectionString(
    List<Stage.ConfigIssue> issues,
    String connectionString,
    String configGroupName,
    String configName,
    Stage.Context context
  );

  public List<HostAndPort> validateZkConnectionString(
    List<Stage.ConfigIssue> issues,
    String connectString,
    String configGroupName,
    String configName,
    Stage.Context context
  );

  public boolean validateTopicExistence(
    Stage.Context context,
    String groupName,
    String configName,
    List<HostAndPort> kafkaBrokers,
    String metadataBrokerList,
    String topic,
    Map<String, Object> kafkaProducerConfigs,
    List<Stage.ConfigIssue> issues,
    boolean producer
  );

  public int getPartitionCount(
    String metadataBrokerList,
    String topic,
    Map<String, Object> kafkaClientConfigs,
    int messageSendMaxRetries,
    long retryBackoffMs
  ) throws StageException;

  public String getVersion();

  public void createTopicIfNotExists(String topic, Map<String, Object> kafkaClientConfigs, String metadataBrokerList)
      throws StageException;

  public boolean isProvideKeytabAllowed(List<Stage.ConfigIssue> issues, Stage.Context context);
}
