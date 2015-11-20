/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;

import java.util.List;

public interface SdcKafkaValidationUtil {

  public List<KafkaBroker> validateKafkaBrokerConnectionString(
    List<Stage.ConfigIssue> issues,
    String connectionString,
    String configGroupName,
    String configName,
    Stage.Context context
  );

  public List<KafkaBroker> validateZkConnectionString(
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
    List<KafkaBroker> kafkaBrokers,
    String metadataBrokerList,
    String topic,
    List<Stage.ConfigIssue> issues
  );

  public int getPartitionCount(
    String metadataBrokerList,
    String topic,
    int messageSendMaxRetries,
    long retryBackoffMs
  ) throws StageException;

  public String getVersion();

}
