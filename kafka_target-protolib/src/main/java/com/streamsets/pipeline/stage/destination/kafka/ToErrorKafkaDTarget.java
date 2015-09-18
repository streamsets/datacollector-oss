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
package com.streamsets.pipeline.stage.destination.kafka;


import com.streamsets.pipeline.api.ErrorStage;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.config.DataFormat;

@StageDef(
    version = 1,
    label = "Write to Kafka",
    description = "Writes records to Kafka as SDC Records",
    icon = "")
@ErrorStage
@HideConfigs(preconditions = true, onErrorRecord = true, value = {"dataFormat", "charset"})
@GenerateResourceBundle
public class ToErrorKafkaDTarget extends KafkaDTarget {

  @Override
  protected Target createTarget() {
    return new KafkaTarget(metadataBrokerList, false, topic, null, null, partitionStrategy, partition, DataFormat.SDC_JSON, null,
                           singleMessagePerBatch, kafkaProducerConfigs, null, null, false, null,
                           null, false, null, false, null);
  }

}
