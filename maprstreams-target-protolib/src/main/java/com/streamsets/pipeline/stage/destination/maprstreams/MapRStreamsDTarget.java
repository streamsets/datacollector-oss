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
package com.streamsets.pipeline.stage.destination.maprstreams;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.configurablestage.DTarget;
import com.streamsets.pipeline.stage.destination.kafka.KafkaConfigBean;
import com.streamsets.pipeline.stage.destination.kafka.KafkaTarget;
import com.streamsets.pipeline.stage.destination.kafka.KafkaTargetConfig;

@StageDef(
  version = 1,
  label = "MapR Streams Producer",
  description = "Writes data to MapR Streams",
  icon = "mapr.png",
  onlineHelpRefUrl = "index.html#Destinations/MapRStreamsProd.html#task_tbh_nbn_2v"
)
@ConfigGroups(value = MapRStreamsTargetGroups.class)
@GenerateResourceBundle
public class MapRStreamsDTarget extends DTarget {

  @ConfigDefBean()
  public MapRStreamsTargetConfigBean maprStreamsTargetConfigBean;

  @Override
  protected Target createTarget() {
    return new KafkaTarget(convertToKafkaConfigBean(maprStreamsTargetConfigBean));
  }

  protected KafkaConfigBean convertToKafkaConfigBean(MapRStreamsTargetConfigBean maprStreamsTargetConfigBean) {

    KafkaConfigBean kafkaConfigBean = new KafkaConfigBean();
    kafkaConfigBean.dataFormat = maprStreamsTargetConfigBean.dataFormat;
    kafkaConfigBean.dataGeneratorFormatConfig = maprStreamsTargetConfigBean.dataGeneratorFormatConfig;

    KafkaTargetConfig kafkaTargetConfig = new KafkaTargetConfig();
    kafkaTargetConfig.kafkaProducerConfigs = maprStreamsTargetConfigBean.mapRStreamsTargetConfig.kafkaProducerConfigs;
    kafkaTargetConfig.partition = maprStreamsTargetConfigBean.mapRStreamsTargetConfig.partition;
    kafkaTargetConfig.partitionStrategy = maprStreamsTargetConfigBean.mapRStreamsTargetConfig.partitionStrategy;
    kafkaTargetConfig.runtimeTopicResolution = maprStreamsTargetConfigBean.mapRStreamsTargetConfig.runtimeTopicResolution;
    kafkaTargetConfig.singleMessagePerBatch = maprStreamsTargetConfigBean.mapRStreamsTargetConfig.singleMessagePerBatch;
    kafkaTargetConfig.topic = maprStreamsTargetConfigBean.mapRStreamsTargetConfig.topic;
    kafkaTargetConfig.topicExpression = maprStreamsTargetConfigBean.mapRStreamsTargetConfig.topicExpression;
    kafkaTargetConfig.topicWhiteList = maprStreamsTargetConfigBean.mapRStreamsTargetConfig.topicWhiteList;
    kafkaConfigBean.kafkaConfig = kafkaTargetConfig;

    return kafkaConfigBean;
  }
}
