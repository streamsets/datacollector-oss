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
package com.streamsets.pipeline.stage.destination.maprstreams;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.configurablestage.DTarget;
import com.streamsets.pipeline.stage.destination.kafka.KafkaTarget;
import com.streamsets.pipeline.stage.destination.kafka.KafkaTargetConfig;

@StageDef(
  version = 3,
  label = "MapR Streams Producer",
  description = "Writes data to MapR Streams",
  icon = "mapr_es.png",
    upgrader = MapRStreamsTargetUpgrader.class,
  onlineHelpRefUrl ="index.html?contextID=task_tbh_nbn_2v"
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

  protected KafkaTargetConfig convertToKafkaConfigBean(MapRStreamsTargetConfigBean maprStreamsTargetConfigBean) {

    KafkaTargetConfig kafkaConfigBean = new KafkaTargetConfig();
    kafkaConfigBean.dataFormat = maprStreamsTargetConfigBean.dataFormat;
    kafkaConfigBean.dataGeneratorFormatConfig = maprStreamsTargetConfigBean.dataGeneratorFormatConfig;
    kafkaConfigBean.kafkaProducerConfigs = maprStreamsTargetConfigBean.mapRStreamsTargetConfig.kafkaProducerConfigs;
    kafkaConfigBean.partition = maprStreamsTargetConfigBean.mapRStreamsTargetConfig.partition;
    kafkaConfigBean.partitionStrategy = maprStreamsTargetConfigBean.mapRStreamsTargetConfig.partitionStrategy;
    kafkaConfigBean.runtimeTopicResolution = maprStreamsTargetConfigBean.mapRStreamsTargetConfig.runtimeTopicResolution;
    kafkaConfigBean.singleMessagePerBatch = maprStreamsTargetConfigBean.mapRStreamsTargetConfig.singleMessagePerBatch;
    kafkaConfigBean.topic = maprStreamsTargetConfigBean.mapRStreamsTargetConfig.topic;
    kafkaConfigBean.topicExpression = maprStreamsTargetConfigBean.mapRStreamsTargetConfig.topicExpression;
    kafkaConfigBean.topicWhiteList = maprStreamsTargetConfigBean.mapRStreamsTargetConfig.topicWhiteList;

    return kafkaConfigBean;
  }
}
