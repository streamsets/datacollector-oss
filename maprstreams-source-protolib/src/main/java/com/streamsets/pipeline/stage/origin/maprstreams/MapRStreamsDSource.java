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
package com.streamsets.pipeline.stage.origin.maprstreams;

import com.streamsets.pipeline.Utils;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ErrorListener;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DClusterSourceOffsetCommitter;
import com.streamsets.pipeline.api.impl.ClusterSource;
import com.streamsets.pipeline.stage.origin.kafka.ClusterKafkaSourceFactory;
import com.streamsets.pipeline.stage.origin.kafka.DelegatingKafkaSource;
import com.streamsets.pipeline.stage.origin.kafka.KafkaConfigBean;
import com.streamsets.pipeline.stage.origin.kafka.StandaloneKafkaSourceFactory;

@StageDef(
    version = 5,
    label = "MapR Streams Consumer",
    description = "Reads data from MapR Streams",
    execution = {ExecutionMode.STANDALONE, ExecutionMode.CLUSTER_YARN_STREAMING},
    libJarsRegex = {"maprfs-\\d+.*"},
    icon = "mapr_es.png",
    recordsByRef = true,
    upgrader = MapRStreamsSourceUpgrader.class,
    onlineHelpRefUrl ="index.html?contextID=task_bfz_gch_2v"
)
@ConfigGroups(value = MapRStreamsSourceGroups.class)
@HideConfigs(value = {Utils.MAPR_STREAMS_DATA_FORMAT_CONFIG_BEAN_PREFIX + "compression"})
@GenerateResourceBundle
public class MapRStreamsDSource extends DClusterSourceOffsetCommitter implements ErrorListener {

  @ConfigDefBean
  public MapRStreamsSourceConfigBean maprstreamsSourceConfigBean;

  private DelegatingKafkaSource delegatingKafkaSource;

  @Override
  protected Source createSource() {
    KafkaConfigBean kafkaConfigBean = convertToKafkaConfigBean(maprstreamsSourceConfigBean);
    delegatingKafkaSource = new DelegatingKafkaSource(new StandaloneKafkaSourceFactory(kafkaConfigBean),
        new ClusterKafkaSourceFactory(kafkaConfigBean)
    );
    return delegatingKafkaSource;
  }

  @Override
  public Source getSource() {
    return source != null ? delegatingKafkaSource.getSource() : null;
  }

  @Override
  public void errorNotification(Throwable throwable) {
    DelegatingKafkaSource delegatingSource = delegatingKafkaSource;
    if (delegatingSource != null) {
      Source source = delegatingSource.getSource();
      if (source instanceof ErrorListener) {
        ((ErrorListener) source).errorNotification(throwable);
      }
    }
  }

  @Override
  public void shutdown() {
    DelegatingKafkaSource delegatingSource = delegatingKafkaSource;
    if (delegatingSource != null) {
      Source source = delegatingSource.getSource();
      if (source instanceof ClusterSource) {
        ((ClusterSource) source).shutdown();
      }
    }
  }

  private KafkaConfigBean convertToKafkaConfigBean(MapRStreamsSourceConfigBean maprstreamsSourceConfigBean) {

    KafkaConfigBean kafkaConfigBean = new KafkaConfigBean();
    kafkaConfigBean.dataFormat = maprstreamsSourceConfigBean.dataFormat;
    kafkaConfigBean.dataFormatConfig = maprstreamsSourceConfigBean.dataFormatConfig;
    kafkaConfigBean.kafkaConsumerConfigs = maprstreamsSourceConfigBean.kafkaConsumerConfigs;
    kafkaConfigBean.consumerGroup = maprstreamsSourceConfigBean.consumerGroup;
    kafkaConfigBean.maxBatchSize = maprstreamsSourceConfigBean.maxBatchSize;
    kafkaConfigBean.produceSingleRecordPerMessage = maprstreamsSourceConfigBean.produceSingleRecordPerMessage;
    kafkaConfigBean.topic = maprstreamsSourceConfigBean.topic;
    kafkaConfigBean.maxWaitTime = maprstreamsSourceConfigBean.maxWaitTime;

    return kafkaConfigBean;
  }
}
