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
package com.streamsets.pipeline.stage.origin.kafka;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ErrorListener;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.RawSource;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DClusterSourceOffsetCommitter;
import com.streamsets.pipeline.api.impl.ClusterSource;
import com.streamsets.pipeline.kafka.api.KafkaOriginGroups;

@StageDef(
  version = 14,
  label = "Kafka Consumer",
  description = "Reads data from Kafka",
  execution = {ExecutionMode.CLUSTER_YARN_STREAMING, ExecutionMode.CLUSTER_MESOS_STREAMING, ExecutionMode.STANDALONE},
  libJarsRegex = {"spark-streaming-kafka.*", "kafka_\\d+.*", "kafka-clients-\\d+.*", "metrics-core-\\d+.*"},
  icon = "kafka.png",
  recordsByRef = true,
  upgrader = KafkaSourceUpgrader.class,
  upgraderDef = "upgrader/KafkaDSource.yaml",
  onlineHelpRefUrl ="index.html?contextID=task_npx_xgf_vq"
)
@RawSource(rawSourcePreviewer = KafkaRawSourcePreviewer.class, mimeType = "*/*")
@ConfigGroups(value = KafkaOriginGroups.class)
@HideConfigs(value = {"kafkaConfigBean.dataFormatConfig.compression"})
@GenerateResourceBundle
public class KafkaDSource extends DClusterSourceOffsetCommitter implements ErrorListener {

  @ConfigDefBean
  public KafkaConfigBean kafkaConfigBean;

  private DelegatingKafkaSource delegatingKafkaSource;

  @Override
  protected Source createSource() {
    delegatingKafkaSource = new DelegatingKafkaSource(new StandaloneKafkaSourceFactory(kafkaConfigBean),
      new ClusterKafkaSourceFactory(kafkaConfigBean));
    return delegatingKafkaSource;
  }

  @Override
  public Source getSource() {
    return source != null?  delegatingKafkaSource.getSource(): null;
  }

  @Override
  public void errorNotification(Throwable throwable) {
    DelegatingKafkaSource delegatingSource = delegatingKafkaSource;
    if (delegatingSource != null) {
      Source source = delegatingSource.getSource();
      if (source instanceof ErrorListener) {
        ((ErrorListener)source).errorNotification(throwable);
      }
    }
  }

  @Override
  public void shutdown() {
    DelegatingKafkaSource delegatingSource = delegatingKafkaSource;
    if (delegatingSource != null) {
      Source source = delegatingSource.getSource();
      if (source instanceof ClusterSource) {
        ((ClusterSource)source).shutdown();
      }
    }
  }
}
