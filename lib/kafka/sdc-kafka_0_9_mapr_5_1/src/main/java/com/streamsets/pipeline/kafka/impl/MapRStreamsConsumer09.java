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
package com.streamsets.pipeline.kafka.impl;

import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.kafka.KafkaConstants;
import com.streamsets.pipeline.lib.kafka.KafkaErrors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class MapRStreamsConsumer09 extends BaseKafkaConsumer09 {

  private static final boolean AUTO_COMMIT_ENABLED_DEFAULT = false;
  private static final String KEY_DESERIALIZER_DEFAULT = "org.apache.kafka.common.serialization.StringDeserializer";
  private static final String VALUE_DESERIALIZER_DEFAULT = "org.apache.kafka.common.serialization.ByteArrayDeserializer";

  private final Stage.Context context;
  private final String consumerGroup;
  private final Map<String, Object> kafkaConsumerConfigs;

  private static final Logger LOG = LoggerFactory.getLogger(MapRStreamsConsumer09.class);

  public MapRStreamsConsumer09(
    String topic,
    String consumerGroup,
    Map<String, Object> kafkaConsumerConfigs,
    Source.Context context,
    int batchSize
  ) {
    super(topic, context, batchSize);
    this.consumerGroup = consumerGroup;
    this.context = context;
    this.kafkaConsumerConfigs = kafkaConsumerConfigs;
  }

  @Override
  protected void configureKafkaProperties(Properties props) {

    // MapR Streams supports following Kafka consumer configs
    // 1. key.deserializer
    // 2. value.deserializer
    // 3. fetch.min.bytes
    // 4. group.id
    // 5. max.partition.fetch.bytes
    // 6. auto.offset.reset
    // 7. enable.auto.commit
    // 8. auto.commit.interval.ms
    // 9. client.id
    // 10. metadata.max.age.ms

    // MapR Streams related proeprties
    // 1. streams.consumer.default.stream
    // 2. streams.rpc.timeout.ms

    props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, AUTO_COMMIT_ENABLED_DEFAULT);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KEY_DESERIALIZER_DEFAULT);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, VALUE_DESERIALIZER_DEFAULT);

    if (this.context.isPreview()) {
      props.setProperty(KafkaConstants.AUTO_OFFSET_RESET_CONFIG, KafkaConstants.AUTO_OFFSET_RESET_PREVIEW_VALUE);
    }

    addUserConfiguredProperties(props);
  }

  @Override
  protected void handlePartitionsForException(
      List<Stage.ConfigIssue> issues,
      Stage.Context context,
      KafkaException e
  ) {
    issues.add(context.createConfigIssue(null, null, KafkaErrors.KAFKA_10, e.toString()));
  }

  @Override
  protected StageException createReadException(Exception e) {
    LOG.error(KafkaErrors.KAFKA_29.getMessage(), e.toString(), e);
    return new StageException(KafkaErrors.KAFKA_29, e.toString(), e);
  }

  private void addUserConfiguredProperties(Properties props) {
    //The following options, if specified, are ignored :
    if (kafkaConsumerConfigs != null && !kafkaConsumerConfigs.isEmpty()) {
      kafkaConsumerConfigs.remove(ConsumerConfig.GROUP_ID_CONFIG);
      kafkaConsumerConfigs.remove(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
      kafkaConsumerConfigs.remove(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
      kafkaConsumerConfigs.remove(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);

      for (Map.Entry<String, Object> producerConfig : kafkaConsumerConfigs.entrySet()) {
        props.put(producerConfig.getKey(), producerConfig.getValue());
      }
    }
  }

}
