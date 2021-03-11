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
import com.streamsets.pipeline.kafka.api.KafkaOriginGroups;
import com.streamsets.pipeline.kafka.api.MessageAndOffset;
import com.streamsets.pipeline.lib.kafka.KafkaAutoOffsetReset;
import com.streamsets.pipeline.lib.kafka.KafkaConstants;
import com.streamsets.pipeline.lib.kafka.KafkaErrors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaConsumer09 extends BaseKafkaConsumer09 {

  private static final boolean AUTO_COMMIT_ENABLED_DEFAULT = false;

  private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumer09.class);

  private final Stage.Context context;
  private final String bootStrapServers;
  private final String consumerGroup;
  private final Map<String, Object> kafkaConsumerConfigs;
  private final boolean isTimestampsEnabled;
  protected String kafkaAutoOffsetReset;

  public KafkaConsumer09(
      String bootStrapServers,
      String topic,
      String consumerGroup,
      Map<String, Object> kafkaConsumerConfigs,
      Source.Context context,
      int batchSize,
      boolean isTimestampsEnabled,
      String kafkaAutoOffsetReset
  ) {
    super(topic, context, batchSize);
    this.bootStrapServers = bootStrapServers;
    this.consumerGroup = consumerGroup;
    this.context = context;
    this.kafkaConsumerConfigs = kafkaConsumerConfigs;
    this.isTimestampsEnabled = isTimestampsEnabled;
    this.kafkaAutoOffsetReset = kafkaAutoOffsetReset;
  }

  @Override
  boolean isTimestampSupported() {
    return false;
  }

  @Override
  boolean isTimestampEnabled() {
    return isTimestampsEnabled;
  }

  @Override
  MessageAndOffset getMessageAndOffset(ConsumerRecord message, boolean isEnabled) {
    return new MessageAndOffset(message.key(), message.value(), message.offset(), message.partition());
  }

  @Override
  protected void validateAutoOffsetReset(List<Stage.ConfigIssue> issues) throws StageException {
    if(KafkaAutoOffsetReset.TIMESTAMP.name().equals(kafkaAutoOffsetReset)) {
      issues.add(context.createConfigIssue(KafkaOriginGroups.KAFKA.name(),
          KAFKA_CONFIG_BEAN_PREFIX + KAFKA_AUTO_OFFSET_RESET,
          KafkaErrors.KAFKA_76
      ));
    }
  }

  @Override
  protected void configureKafkaProperties(Properties props) {
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, AUTO_COMMIT_ENABLED_DEFAULT);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaAutoOffsetReset.toLowerCase());
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
      kafkaConsumerConfigs.remove(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
      kafkaConsumerConfigs.remove(ConsumerConfig.GROUP_ID_CONFIG);
      kafkaConsumerConfigs.remove(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
      kafkaConsumerConfigs.remove(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);

      for (Map.Entry<String, Object> producerConfig : kafkaConsumerConfigs.entrySet()) {
        props.put(producerConfig.getKey(), producerConfig.getValue());
      }
    }
  }

}
