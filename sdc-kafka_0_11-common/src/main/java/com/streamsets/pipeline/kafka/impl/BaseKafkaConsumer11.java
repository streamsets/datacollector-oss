/*
 * Copyright 2018 StreamSets Inc.
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
import com.streamsets.pipeline.kafka.api.MessageAndOffset;
import com.streamsets.pipeline.kafka.api.MessageAndOffsetWithTimestamp;
import com.streamsets.pipeline.lib.kafka.KafkaAutoOffsetReset;
import com.streamsets.pipeline.lib.kafka.KafkaErrors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public abstract class BaseKafkaConsumer11 extends KafkaConsumer09 {

  private static final Logger LOG = LoggerFactory.getLogger(BaseKafkaConsumer11.class);

  private final long timestampToSearchOffsets;
  private final Properties auxiliaryKafkaConsumerProperties;

  public BaseKafkaConsumer11(
      String bootStrapServers,
      String topic,
      String consumerGroup,
      Map<String, Object> kafkaConsumerConfigs,
      Source.Context context,
      int batchSize,
      boolean isTimestampsEnabled,
      String kafkaAutoOffsetReset,
      long timestampToSearchOffsets
  ) {
    super(
        bootStrapServers,
        topic,
        consumerGroup,
        kafkaConsumerConfigs,
        context,
        batchSize,
        isTimestampsEnabled,
        kafkaAutoOffsetReset
    );
    this.timestampToSearchOffsets = timestampToSearchOffsets;

    auxiliaryKafkaConsumerProperties = new Properties();
    // First set any and all properties set by user, this is super important as all security properties will be set here
    if (kafkaConsumerConfigs != null && !kafkaConsumerConfigs.isEmpty()) {
      auxiliaryKafkaConsumerProperties.putAll(kafkaConsumerConfigs);
    }
    // And finally finish setting the rest of what we need
    auxiliaryKafkaConsumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
    auxiliaryKafkaConsumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.NONE.name().toLowerCase());
    auxiliaryKafkaConsumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
    auxiliaryKafkaConsumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    auxiliaryKafkaConsumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    auxiliaryKafkaConsumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
  }

  @Override
  protected void subscribeConsumer() {
    kafkaConsumer.subscribe(Collections.singletonList(topic), this);
  }

  @Override
  MessageAndOffset getMessageAndOffset(ConsumerRecord message, boolean isEnabled) {
    MessageAndOffset messageAndOffset;
    if (message.timestampType() != TimestampType.NO_TIMESTAMP_TYPE && message.timestamp() > 0 && isEnabled) {
      messageAndOffset = new MessageAndOffsetWithTimestamp(
          message.key(),
          message.value(),
          message.offset(),
          message.partition(),
          message.timestamp(),
          message.timestampType().toString()
      );
    } else {
      messageAndOffset = new MessageAndOffset(message.key(), message.value(), message.offset(), message.partition());
    }
    return messageAndOffset;
  }

  @Override
  protected boolean isTimestampSupported() {
    return true;
  }

  @Override
  protected void validateAutoOffsetReset(List<Stage.ConfigIssue> issues) throws StageException {
    if (KafkaAutoOffsetReset.TIMESTAMP.name().equals(kafkaAutoOffsetReset)) {
      if (firstConnection()) {
        setOffsetsByTimestamp();
      }
      kafkaAutoOffsetReset = KafkaAutoOffsetReset.EARLIEST.name().toLowerCase();
    }
  }

  private boolean firstConnection() throws StageException {
    try (Consumer kafkaAuxiliaryConsumer = new KafkaConsumer(auxiliaryKafkaConsumerProperties)) {
      List<PartitionInfo> partitionInfoList = kafkaAuxiliaryConsumer.partitionsFor(topic);
      for (PartitionInfo partitionInfo : partitionInfoList) {
        TopicPartition topicPartition = new TopicPartition(topic, partitionInfo.partition());
        try {
          OffsetAndMetadata offsetAndMetadata = kafkaAuxiliaryConsumer.committed(topicPartition);
          if (offsetAndMetadata != null) {
            // Already defined offset for that partition
            LOG.debug("Offset defined for partition {}", topicPartition.partition());
            kafkaAuxiliaryConsumer.close();
            return false;
          }
        } catch (Exception ex) {
          // Could not obtain committed offset for corresponding partition
          LOG.error(KafkaErrors.KAFKA_30.getMessage(), ex.toString(), ex);
          throw new StageException(KafkaErrors.KAFKA_30, ex.toString(), ex);
        }
      }
    }

    // There was no offset already defined for any partition so it is the first connection
    return true;
  }

  private void setOffsetsByTimestamp() {
    try (KafkaConsumer kafkaAuxiliaryConsumer = new KafkaConsumer(auxiliaryKafkaConsumerProperties)) {
      // Build map of topics partitions and timestamp to use when searching offset for that partition (same timestamp
      // for all the partitions)
      List<PartitionInfo> partitionInfoList = kafkaAuxiliaryConsumer.partitionsFor(topic);

      if (partitionInfoList != null) {
        Map<TopicPartition, Long> partitionsAndTimestampMap = partitionInfoList.stream().map(e -> new TopicPartition(
            topic,
            e.partition()
        )).collect(Collectors.toMap(e -> e, (e) -> timestampToSearchOffsets));

        // Get Offsets by timestamp using previously built map and commit them to corresponding partition
        if (!partitionsAndTimestampMap.isEmpty()) {
          Map<TopicPartition, OffsetAndTimestamp> partitionsOffsets = kafkaAuxiliaryConsumer.offsetsForTimes(
              partitionsAndTimestampMap);
          if (partitionsOffsets != null && !partitionsOffsets.isEmpty()) {
            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = partitionsOffsets.entrySet().stream().filter(
                entry -> entry.getKey() != null && entry.getValue() != null).collect(
                Collectors.toMap(entry -> entry.getKey(), entry -> new OffsetAndMetadata(entry.getValue().offset())));

            if (!offsetsToCommit.isEmpty()) {
              kafkaAuxiliaryConsumer.commitSync(offsetsToCommit);
            }
          }
        }
      }
    }
  }

}
