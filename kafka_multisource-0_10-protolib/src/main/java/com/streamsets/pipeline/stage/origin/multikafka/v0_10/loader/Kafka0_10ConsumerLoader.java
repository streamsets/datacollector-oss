/**
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
package com.streamsets.pipeline.stage.origin.multikafka.v0_10.loader;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.kafka.KafkaAutoOffsetReset;
import com.streamsets.pipeline.lib.kafka.KafkaErrors;
import com.streamsets.pipeline.stage.origin.multikafka.MultiSdcKafkaConsumer;
import com.streamsets.pipeline.stage.origin.multikafka.loader.KafkaConsumerLoader;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class Kafka0_10ConsumerLoader extends KafkaConsumerLoader {

  private static final Logger LOG = LoggerFactory.getLogger(Kafka0_10ConsumerLoader.class);

  private Properties auxiliaryKafkaConsumerProperties;
  private long timestampToSearchOffsets;

  @Override
  protected void validateConsumerConfiguration(
      Properties properties,
      Stage.Context context,
      KafkaAutoOffsetReset kafkaAutoOffsetReset,
      long timestampToSearchOffsets,
      List<String> topicsList
  ) throws StageException {
    if (!context.isPreview()) {
      if (kafkaAutoOffsetReset == KafkaAutoOffsetReset.TIMESTAMP) {
        // Update private properties
        this.timestampToSearchOffsets = timestampToSearchOffsets;
        // Get Auxiliary Kafka Consumer properties
        auxiliaryKafkaConsumerProperties = (Properties) properties.clone();
        auxiliaryKafkaConsumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
            OffsetResetStrategy.NONE.name().toLowerCase());
        auxiliaryKafkaConsumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // Check if it is the first time we connect to this kafka instance with these topics and consumer group
        try (KafkaConsumer kafkaAuxiliaryConsumer = new KafkaConsumer(auxiliaryKafkaConsumerProperties)) {
          for (String topic : topicsList) {
            if (firstConnection(topic, kafkaAuxiliaryConsumer)) {
              setOffsetsByTimestamp(topic, kafkaAuxiliaryConsumer);
            }
          }
        }

        // Set offset strategy to earliest after setting offsets by timestamp if possible
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KafkaAutoOffsetReset.EARLIEST.getLabel().toLowerCase());
      } else {
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaAutoOffsetReset.name().toLowerCase());
      }
    }
  }

  @Override
  protected MultiSdcKafkaConsumer createConsumerInternal(Properties properties) {
    return new WrapperKafkaConsumer(new KafkaConsumer(properties));
  }

  private boolean firstConnection(String topic, KafkaConsumer kafkaAuxiliaryConsumer) throws StageException {
    LOG.debug("Checking first connection for Topic {}", topic);
    if (topic != null && !topic.isEmpty()) {
      List<PartitionInfo> partitionInfoList = kafkaAuxiliaryConsumer.partitionsFor(topic);
      for (PartitionInfo partitionInfo : partitionInfoList) {
        if (partitionInfo != null) {
          TopicPartition topicPartition = new TopicPartition(topic, partitionInfo.partition());
          try {
            OffsetAndMetadata offsetAndMetadata = kafkaAuxiliaryConsumer.committed(topicPartition);
            if (offsetAndMetadata != null) {
              // Already defined offset for that partition
              LOG.debug("Offset defined for Topic {} , partition {}", topic, topicPartition.partition());
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
    }

    // There was no offset already defined for any partition so it is the first connection
    return true;
  }

  private void setOffsetsByTimestamp(String topic, KafkaConsumer kafkaAuxiliaryConsumer) {
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

  /**
   * Wrapper around the KafkaConsumer that will simply delegate all the important methods.
   */
  private class WrapperKafkaConsumer implements MultiSdcKafkaConsumer {

    private Consumer delegate;

    public WrapperKafkaConsumer(Consumer consumer) {
      this.delegate = consumer;
    }

    @Override
    public void subscribe(List topics) {
      delegate.subscribe(topics);
    }

    @Override
    public ConsumerRecords poll(long timeout) {
      return delegate.poll(timeout);
    }

    @Override
    public void unsubscribe() {
      delegate.unsubscribe();
    }

    @Override
    public void close() {
      delegate.close();
    }

    @Override
    public void commitSync(Map offsetsMap) { delegate.commitSync(offsetsMap); }
  }
}
