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
import com.streamsets.pipeline.kafka.api.MessageAndOffset;
import com.streamsets.pipeline.kafka.api.MessageAndOffsetWithTimestamp;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;

import java.util.Collections;
import java.util.Map;

public class KafkaConsumer10 extends KafkaConsumer09 {
  public KafkaConsumer10(
      String bootStrapServers,
      String topic,
      String consumerGroup,
      Map<String, Object> kafkaConsumerConfigs,
      Source.Context context,
      int batchSize,
      boolean isTimestampEnabled,
      String kafkaAutoOffsetReset
  ) {
    super(bootStrapServers,
        topic,
        consumerGroup,
        kafkaConsumerConfigs,
        context,
        batchSize,
        isTimestampEnabled,
        kafkaAutoOffsetReset
    );
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
}
