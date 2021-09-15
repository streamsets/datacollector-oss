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
package com.streamsets.pipeline.stage.origin.multikafka;

import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;

/**
 * This is very thin wrapper on top of KafkaConsumer (Kafka native class) that is abstracting method calls that
 * the MultiKafkaSource needs. The wrapper should be very thin - most likely just delegating calls, any advanced
 * logic needs to be inside the source itself.
 */
public interface MultiSdcKafkaConsumer<K, V> {

  void subscribe(List<String> topics);

  void subscribe(Pattern var1);

  ConsumerRecords<K, V> poll(long timeout);

  void unsubscribe();

  void close();

  void commitSync(Map<TopicPartition, OffsetAndMetadata> offsetsMap);

}
