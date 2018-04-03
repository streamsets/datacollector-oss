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
package com.streamsets.pipeline.spark;

import com.streamsets.pipeline.Utils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Properties;

public class Kafka010SparkStreamingBinding extends SparkStreamingBinding {

  public Kafka010SparkStreamingBinding(Properties properties) {
    super(properties);
  }

  @Override
  public JavaStreamingContextFactory getStreamingContextFactory(
      SparkConf conf,
      String topic,
      String groupId,
      String autoOffsetValue,
      boolean isRunningInMesos
  ) {
    return new JavaStreamingContextFactoryImplKafka010(
        conf,
        Utils.getKafkaMaxWaitTime(getProperties()),
        Utils.getKafkaMetadataBrokerList(getProperties()),
        topic,
        Utils.getNumberOfPartitions(getProperties()),
        groupId,
        autoOffsetValue,
        isRunningInMesos,
        Utils.getKafkaPartitionRateLimit(getProperties()),
        Utils.getExtraKafkaConfigs(getProperties())
    );
  }

  protected class JavaStreamingContextFactoryImplKafka010 extends JavaStreamingContextFactoryImpl {

    JavaStreamingContextFactoryImplKafka010(
        SparkConf sparkConf,
        long duration,
        String metaDataBrokerList,
        String topic,
        int numberOfPartitions,
        String groupId,
        String autoOffsetValue,
        boolean isRunningInMesos,
        int maxRatePerPartition,
        Map<String, String> extraKafkaConfigs) {
      super(sparkConf, duration, metaDataBrokerList, topic, numberOfPartitions, groupId, autoOffsetValue, isRunningInMesos, maxRatePerPartition, extraKafkaConfigs);
    }

    @Override
    public JavaStreamingContext createDStream(JavaStreamingContext result, Map<String, Object> props) {
      props.put("bootstrap.servers", metaDataBrokerList);
      if (!autoOffsetValue.isEmpty()) {
        autoOffsetValue = getConfigurableAutoOffsetResetIfNonEmpty(autoOffsetValue);
        props.put(AUTO_OFFSET_RESET, autoOffsetValue);
      }
      props.putAll(extraKafkaConfigs);

      List<String> topics = ImmutableList.of(topic);
      JavaInputDStream<ConsumerRecord<byte[], byte[]>> stream;

      if (offsetHelper.isSDCCheckPointing()) {
        Map<TopicPartition, Long> fromOffsets = KafkaOffsetManagerImpl.get().getOffsetForDStream(topic, numberOfPartitions);
        stream =
            KafkaUtils.createDirectStream(
                result,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<byte[], byte[]>Assign(new ArrayList<TopicPartition>(fromOffsets.keySet()), props, fromOffsets)
            );
      } else {
        stream  = KafkaUtils.createDirectStream(
            result,
            LocationStrategies.PreferConsistent(),
            ConsumerStrategies.<byte[], byte[]>Subscribe(topics, props)
        );

      }
      Driver$.MODULE$.foreach(stream.dstream(), KafkaOffsetManagerImpl.get());
      return result;
    }
  }
}
