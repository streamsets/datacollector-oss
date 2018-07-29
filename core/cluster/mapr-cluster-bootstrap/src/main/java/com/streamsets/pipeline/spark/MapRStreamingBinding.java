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
package com.streamsets.pipeline.spark;

import com.streamsets.pipeline.Utils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka09.KafkaUtils;
import org.apache.spark.streaming.kafka09.LocationStrategies;
import org.apache.spark.streaming.kafka09.ConsumerStrategies;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class MapRStreamingBinding extends SparkStreamingBinding {

  public MapRStreamingBinding(Properties properties) {
    super(properties);
  }

  @Override
  public String getTopic() {
    return Utils.getMaprStreamsTopic(getProperties());
  }

  @Override
  protected String getConsumerGroup() {
    return Utils.getMaprStreamsConsumerGroup(getProperties());
  }

  @Override
  public JavaStreamingContextFactory getStreamingContextFactory(
      SparkConf conf,
      String topic,
      String groupId,
      String autoOffsetValue,
      boolean isRunningInMesos
  ) {
    return new MapRJavaStreamingContextFactoryImpl(
        conf,
        Utils.getMaprStreamsWaitTime(getProperties()),
        topic,
        Utils.getNumberOfPartitions(getProperties()),
        Utils.getPropertyOrEmptyString(getProperties(), AUTO_OFFSET_RESET),
        groupId,
        isRunningInMesos,
        Utils.getMaprStreamsRateLimit(getProperties()),
        Utils.getExtraKafkaConfigs(getProperties())
    );
  }

  public class MapRJavaStreamingContextFactoryImpl extends JavaStreamingContextFactoryImpl {

    public MapRJavaStreamingContextFactoryImpl(
        SparkConf sparkConf,
        long duration,
        String topic,
        int numberOfPartitions,
        String autoOffsetValue,
        String groupId,
        boolean isRunningInMesos,
        int maxRatePerPartition,
        Map<String, String> extraKafkaConfigs
    ) {
      super(sparkConf, duration, null, topic, numberOfPartitions, groupId, autoOffsetValue, isRunningInMesos, maxRatePerPartition, extraKafkaConfigs);
    }

    @Override
    public JavaStreamingContext createDStream(JavaStreamingContext result, Map<String, Object> props) {
      List<String> topics = ImmutableList.of(topic);
      if (!autoOffsetValue.isEmpty()) {
        props.put(SparkStreamingBinding.AUTO_OFFSET_RESET, autoOffsetValue);
      }
      props.putAll(extraKafkaConfigs);

      JavaInputDStream<ConsumerRecord<byte[], byte[]>> stream;

      if (offsetHelper.isSDCCheckPointing()) {
        Map<TopicPartition, Long> fromOffsets = MaprStreamsOffsetManagerImpl.get().getOffsetForDStream(topic, numberOfPartitions);
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
      Driver$.MODULE$.foreach(stream.dstream(), MaprStreamsOffsetManagerImpl.get());
      return result;
    }
  }
}
