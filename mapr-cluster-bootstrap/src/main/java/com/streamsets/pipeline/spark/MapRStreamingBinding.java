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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.v09.KafkaUtils;
import scala.Tuple2;
import scala.reflect.ClassTag;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;

public class MapRStreamingBinding extends AbstractStreamingBinding {

  private static class MessageHandlerFunction implements Function<ConsumerRecord<byte[], byte[]>, Tuple2> {
    @Override
    public Tuple2<byte[], byte[]> call(ConsumerRecord<byte[], byte[]> v1) throws Exception {
      return new Tuple2<>(v1.key(), v1.value());
    }
  }

  private static final Function<ConsumerRecord<byte[], byte[]>, Tuple2> MESSAGE_HANDLER_FUNCTION
      = new MessageHandlerFunction();


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
  protected JavaStreamingContextFactory getStreamingContextFactory(
      SparkConf conf,
      String topic,
      String groupId,
      String autoOffsetValue,
      boolean isRunningInMesos
  ) {
    return new JavaStreamingContextFactoryImpl(
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

  private static class JavaStreamingContextFactoryImpl implements JavaStreamingContextFactory {

    private final SparkConf sparkConf;
    private final long duration;
    private final String topic;
    private final int numberOfPartitions;
    private final boolean isRunningInMesos;
    private final String autoOffsetValue;
    private final String groupId;
    private final int maxRatePerPartition;
    private final Map<String, String> extraKafkaConfigs;

    public JavaStreamingContextFactoryImpl(
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
      this.sparkConf = sparkConf;
      this.duration = duration;
      this.topic = topic;
      this.numberOfPartitions = numberOfPartitions;
      this.autoOffsetValue = autoOffsetValue;
      this.groupId = groupId;
      this.isRunningInMesos = isRunningInMesos;
      this.maxRatePerPartition = maxRatePerPartition;
      this.extraKafkaConfigs = extraKafkaConfigs;

    }

    private Map<TopicPartition, Long> getOffsetForDStream(Map<Integer, Long> partitionToOffset) {
      Map<TopicPartition, Long> offsetForDStream = new HashMap<>();
      for (Map.Entry<Integer, Long> partitionAndOffset : partitionToOffset.entrySet()) {
        offsetForDStream.put(new TopicPartition(topic, partitionAndOffset.getKey()), partitionAndOffset.getValue());
      }
      return offsetForDStream;
    }


    @Override
    @SuppressWarnings("unchecked")
    public JavaStreamingContext create() {
      sparkConf.set("spark.streaming.kafka.maxRatePerPartition", String.valueOf(maxRatePerPartition));
      JavaStreamingContext result = new JavaStreamingContext(sparkConf, new Duration(duration));
      Map<String, String> props = new HashMap<>();
      if (!autoOffsetValue.isEmpty()) {
        props.put(AbstractStreamingBinding.AUTO_OFFSET_RESET, autoOffsetValue);
      }
      logMessage("topic list " + topic, isRunningInMesos);
      logMessage("Auto offset reset is set to " + autoOffsetValue, isRunningInMesos);
      props.putAll(extraKafkaConfigs);
      for (Map.Entry<String, String> map : props.entrySet()) {
        logMessage(Utils.format("Adding extra kafka config, {}:{}", map.getKey(), map.getValue()), isRunningInMesos);
      }
      props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
      props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
      props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
      JavaPairInputDStream<byte[], byte[]> dStream;
      if (offsetHelper.isSDCCheckPointing()) {
        JavaInputDStream stream =
            KafkaUtils.createDirectStream(
                result,
                byte[].class,
                byte[].class,
                Tuple2.class,
                props,
                MaprStreamsOffsetManagerImpl.get().getOffsetForDStream(topic, numberOfPartitions),
                MESSAGE_HANDLER_FUNCTION
            );
        ClassTag<byte[]> byteClassTag = scala.reflect.ClassTag$.MODULE$.apply(byte[].class);
        dStream = JavaPairInputDStream.fromInputDStream(stream.inputDStream(), byteClassTag, byteClassTag);
      } else {
        dStream =
            KafkaUtils.createDirectStream(result, byte[].class, byte[].class,
                props, new HashSet<>(Arrays.asList(topic.split(","))));
      }
      Driver$.MODULE$.foreach(dStream.dstream(), MaprStreamsOffsetManagerImpl.get());
      return result;
    }
  }

}

