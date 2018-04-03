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

import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.Utils;
import kafka.message.MessageAndMetadata;
import kafka.serializer.DefaultDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;
import scala.reflect.ClassTag;

import java.util.Map;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;

public class Kafka08SparkStreamingBinding extends SparkStreamingBinding {

  private static class MessageHandlerFunction implements Function<MessageAndMetadata<byte[], byte[]>, Tuple2<byte[], byte[]>> {
    @Override
    public Tuple2<byte[], byte[]> call(MessageAndMetadata<byte[], byte[]> v1) throws Exception {
      return new Tuple2<>(v1.key(), v1.message());
    }
  }

  public static Function<MessageAndMetadata<byte[], byte[]>, Tuple2<byte[], byte[]>> MESSAGE_HANDLER_FUNCTION  = new MessageHandlerFunction();

  public Kafka08SparkStreamingBinding(Properties properties) {
    super(properties);
  }

  @Override
  protected String getTopic() {
    return Utils.getKafkaTopic(getProperties());
  }

  @Override
  protected String getConsumerGroup() {
    return Utils.getKafkaConsumerGroup(getProperties());
  }

  @Override
  public JavaStreamingContextFactory getStreamingContextFactory(
      SparkConf conf,
      String topic,
      String groupId,
      String autoOffsetValue,
      boolean isRunningInMesos
  ) {
      return new JavaStreamingContextFactoryImplKafka08(
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

  protected class JavaStreamingContextFactoryImplKafka08 extends JavaStreamingContextFactoryImpl {

    JavaStreamingContextFactoryImplKafka08(
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

      Map<String, String> kafkaProps = props.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, v -> (String)v.getValue()));
      JavaPairInputDStream<byte[], byte[]> dStream;
      if (offsetHelper.isSDCCheckPointing()) {
        JavaInputDStream<Tuple2<byte[], byte[]>> stream =
            KafkaUtils.createDirectStream(
                result,
                byte[].class,
                byte[].class,
                DefaultDecoder.class,
                DefaultDecoder.class,
                (Class<Tuple2<byte[], byte[]>>) ((Class)(Tuple2.class)),
                kafkaProps,
                KafkaOffsetManagerImpl.get().getOffsetForDStream(topic, numberOfPartitions),
                MESSAGE_HANDLER_FUNCTION
            );
        ClassTag<byte[]> byteClassTag = scala.reflect.ClassTag$.MODULE$.apply(byte[].class);
        dStream = JavaPairInputDStream.fromInputDStream(stream.inputDStream(), byteClassTag, byteClassTag);
      } else {
        dStream =
            KafkaUtils.createDirectStream(result, byte[].class, byte[].class, DefaultDecoder.class, DefaultDecoder.class,
                kafkaProps, ImmutableSet.copyOf(Arrays.asList(topic.split(","))));
      }
      Driver$.MODULE$.foreachTuple(dStream.dstream(), KafkaOffsetManagerImpl.get());
      return result;
    }


  }
}
