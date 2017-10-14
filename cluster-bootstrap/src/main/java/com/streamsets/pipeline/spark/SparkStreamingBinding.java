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
import kafka.message.MessageAndMetadata;
import kafka.serializer.DefaultDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;
import scala.reflect.ClassTag;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;

public class SparkStreamingBinding extends AbstractStreamingBinding {
  //https://issues.streamsets.com/browse/SDC-3961
  //>= 0.9
  private static final String KAFKA_AUTO_RESET_EARLIEST = "earliest";
  private static final String KAFKA_AUTO_RESET_LATEST = "latest";

  //< 0.9
  private static final String KAFKA_AUTO_RESET_SMALLEST = "smallest";
  private static final String KAFKA_AUTO_RESET_LARGEST = "largest";

  private static final Map<String, String> KAFKA_POST_0_9_TO_PRE_0_9_CONFIG_CHANGES = new HashMap<>();
  public static final String GROUP_ID_KEY = "group.id";

  static {
    KAFKA_POST_0_9_TO_PRE_0_9_CONFIG_CHANGES.put(KAFKA_AUTO_RESET_EARLIEST, KAFKA_AUTO_RESET_SMALLEST);
    KAFKA_POST_0_9_TO_PRE_0_9_CONFIG_CHANGES.put(KAFKA_AUTO_RESET_LATEST, KAFKA_AUTO_RESET_LARGEST);
  }

  private static class MessageHandlerFunction implements Function<MessageAndMetadata<byte[], byte[]>, Tuple2<byte[], byte[]>> {
    @Override
    public Tuple2<byte[], byte[]> call(MessageAndMetadata<byte[], byte[]> v1) throws Exception {
      return new Tuple2<>(v1.key(), v1.message());
    }
  }

  public static Function<MessageAndMetadata<byte[], byte[]>, Tuple2<byte[], byte[]>> MESSAGE_HANDLER_FUNCTION  = new MessageHandlerFunction();

  public SparkStreamingBinding(Properties properties) {
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

  static String getConfigurableAutoOffsetResetIfNonEmpty(String autoOffsetValue) {
    String configurableAutoOffsetResetValue = KAFKA_POST_0_9_TO_PRE_0_9_CONFIG_CHANGES.get(autoOffsetValue);
    return (configurableAutoOffsetResetValue == null)? autoOffsetValue : configurableAutoOffsetResetValue;
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

  public static class JavaStreamingContextFactoryImpl implements JavaStreamingContextFactory {
    private final SparkConf sparkConf;
    private final long duration;
    private final String metaDataBrokerList;
    private final String topic;
    private final int numberOfPartitions;
    private final String groupId;
    private final boolean isRunningInMesos;
    private final int maxRatePerPartition;
    private final Map<String, String> extraKafkaConfigs;
    private String autoOffsetValue;

    public JavaStreamingContextFactoryImpl(
        SparkConf sparkConf,
        long duration,
        String metaDataBrokerList,
        String topic,
        int numberOfPartitions,
        String groupId,
        String autoOffsetValue,
        boolean isRunningInMesos,
        int maxRatePerPartition,
        Map<String, String> extraKafkaConfigs
    ) {
      this.sparkConf = sparkConf;
      this.duration = duration;
      this.metaDataBrokerList = metaDataBrokerList;
      this.topic = topic;
      this.numberOfPartitions = numberOfPartitions;
      this.autoOffsetValue = autoOffsetValue;
      this.isRunningInMesos = isRunningInMesos;
      this.groupId = groupId;
      this.maxRatePerPartition = maxRatePerPartition;
      this.extraKafkaConfigs = extraKafkaConfigs;
    }

    @Override
    @SuppressWarnings("unchecked")
    public JavaStreamingContext create() {
      sparkConf.set("spark.streaming.kafka.maxRatePerPartition", String.valueOf(maxRatePerPartition));
      JavaStreamingContext result = new JavaStreamingContext(sparkConf, new Duration(duration));
      Map<String, String> props = new HashMap<>();
      props.putAll(extraKafkaConfigs);
      for (Map.Entry<String, String> map : props.entrySet()) {
        logMessage(Utils.format("Adding extra kafka config, {}:{}", map.getKey(), map.getValue()), isRunningInMesos);
      }
      props.put("metadata.broker.list", metaDataBrokerList);
      props.put(GROUP_ID_KEY, groupId);
      if (!autoOffsetValue.isEmpty()) {
        autoOffsetValue = getConfigurableAutoOffsetResetIfNonEmpty(autoOffsetValue);
        props.put(AUTO_OFFSET_RESET, autoOffsetValue);
      }
      logMessage("Meta data broker list " + metaDataBrokerList, isRunningInMesos);
      logMessage("Topic is " + topic, isRunningInMesos);
      logMessage("Auto offset reset is set to " + autoOffsetValue, isRunningInMesos);
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
                props,
                KafkaOffsetManagerImpl.get().getOffsetForDStream(topic, numberOfPartitions),
                MESSAGE_HANDLER_FUNCTION
            );
        ClassTag<byte[]> byteClassTag = scala.reflect.ClassTag$.MODULE$.apply(byte[].class);
        dStream = JavaPairInputDStream.fromInputDStream(stream.inputDStream(), byteClassTag, byteClassTag);
      } else {
        dStream =
            KafkaUtils.createDirectStream(result, byte[].class, byte[].class, DefaultDecoder.class, DefaultDecoder.class,
                props, new HashSet<>(Arrays.asList(topic.split(","))));
      }
      Driver$.MODULE$.foreach(dStream.dstream(), KafkaOffsetManagerImpl.get());
      return result;
    }
  }
}
