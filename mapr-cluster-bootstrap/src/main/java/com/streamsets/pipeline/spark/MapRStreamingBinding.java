/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import org.apache.spark.streaming.kafka.v09.KafkaUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;

public class MapRStreamingBinding extends AbstractStreamingBinding {

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
      String checkPointPath,
      String topic,
      String groupId,
      String autoOffsetValue,
      boolean isRunningInMesos
  ) {
    return new JavaStreamingContextFactoryImpl(
        conf,
        Utils.getMaprStreamsWaitTime(getProperties()),
        checkPointPath,
        topic,
        Utils.getPropertyOrEmptyString(getProperties(), AUTO_OFFSET_RESET),
        groupId,
        isRunningInMesos
    );
  }

  private static class JavaStreamingContextFactoryImpl implements JavaStreamingContextFactory {

    private final SparkConf sparkConf;
    private final long duration;
    private final String checkPointPath;
    private final String topic;
    private final boolean isRunningInMesos;
    private final String autoOffsetValue;
    private final String groupId;

    public JavaStreamingContextFactoryImpl(
        SparkConf sparkConf,
        long duration,
        String checkPointPath,
        String topic,
        String autoOffsetValue,
        String groupId,
        boolean isRunningInMesos
    ) {
      this.sparkConf = sparkConf;
      this.duration = duration;
      this.checkPointPath = checkPointPath;
      this.topic = topic;
      this.autoOffsetValue = autoOffsetValue;
      this.groupId = groupId;
      this.isRunningInMesos = isRunningInMesos;
    }

    @Override
    public JavaStreamingContext create() {
      JavaStreamingContext result = new JavaStreamingContext(sparkConf, new Duration(duration));
      result.checkpoint(checkPointPath);
      Map<String, String> props = new HashMap<>();
      if (!autoOffsetValue.isEmpty()) {
        props.put(AbstractStreamingBinding.AUTO_OFFSET_RESET, autoOffsetValue);
      }
      logMessage("topic list " + topic, isRunningInMesos);
      logMessage("Auto offset reset is set to " + autoOffsetValue, isRunningInMesos);
      props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
      props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
      props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
      JavaPairInputDStream<byte[], byte[]> dStream =
          KafkaUtils.createDirectStream(result, byte[].class, byte[].class,
              props, new HashSet<>(Arrays.asList(topic.split(","))));
      // This is not using foreach(Function<R, Void> foreachFunc) as its deprecated
      dStream.foreachRDD(new MapRSparkDriverFunction());
      return result;
    }
  }

}

