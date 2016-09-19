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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.spark;

import com.streamsets.pipeline.Utils;
import kafka.serializer.DefaultDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import org.apache.spark.streaming.kafka.KafkaUtils;

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
  private static final String GROUP_ID_KEY = "group.id";
  static {
    KAFKA_POST_0_9_TO_PRE_0_9_CONFIG_CHANGES.put(KAFKA_AUTO_RESET_EARLIEST, KAFKA_AUTO_RESET_SMALLEST);
    KAFKA_POST_0_9_TO_PRE_0_9_CONFIG_CHANGES.put(KAFKA_AUTO_RESET_LATEST, KAFKA_AUTO_RESET_LARGEST);
  }

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
      String checkPointPath,
      String topic,
      String groupId,
      String autoOffsetValue,
      boolean isRunningInMesos
  ) {
    JavaStreamingContextFactory javaStreamingContextFactory = new JavaStreamingContextFactoryImpl(
        conf,
        Utils.getKafkaMaxWaitTime(getProperties()),
        checkPointPath.toString(),
        Utils.getKafkaMetadataBrokerList(getProperties()),
        topic,
        groupId,
        autoOffsetValue,
        isRunningInMesos
    );
    return javaStreamingContextFactory;
  }

  private static class JavaStreamingContextFactoryImpl implements JavaStreamingContextFactory {
    private final SparkConf sparkConf;
    private final long duration;
    private final String checkPointPath;
    private final String metaDataBrokerList;
    private final String topic;
    private final String groupId;
    private final boolean isRunningInMesos;
    private String autoOffsetValue;

    public JavaStreamingContextFactoryImpl(
        SparkConf sparkConf,
        long duration,
        String checkPointPath,
        String metaDataBrokerList,
        String topic,
        String groupId,
        String autoOffsetValue,
        boolean isRunningInMesos
    ) {
      this.sparkConf = sparkConf;
      this.duration = duration;
      this.checkPointPath = checkPointPath;
      this.metaDataBrokerList = metaDataBrokerList;
      this.topic = topic;
      this.autoOffsetValue = autoOffsetValue;
      this.isRunningInMesos = isRunningInMesos;
      this.groupId = groupId;
    }

    @Override
    public JavaStreamingContext create() {
      JavaStreamingContext result = new JavaStreamingContext(sparkConf, new Duration(duration));
      result.checkpoint(checkPointPath);
      Map<String, String> props = new HashMap<String, String>();
      props.put("metadata.broker.list", metaDataBrokerList);
      props.put(GROUP_ID_KEY, groupId);
      if (!autoOffsetValue.isEmpty()) {
        autoOffsetValue = getConfigurableAutoOffsetResetIfNonEmpty(autoOffsetValue);
        props.put(AUTO_OFFSET_RESET, autoOffsetValue);
      }
      logMessage("Meta data broker list " + metaDataBrokerList, isRunningInMesos);
      logMessage("Topic is " + topic, isRunningInMesos);
      logMessage("Auto offset reset is set to " + autoOffsetValue, isRunningInMesos);
      JavaPairInputDStream<byte[], byte[]> dStream =
          KafkaUtils.createDirectStream(result, byte[].class, byte[].class, DefaultDecoder.class, DefaultDecoder.class,
              props, new HashSet<String>(Arrays.asList(topic.split(","))));
      dStream.foreachRDD(new SparkDriverFunction());
      return result;
    }
  }
}
