/**
 * Copyright 2015 StreamSets Inc.
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
package com.streamsets.pipeline.kafka.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.kafka.api.PartitionStrategy;
import com.streamsets.pipeline.kafka.api.ProducerFactorySettings;
import com.streamsets.pipeline.kafka.api.SdcKafkaProducer;
import com.streamsets.pipeline.kafka.api.SdcKafkaProducerFactory;
import com.streamsets.pipeline.lib.kafka.KafkaErrors;

import kafka.admin.AdminUtils;
import kafka.server.KafkaServer;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.security.JaasUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class TestKafkaProducer09 {

  @Test
  public void testKafkaProducer09Version() throws IOException {
    SdcKafkaProducer sdcKafkaProducer = createSdcKafkaProducer(TestUtil.getFreePort(), new HashMap<String, Object>());
    Assert.assertEquals(Kafka09Constants.KAFKA_VERSION, sdcKafkaProducer.getVersion());
  }

  @Test
  public void testKafkaProducer09Write() throws IOException, StageException {
    int zkConnectionTimeout = 6000;
    int zkSessionTimeout = 6000;

    EmbeddedZookeeper zookeeper = new EmbeddedZookeeper();
    String zkConnect = String.format("127.0.0.1:%d", zookeeper.port());
    ZkUtils zkUtils = ZkUtils.apply(
      zkConnect, zkSessionTimeout, zkConnectionTimeout,
      JaasUtils.isZkSecurityEnabled());

    int port = TestUtil.getFreePort();
    KafkaServer kafkaServer = TestUtil.createKafkaServer(port, zkConnect);

    final String topic = "TestKafkaProducer09_1";
    final String message = "Hello StreamSets";

    HashMap<String, Object> kafkaProducerConfigs = new HashMap<>();
    kafkaProducerConfigs.put("retries", 0);
    kafkaProducerConfigs.put("batch.size", 100);
    kafkaProducerConfigs.put("linger.ms", 0);

    SdcKafkaProducer sdcKafkaProducer = createSdcKafkaProducer(port, kafkaProducerConfigs);
    sdcKafkaProducer.init();
    sdcKafkaProducer.enqueueMessage(topic, message.getBytes(), "0");
    sdcKafkaProducer.write();

    verify(topic, 1, "localhost:" + port, message);

    AdminUtils.deleteTopic(
      zkUtils,
      topic
    );

    kafkaServer.shutdown();
    zookeeper.shutdown();
  }

  @Test
  public void testKafkaProducer09WriteFailsRecordTooLarge() throws IOException, StageException {
    int zkConnectionTimeout = 6000;
    int zkSessionTimeout = 6000;

    EmbeddedZookeeper zookeeper = new EmbeddedZookeeper();
    String zkConnect = String.format("127.0.0.1:%d", zookeeper.port());
    ZkUtils zkUtils = ZkUtils.apply(
      zkConnect, zkSessionTimeout, zkConnectionTimeout,
      JaasUtils.isZkSecurityEnabled());

    int port = TestUtil.getFreePort();
    KafkaServer kafkaServer = TestUtil.createKafkaServer(port, zkConnect);

    final String topic = "TestKafkaProducer09_1";

    HashMap<String, Object> kafkaProducerConfigs = new HashMap<>();
    kafkaProducerConfigs.put("retries", 0);
    kafkaProducerConfigs.put("batch.size", 100);
    kafkaProducerConfigs.put("linger.ms", 0);
    // Set the message size to 510 as "message.max.bytes" is set to 500
    final String message = StringUtils.leftPad("a", 510, "b");
    SdcKafkaProducer sdcKafkaProducer = createSdcKafkaProducer(port, kafkaProducerConfigs);
    sdcKafkaProducer.init();
    sdcKafkaProducer.enqueueMessage(topic, message.getBytes(), "0");
    try {
      sdcKafkaProducer.write();
      fail("Expected exception but didn't get any");
    } catch (StageException se) {
      assertEquals(KafkaErrors.KAFKA_69, se.getErrorCode());
    } catch (Exception e) {
      fail("Expected Stage Exception but got " + e);
    }

    AdminUtils.deleteTopic(
      zkUtils,
      topic
    );

    kafkaServer.shutdown();
    zookeeper.shutdown();
  }

  @Test
  public void testKafkaProducer09WriteException() throws IOException, StageException {
    int zkConnectionTimeout = 6000;
    int zkSessionTimeout = 6000;

    EmbeddedZookeeper zookeeper = new EmbeddedZookeeper();
    String zkConnect = String.format("127.0.0.1:%d", zookeeper.port());
    ZkUtils zkUtils = ZkUtils.apply(
      zkConnect, zkSessionTimeout, zkConnectionTimeout,
      JaasUtils.isZkSecurityEnabled());

    int port = TestUtil.getFreePort();
    KafkaServer kafkaServer = TestUtil.createKafkaServer(port, zkConnect);

    final String topic = "TestKafkaProducer09_1";
    final String message = "Hello StreamSets";

    HashMap<String, Object> kafkaProducerConfigs = new HashMap<>();
    kafkaProducerConfigs.put("retries", 0);
    kafkaProducerConfigs.put("batch.size", 100);
    kafkaProducerConfigs.put("linger.ms", 0);

    SdcKafkaProducer sdcKafkaProducer = createSdcKafkaProducer(port, kafkaProducerConfigs);
    sdcKafkaProducer.init();
    sdcKafkaProducer.enqueueMessage(topic, message.getBytes(), "0");
    sdcKafkaProducer.write();

    AdminUtils.deleteTopic(
      zkUtils,
      topic
    );

    kafkaServer.shutdown();

    // attempt writing when kafka server is down
    sdcKafkaProducer.enqueueMessage(topic, "Hello".getBytes(), "0");

    try {
      sdcKafkaProducer.write();
      Assert.fail("Expected KafkaConnectionException");
    } catch (StageException e) {
      Assert.assertEquals(KafkaErrors.KAFKA_50, e.getErrorCode());
    }

    zookeeper.shutdown();
  }

  private void verify(
      final String topic,
      final int numMessages,
      final String metadataBrokerList,
      String message
  ) {
    Properties props = new Properties();
    props.put("bootstrap.servers", metadataBrokerList);
    props.put("group.id", "test");
    props.put("enable.auto.commit", "false");
    props.put("auto.commit.interval.ms", "1000");
    props.put("auto.offset.reset", "earliest");
    props.put("session.timeout.ms", "30000");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Arrays.asList(topic));
    List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
    while (buffer.size() < 1) {
      ConsumerRecords<String, String> records = consumer.poll(1000);
      for (ConsumerRecord<String, String> record : records) {
        buffer.add(record);
      }
    }
    Assert.assertEquals(numMessages, buffer.size());
    Assert.assertEquals(message, buffer.get(0).value());
  }

  private SdcKafkaProducer createSdcKafkaProducer(int port, Map<String, Object> kafkaConfigs) {
    ProducerFactorySettings settings = new ProducerFactorySettings(
      kafkaConfigs,
      PartitionStrategy.DEFAULT,
      "localhost:" + port,
      DataFormat.JSON
    );
    SdcKafkaProducerFactory sdcKafkaProducerFactory = SdcKafkaProducerFactory.create(settings);
    return sdcKafkaProducerFactory.create();
  }
}
