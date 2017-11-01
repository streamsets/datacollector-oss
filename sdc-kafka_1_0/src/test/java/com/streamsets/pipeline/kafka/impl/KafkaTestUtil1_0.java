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

import com.google.common.util.concurrent.Uninterruptibles;
import com.streamsets.pipeline.kafka.common.SdcKafkaTestUtil;
import com.streamsets.testing.NetworkUtils;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.server.RunningAsBroker;
import kafka.utils.TestUtils;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class KafkaTestUtil1_0 extends SdcKafkaTestUtil {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaTestUtil1_0.class);
  private static final long TIME_OUT = 5000;

  private static List<KafkaServer> kafkaServers;
  private static Map<String, String> kafkaProps;
  private static String metadataBrokerURI;
  private static String zkConnect;
  private static EmbeddedZookeeper zookeeper;
  private static ZkUtils zkUtils;

  public KafkaTestUtil1_0() {
  }

  public String getMetadataBrokerURI() {
    return metadataBrokerURI;
  }

  public String getZkConnect() {
    return zkConnect;
  }

  public List<KafkaServer> getKafkaServers() {
    return kafkaServers;
  }

  public void startZookeeper() {
    int zkConnectionTimeout = 6000;
    int zkSessionTimeout = 6000;
    zookeeper = new EmbeddedZookeeper();
    zkConnect = String.format("127.0.0.1:%d", zookeeper.port());
    zkUtils = ZkUtils.apply(
        zkConnect, zkSessionTimeout, zkConnectionTimeout,
        JaasUtils.isZkSecurityEnabled());
  }

  public void startKafkaBrokers(int numberOfBrokers) throws IOException {
    kafkaServers = new ArrayList<>(numberOfBrokers);
    kafkaProps = new HashMap<>();
    // setup Broker
    StringBuilder sb = new StringBuilder();
    int brokerId = new Random().nextInt(100);
    if(brokerId < 0) {
      brokerId = -brokerId;
    }
    for (int i = 0; i < numberOfBrokers; i++) {
      int port = NetworkUtils.getRandomPort();
      KafkaServer kafkaServer = createKafkaServer(++brokerId, port, zkConnect);
      long start = System.currentTimeMillis();
      while (System.currentTimeMillis() < start + 5000L &&
          kafkaServer.brokerState().currentState() != RunningAsBroker.state()) {
        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
      }
      kafkaServers.add(kafkaServer);
      sb.append("localhost:" + port).append(",");
    }
    metadataBrokerURI = sb.deleteCharAt(sb.length() - 1).toString();
    LOG.info("Setting metadataBrokerList and auto.offset.reset for test case");
    kafkaProps.put("auto.offset.reset", "smallest");
  }

  public void createTopic(String topic, int partitions, int replicationFactor) {
    // new param for kafka 0.10
    RackAwareMode rackAwareMode = RackAwareMode.Enforced$.MODULE$;
    AdminUtils.createTopic(zkUtils, topic, partitions, replicationFactor, new Properties(), rackAwareMode);
  }

  public void shutdown() {
    for (KafkaServer kafkaServer : kafkaServers) {
      kafkaServer.shutdown();
    }
    zookeeper.shutdown();
    metadataBrokerURI = null;
    zkConnect = null;
    kafkaProps = null;
  }

  public static KafkaServer createKafkaServer(int brokerId, int port, String zkConnect) {
    final Option<File> noFile = scala.Option.apply(null);
    final Option<SecurityProtocol> noInterBrokerSecurityProtocol = scala.Option.apply(null);
    // new params for kafka 0.10.0
    Option<Properties> saslProperties = scala.Option.apply(null);
    Option rack = scala.Option.apply(RackAwareMode.Enforced$.MODULE$.toString());
    Properties props = TestUtils.createBrokerConfig(
        brokerId,
        zkConnect,
        false,
        false,
        port,
        noInterBrokerSecurityProtocol,
        noFile,
        saslProperties,
        true,
        false,
        TestUtils.RandomPort(),
        false,
        TestUtils.RandomPort(),
        false,
        TestUtils.RandomPort(),
        rack,
        1
    );
    props.setProperty("auto.create.topics.enable", "true");
    props.setProperty("num.partitions", "1");
    props.setProperty("zookeeper.connect", zkConnect);
    KafkaConfig config = new KafkaConfig(props);
    return TestUtils.createServer(config, Time.SYSTEM);
  }

  public Producer<String, String> createProducer(String metadataBrokerURI, boolean setPartitioner) {
    Properties props = new Properties();
    props.put("metadata.broker.list", metadataBrokerURI);
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    if (setPartitioner) {
      props.put("partitioner.class", "com.streamsets.pipeline.kafka.impl.ExpressionPartitioner");
    }
    props.put("request.required.acks", "1");
    ProducerConfig config = new ProducerConfig(props);
    Producer<String, String> producer = new Producer<>(config);
    return producer;
  }

  @Override
  public Map<String, String> setMaxAcks(Map<String, String> producerConfigs) {
    producerConfigs.put("acks", "all");
    return producerConfigs;
  }

  @Override
  public void setAutoOffsetReset(Map<String, String> kafkaConsumerConfigs) {
    kafkaConsumerConfigs.put("auto.offset.reset", "earliest");
  }
}

