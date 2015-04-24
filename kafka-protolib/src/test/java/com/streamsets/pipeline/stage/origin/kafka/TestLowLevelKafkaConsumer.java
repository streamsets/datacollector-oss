/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.kafka;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.KafkaBroker;
import com.streamsets.pipeline.lib.KafkaTestUtil;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.TestZKUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class TestLowLevelKafkaConsumer {

  private static KafkaServer kafkaServer;
  private static ZkClient zkClient;
  private static EmbeddedZookeeper zkServer;
  private static int port;

  private static Producer<String, String> producer;

  private static final String HOST = "localhost";
  private static final int BROKER_ID = 0;
  private static final int PARTITIONS = 1;
  private static final int REPLICATION_FACTOR = 1;
  private static final String TOPIC = "test";
  private static final int TIME_OUT = 2000;

  @Before
  public void setUp() {
    //Init zookeeper
    String zkConnect = TestZKUtils.zookeeperConnect();
    zkServer = new EmbeddedZookeeper(zkConnect);
    zkClient = new ZkClient(zkServer.connectString(), 30000, 30000, ZKStringSerializer$.MODULE$);
    // setup Broker
    port = TestUtils.choosePort();
    Properties props = TestUtils.createBrokerConfig(BROKER_ID, port);
    List<KafkaServer> servers = new ArrayList<>();
    kafkaServer = TestUtils.createServer(new KafkaConfig(props), new MockTime());
    servers.add(kafkaServer);

    producer = KafkaTestUtil.createProducer(HOST, port, true);
  }

  @After
  public void tearDown() {
    kafkaServer.shutdown();
    zkClient.close();
    zkServer.shutdown();
  }

  @Test(expected = StageException.class)
  public void testReadAfterKafkaShutdown() throws Exception {
    KafkaTestUtil.createTopic(zkClient, ImmutableList.of(kafkaServer), "testReadAfterZookeeperShutdown", PARTITIONS,
      REPLICATION_FACTOR, TIME_OUT);
    List<KeyedMessage<String, String>> data = KafkaTestUtil.produceStringMessages("testProduceStringRecords",
      String.valueOf(0));
    //writes 9 messages to kafka topic
    for(KeyedMessage<String, String> d : data) {
      producer.send(d);
    }

    LowLevelKafkaConsumer kafkaConsumer = new LowLevelKafkaConsumer("testReadAfterZookeeperShutdown", 0, new KafkaBroker(HOST, port), 0, 8000,
      2000, "testKafkaConsumer" + "_client");
    kafkaConsumer.init();
    //shutdown zookeeper server
    kafkaServer.shutdown();
    //attempt to read
    kafkaConsumer.read(0);
  }

  @Test(expected = StageException.class)
  public void testGetOffsetAfterKafkaShutdown() throws Exception {
    KafkaTestUtil.createTopic(zkClient, ImmutableList.of(kafkaServer), "testGetOffsetAfterZookeeperShutdown", PARTITIONS,
      REPLICATION_FACTOR, TIME_OUT);
    List<KeyedMessage<String, String>> data = KafkaTestUtil.produceStringMessages("testGetOffsetAfterZookeeperShutdown",
      String.valueOf(0));
    //writes 9 messages to kafka topic
    for(KeyedMessage<String, String> d : data) {
      producer.send(d);
    }

    LowLevelKafkaConsumer kafkaConsumer = new LowLevelKafkaConsumer("testGetOffsetAfterZookeeperShutdown", 0, new KafkaBroker(HOST, port), 0, 8000,
      2000, "testKafkaConsumer" + "_client");
    kafkaConsumer.init();
    //shutdown zookeeper server
    kafkaServer.shutdown();
    //attempt to read
    kafkaConsumer.getOffsetToRead(true);
  }

  @Test
  public void testReadInvalidOffset() throws Exception {
    KafkaTestUtil.createTopic(zkClient, ImmutableList.of(kafkaServer), "testReadInvalidOffset", PARTITIONS,
      REPLICATION_FACTOR, TIME_OUT);
    List<KeyedMessage<String, String>> data = KafkaTestUtil.produceStringMessages("testReadInvalidOffset",
      String.valueOf(0));
    //writes 9 messages to kafka topic
    for(KeyedMessage<String, String> d : data) {
      producer.send(d);
    }

    LowLevelKafkaConsumer kafkaConsumer = new LowLevelKafkaConsumer("testReadInvalidOffset", 0, new KafkaBroker(HOST, port), 0, 8000,
      2000, "testKafkaConsumer" + "_client");
    kafkaConsumer.init();
    //attempt to read invalid offset
    List<MessageAndOffset> read = kafkaConsumer.read(12);
    Assert.assertEquals(0, read.size());
  }

  @Test
  public void testReadValidOffset() throws Exception {
    KafkaTestUtil.createTopic(zkClient, ImmutableList.of(kafkaServer), "testReadValidOffset", PARTITIONS,
      REPLICATION_FACTOR, TIME_OUT);
    List<KeyedMessage<String, String>> data = KafkaTestUtil.produceStringMessages("testReadValidOffset",
      String.valueOf(0));
    //writes 9 messages to kafka topic
    for(KeyedMessage<String, String> d : data) {
      producer.send(d);
    }

    LowLevelKafkaConsumer kafkaConsumer = new LowLevelKafkaConsumer("testReadValidOffset", 0, new KafkaBroker(HOST, port), 0, 8000,
      2000, "testKafkaConsumer" + "_client");
    kafkaConsumer.init();
    //attempt to read invalid offset
    List<MessageAndOffset> read = kafkaConsumer.read(6);
    Assert.assertEquals(3, read.size());
  }
}
