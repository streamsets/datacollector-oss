/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.kafka;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.SourceRunner;
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
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class TestKafkaSourceUnavailability {

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

    producer = KafkaTestUtil.createProducer(HOST, port);
  }

  @After
  public void tearDown() {
    kafkaServer.shutdown();
    zkClient.close();
    zkServer.shutdown();
  }


  @Test(expected = StageException.class)
  public void testKafkaServerDown() throws StageException {

    KafkaTestUtil.createTopic(zkClient, ImmutableList.of(kafkaServer), "testKafkaServerDown", PARTITIONS,
      REPLICATION_FACTOR, TIME_OUT);
    List<KeyedMessage<String, String>> data = KafkaTestUtil.produceStringMessages("testKafkaServerDown",
      String.valueOf(0));
    for (KeyedMessage<String, String> d : data) {
      producer.send(d);
    }

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaSource.class)
      .addOutputLane("lane")
      .addConfiguration("topic", "testKafkaServerDown")
      .addConfiguration("partition", 0)
      .addConfiguration("brokerHost", HOST)
      .addConfiguration("brokerPort", port)
      .addConfiguration("fromBeginning", true)
      .addConfiguration("maxBatchSize", 64000)
      .addConfiguration("maxWaitTime", 5000)
      .addConfiguration("minBatchSize", 100)
      .addConfiguration("consumerPayloadType", ConsumerPayloadType.LOG)
      .build();

    sourceRunner.runInit();
    kafkaServer.shutdown();
    sourceRunner.runProduce(null, 5);
  }

  @Test
  public void testZookeeperDown() throws StageException {

    KafkaTestUtil.createTopic(zkClient, ImmutableList.of(kafkaServer), "testKafkaServerDown", PARTITIONS,
      REPLICATION_FACTOR, TIME_OUT);
    List<KeyedMessage<String, String>> data = KafkaTestUtil.produceStringMessages("testKafkaServerDown",
      String.valueOf(0));
    for (KeyedMessage<String, String> d : data) {
      producer.send(d);
    }

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaSource.class)
      .addOutputLane("lane")
      .addConfiguration("topic", "testKafkaServerDown")
      .addConfiguration("partition", 0)
      .addConfiguration("brokerHost", HOST)
      .addConfiguration("brokerPort", port)
      .addConfiguration("fromBeginning", true)
      .addConfiguration("maxBatchSize", 64000)
      .addConfiguration("maxWaitTime", 5000)
      .addConfiguration("minBatchSize", 100)
      .addConfiguration("consumerPayloadType", ConsumerPayloadType.LOG)
      .build();

    sourceRunner.runInit();
    zkServer.shutdown();
    sourceRunner.runProduce(null, 5);
    //recreate zookeeper and kafka server
  }
}
