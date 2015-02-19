/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.kafka;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.TargetRunner;
import kafka.admin.AdminUtils;
import kafka.consumer.KafkaStream;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class TestKafkaTargetUnavailability {

  private static KafkaServer kafkaServer;
  private static ZkClient zkClient;
  private static EmbeddedZookeeper zkServer;
  private static List<KafkaStream<byte[], byte[]>> kafkaStreams;
  private static int port;

  private static final String HOST = "localhost";
  private static final int BROKER_ID = 0;
  private static final int PARTITIONS = 1;
  private static final int REPLICATION_FACTOR = 1;
  private static final String TOPIC = "test";
  private static final int TIME_OUT = 1000;

  @Before
  public void setUp() {
    //Init zookeeper
    String zkConnect = TestZKUtils.zookeeperConnect();
    zkServer = new EmbeddedZookeeper(zkConnect);
    zkClient = new ZkClient(zkServer.connectString(), 30000, 30000, ZKStringSerializer$.MODULE$);
    // setup Broker
    port = TestUtils.choosePort();
    Properties props = TestUtils.createBrokerConfig(BROKER_ID, port);
    kafkaServer = TestUtils.createServer(new KafkaConfig(props), new MockTime());
    // create topic
    AdminUtils.createTopic(zkClient, TOPIC, PARTITIONS, REPLICATION_FACTOR, new Properties());
    List<KafkaServer> servers = new ArrayList<>();
    servers.add(kafkaServer);
    TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asBuffer(servers), TOPIC, 0, TIME_OUT);

    kafkaStreams = KafkaTestUtil.createKafkaStream(zkServer.connectString(), TOPIC, PARTITIONS);
  }

  @After
  public void tearDown() {
    kafkaServer.shutdown();
    zkClient.close();
    zkServer.shutdown();
  }

  //The test is commented out as they take a long time to complete ~ 10 seconds
  //@Test
  public void testKafkaServerDown() throws InterruptedException, StageException {

    KafkaTarget kafkaTarget = new KafkaTarget();
    TargetRunner targetRunner = new TargetRunner.Builder(kafkaTarget)
      .addConfiguration("topic", TOPIC)
      .addConfiguration("partition", "0")
      .addConfiguration("brokerHost", HOST)
      .addConfiguration("brokerPort", port)
      .addConfiguration("kafkaProducerConfigs", null)
      .addConfiguration("payloadType", ConsumerPayloadType.LOG)
      .addConfiguration("partitionStrategy", PartitionStrategy.EXPRESSION)
      .addConfiguration("constants", null)
      .addConfiguration("csvFileFormat", "DEFAULT")
      .build();

    targetRunner.runInit();
    List<Record> logRecords = KafkaTestUtil.createStringRecords();

    kafkaServer.shutdown();

    try {
      targetRunner.runWrite(logRecords);
      Assert.fail("Expected StageException, got none.");
    } catch (StageException e) {
      Assert.assertEquals(Errors.KAFKA_16, e.getErrorCode());
    }

    targetRunner.runDestroy();
  }

  //The test is commented out as they take a long time to complete ~ 5 seconds
  //@Test
  public void testZookeeperDown() throws InterruptedException, StageException {

    KafkaTarget kafkaTarget = new KafkaTarget();
    TargetRunner targetRunner = new TargetRunner.Builder(kafkaTarget)
      .addConfiguration("topic", TOPIC)
      .addConfiguration("partition", "0")
      .addConfiguration("brokerHost", HOST)
      .addConfiguration("brokerPort", port)
      .addConfiguration("kafkaProducerConfigs", null)
      .addConfiguration("payloadType", ConsumerPayloadType.LOG)
      .addConfiguration("partitionStrategy", PartitionStrategy.EXPRESSION)
      .addConfiguration("constants", null)
      .addConfiguration("csvFileFormat", "DEFAULT")
      .build();

    targetRunner.runInit();
    List<Record> logRecords = KafkaTestUtil.createStringRecords();

    zkServer.shutdown();
    Thread.sleep(500);
    try {
      targetRunner.runWrite(logRecords);
    } catch (StageException e) {
      Assert.assertEquals(Errors.KAFKA_16, e.getErrorCode());
    }
    targetRunner.runDestroy();
  }
}
