/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.Properties;

import com.google.common.collect.Lists;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.Stage;
import junit.framework.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.streamsets.pipeline.api.StageException;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.TestZKUtils;
import kafka.zk.EmbeddedZookeeper;
import org.mockito.Mockito;

public class TestKafkaUtil {
  private static EmbeddedZookeeper zkServer;
  private static int port;

  @BeforeClass
  public static void setUp() {
    String zkConnect = TestZKUtils.zookeeperConnect();
    zkServer = new EmbeddedZookeeper(zkConnect);
    port = TestUtils.choosePort();
  }

  @AfterClass
  public static void tearDown() {
    if (zkServer != null) {
      zkServer.shutdown();
    }
  }

  @Test(timeout = 60000)
  public void testFetchTopicMetaData() throws Exception {
    // setup Broker
    Properties props = TestUtils.createBrokerConfig(0, port);
    props.setProperty("auto.create.topics.enable", "true");
    KafkaServer kafkaServer = null;
    kafkaServer = TestUtils.createServer(new KafkaConfig(props), new MockTime());
    assertEquals(1, KafkaUtil.getPartitionCount(new String("localhost:" + port), "testFetchTopicMetaData", 3, 1000));
    kafkaServer.shutdown();
  }

  @Test(timeout = 60000)
  public void testFetchTopicMetaDataAutoCreateFalse() throws Exception {
    // setup Broker
    Properties props = TestUtils.createBrokerConfig(0, port);
    props.setProperty("auto.create.topics.enable", "false");
    KafkaServer kafkaServer = TestUtils.createServer(new KafkaConfig(props), new MockTime());
    try {
      KafkaUtil.getPartitionCount(new String("localhost:" + port), "testFetchTopicMetaDataAutoCreateFalse", 3, 1000);
      fail("Expected StageException but didn't get any");
    } catch (StageException e) {
      assertEquals(Errors.KAFKA_03, e.getErrorCode());
    } catch (Exception e) {
      fail("Expected stage exception with error code " + Errors.KAFKA_03 + " but got " + e);
    }
    kafkaServer.shutdown();
  }

  @Test
  public void testValidateBrokerURI() {
    List<Stage.ConfigIssue> issues = Lists.newArrayList();
    Stage.Context ctx = Mockito.mock(Stage.Context.class);
    Stage.ConfigIssue issue = Mockito.mock(Stage.ConfigIssue.class);
    Mockito.when(ctx.createConfigIssue(Mockito.anyString(), Mockito.anyString(),
        Mockito.any(ErrorCode.class), Mockito.any())).thenReturn(issue);

    List<KafkaBroker> kafkaBrokers = KafkaUtil.validateConnectionString(issues, "localhost:2181", "KAFKA",
        "zookeeperConnect", ctx);
    Mockito.verifyZeroInteractions(ctx);
    assertTrue(kafkaBrokers.size() == 1);
    assertEquals(kafkaBrokers.get(0).getHost(), "localhost");
    assertEquals(kafkaBrokers.get(0).getPort(), 2181);


    kafkaBrokers = KafkaUtil.validateConnectionString(issues, "localhost:2181#####", "KAFKA",
        "zookeeperConnect", ctx);
    Mockito.verify(ctx, Mockito.times(1)).createConfigIssue(Mockito.anyString(), Mockito.anyString(),
        Mockito.any(ErrorCode.class), Mockito.any());

    kafkaBrokers = KafkaUtil.validateConnectionString(issues, "localhost1:2181,localhost2:2181", "KAFKA",
        "zookeeperConnect", ctx);
    Mockito.verifyZeroInteractions(ctx);
    assertTrue(kafkaBrokers.size() == 2);
    assertEquals(kafkaBrokers.get(0).getHost(), "localhost1");
    assertEquals(kafkaBrokers.get(0).getPort(), 2181);
    assertEquals(kafkaBrokers.get(1).getHost(), "localhost2");
    assertEquals(kafkaBrokers.get(1).getPort(), 2181);

    kafkaBrokers = KafkaUtil.validateConnectionString(issues, "localhost:2181/kafka", "KAFKA",
        "zookeeperConnect", ctx);
    Mockito.verifyZeroInteractions(ctx);
    assertEquals(kafkaBrokers.get(0).getHost(), "localhost");
    assertEquals(kafkaBrokers.get(0).getPort(), 2181);
  }

}
