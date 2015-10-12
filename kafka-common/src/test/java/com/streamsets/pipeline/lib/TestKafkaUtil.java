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
package com.streamsets.pipeline.lib;

import com.google.common.collect.Lists;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.kafka.KafkaErrors;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.TestZKUtils;
import kafka.zk.EmbeddedZookeeper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

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
    Assert.assertEquals(1, KafkaUtil.getPartitionCount("localhost:" + port, "testFetchTopicMetaData", 3, 1000));
    kafkaServer.shutdown();
  }

  @Test(timeout = 60000)
  public void testFetchTopicMetaDataAutoCreateFalse() throws Exception {
    // setup Broker
    Properties props = TestUtils.createBrokerConfig(0, port);
    props.setProperty("auto.create.topics.enable", "false");
    KafkaServer kafkaServer = TestUtils.createServer(new KafkaConfig(props), new MockTime());
    try {
      KafkaUtil.getPartitionCount("localhost:" + port, "testFetchTopicMetaDataAutoCreateFalse", 3, 1000);
      Assert.fail("Expected StageException but didn't get any");
    } catch (IOException e) {
      //NOP assertEquals(Errors.KAFKA_03, e.getErrorCode());
    } catch (Exception e) {
      Assert.fail("Expected stage exception with error code " + KafkaErrors.KAFKA_03 + " but got " + e);
    }
    kafkaServer.shutdown();
  }

  @Test
  public void testValidateZkConnectString() {
    List<Stage.ConfigIssue> issues = Lists.newArrayList();
    Stage.Context ctx = Mockito.mock(Stage.Context.class);
    Stage.ConfigIssue issue = Mockito.mock(Stage.ConfigIssue.class);
    Mockito.when(ctx.createConfigIssue(Mockito.anyString(), Mockito.anyString(),
        Mockito.any(ErrorCode.class), Mockito.any(), Mockito.any())).thenReturn(issue);

    List<KafkaBroker> kafkaBrokers = KafkaUtil.validateZkConnectionString(issues, "localhost:2181", "KAFKA",
      "zookeeperConnect", ctx);
    Mockito.verifyZeroInteractions(ctx);
    Assert.assertTrue(kafkaBrokers.size() == 1);
    Assert.assertEquals(kafkaBrokers.get(0).getHost(), "localhost");
    Assert.assertEquals(kafkaBrokers.get(0).getPort(), 2181);

    kafkaBrokers = KafkaUtil.validateZkConnectionString(issues, "localhost1:2181,localhost2:2181", "KAFKA",
      "zookeeperConnect", ctx);
    Mockito.verifyZeroInteractions(ctx);
    Assert.assertTrue(kafkaBrokers.size() == 2);
    Assert.assertEquals(kafkaBrokers.get(0).getHost(), "localhost1");
    Assert.assertEquals(kafkaBrokers.get(0).getPort(), 2181);
    Assert.assertEquals(kafkaBrokers.get(1).getHost(), "localhost2");
    Assert.assertEquals(kafkaBrokers.get(1).getPort(), 2181);

    kafkaBrokers = KafkaUtil.validateZkConnectionString(issues, "localhost1:2181,localhost2:2181/kafka", "KAFKA",
      "zookeeperConnect", ctx);
    Mockito.verifyZeroInteractions(ctx);
    Assert.assertTrue(kafkaBrokers.size() == 2);
    Assert.assertEquals(kafkaBrokers.get(0).getHost(), "localhost1");
    Assert.assertEquals(kafkaBrokers.get(0).getPort(), 2181);
    Assert.assertEquals(kafkaBrokers.get(1).getHost(), "localhost2");
    Assert.assertEquals(kafkaBrokers.get(1).getPort(), 2181);

    kafkaBrokers = KafkaUtil.validateZkConnectionString(issues, "localhost:2181/kafka", "KAFKA",
      "zookeeperConnect", ctx);
    Mockito.verifyZeroInteractions(ctx);
    Assert.assertEquals(kafkaBrokers.get(0).getHost(), "localhost");
    Assert.assertEquals(kafkaBrokers.get(0).getPort(), 2181);

    kafkaBrokers = KafkaUtil.validateZkConnectionString(issues, "localhost1:2181/kafka,localhost2:2181/kafka", "KAFKA",
      "zookeeperConnect", ctx);
    //  - /kafka,localhost2:2181/kafka is a valid chroot path
    Mockito.verifyZeroInteractions(ctx);
    Assert.assertEquals(kafkaBrokers.get(0).getHost(), "localhost1");
    Assert.assertEquals(kafkaBrokers.get(0).getPort(), 2181);

    kafkaBrokers = KafkaUtil.validateZkConnectionString(issues, "localhost:2181/", "KAFKA",
      "zookeeperConnect", ctx);
    Mockito.verifyZeroInteractions(ctx);
    Assert.assertEquals(kafkaBrokers.get(0).getHost(), "localhost");
    Assert.assertEquals(kafkaBrokers.get(0).getPort(), 2181);

    KafkaUtil.validateZkConnectionString(issues, "localhost:2181/localhost.2181/", "KAFKA",
      "zookeeperConnect", ctx);
    Mockito.verify(ctx, Mockito.times(1)).createConfigIssue(Mockito.anyString(), Mockito.anyString(),
      Mockito.any(ErrorCode.class), Mockito.any(), Mockito.any());

    KafkaUtil.validateZkConnectionString(issues, "localhost:2181#####", "KAFKA",
      "zookeeperConnect", ctx);
    Mockito.verify(ctx, Mockito.times(2)).createConfigIssue(Mockito.anyString(), Mockito.anyString(),
      Mockito.any(ErrorCode.class), Mockito.any(), Mockito.any());

    KafkaUtil.validateZkConnectionString(issues, "localhost:2181:675425", "KAFKA",
      "zookeeperConnect", ctx);
    Mockito.verify(ctx, Mockito.times(3)).createConfigIssue(Mockito.anyString(), Mockito.anyString(),
      Mockito.any(ErrorCode.class), Mockito.any(), Mockito.any());

    //Errors.KAFKA_06, not that the number of parameters to createConfigIssue is on less here.
    KafkaUtil.validateZkConnectionString(issues, "", "KAFKA", "zookeeperConnect", ctx);
    Mockito.verify(ctx, Mockito.times(1)).createConfigIssue(Mockito.anyString(), Mockito.anyString(),
      Mockito.any(ErrorCode.class), Mockito.any());

    KafkaUtil.validateZkConnectionString(issues, null, "KAFKA", "zookeeperConnect", ctx);
    Mockito.verify(ctx, Mockito.times(2)).createConfigIssue(Mockito.anyString(), Mockito.anyString(),
      Mockito.any(ErrorCode.class), Mockito.any());

  }

  @Test
  public void testValidateKafkaBrokerConnectString() {
    List<Stage.ConfigIssue> issues = Lists.newArrayList();
    Stage.Context ctx = Mockito.mock(Stage.Context.class);
    Stage.ConfigIssue issue = Mockito.mock(Stage.ConfigIssue.class);
    Mockito.when(ctx.createConfigIssue(Mockito.anyString(), Mockito.anyString(),
      Mockito.any(ErrorCode.class), Mockito.any())).thenReturn(issue);

    List<KafkaBroker> kafkaBrokers = KafkaUtil.validateKafkaBrokerConnectionString(issues, "localhost:9001", "KAFKA",
      "kafkaBroker", ctx);
    Mockito.verifyZeroInteractions(ctx);
    Assert.assertTrue(kafkaBrokers.size() == 1);
    Assert.assertEquals(kafkaBrokers.get(0).getHost(), "localhost");
    Assert.assertEquals(kafkaBrokers.get(0).getPort(), 9001);

    kafkaBrokers = KafkaUtil.validateKafkaBrokerConnectionString(issues,
      "localhost1:9001,localhost2:9002,localhost3:9003", "KAFKA", "kafkaBroker", ctx);
    Mockito.verifyZeroInteractions(ctx);
    Assert.assertTrue(kafkaBrokers.size() == 3);
    Assert.assertEquals(kafkaBrokers.get(0).getHost(), "localhost1");
    Assert.assertEquals(kafkaBrokers.get(0).getPort(), 9001);
    Assert.assertEquals(kafkaBrokers.get(1).getHost(), "localhost2");
    Assert.assertEquals(kafkaBrokers.get(1).getPort(), 9002);
    Assert.assertEquals(kafkaBrokers.get(2).getHost(), "localhost3");
    Assert.assertEquals(kafkaBrokers.get(2).getPort(), 9003);

    KafkaUtil.validateKafkaBrokerConnectionString(issues, "", "KAFKA",
      "kafkaBroker", ctx);
    Mockito.verify(ctx, Mockito.times(1)).createConfigIssue(Mockito.anyString(), Mockito.anyString(),
      Mockito.any(ErrorCode.class), Mockito.any());

    KafkaUtil.validateKafkaBrokerConnectionString(issues, null, "KAFKA",
      "kafkaBroker", ctx);
    Mockito.verify(ctx, Mockito.times(2)).createConfigIssue(Mockito.anyString(), Mockito.anyString(),
      Mockito.any(ErrorCode.class), Mockito.any());

    KafkaUtil.validateKafkaBrokerConnectionString(issues, "localhost:myPort", "KAFKA",
      "kafkaBroker", ctx);
    Mockito.verify(ctx, Mockito.times(3)).createConfigIssue(Mockito.anyString(), Mockito.anyString(),
      Mockito.any(ErrorCode.class), Mockito.any());

    KafkaUtil.validateKafkaBrokerConnectionString(issues, "localhost:2181:jdfhdksf", "KAFKA",
      "kafkaBroker", ctx);
    Mockito.verify(ctx, Mockito.times(4)).createConfigIssue(Mockito.anyString(), Mockito.anyString(),
      Mockito.any(ErrorCode.class), Mockito.any());
  }

}
