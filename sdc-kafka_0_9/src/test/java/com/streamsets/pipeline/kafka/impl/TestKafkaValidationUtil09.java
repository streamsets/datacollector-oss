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

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.kafka.api.KafkaBroker;
import com.streamsets.pipeline.kafka.api.SdcKafkaValidationUtil;
import com.streamsets.pipeline.kafka.api.SdcKafkaValidationUtilFactory;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import kafka.admin.AdminUtils;
import kafka.server.KafkaServer;
import kafka.utils.TestUtils;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;
import org.apache.kafka.common.security.JaasUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;

public class TestKafkaValidationUtil09 {

  private int port;
  private EmbeddedZookeeper zookeeper;
  private String zkConnect;
  private ZkUtils zkUtils;
  private KafkaServer kafkaServer;
  private SdcKafkaValidationUtil sdcKafkaValidationUtil;

  @Before
  public void setUp() throws IOException {
    int zkConnectionTimeout = 6000;
    int zkSessionTimeout = 6000;

    zookeeper = new EmbeddedZookeeper();
    zkConnect = String.format("127.0.0.1:%d", zookeeper.port());
    zkUtils = ZkUtils.apply(
      zkConnect, zkSessionTimeout, zkConnectionTimeout,
      JaasUtils.isZkSecurityEnabled());

    port = TestUtil.getFreePort();
    kafkaServer = TestUtil.createKafkaServer(port, zkConnect, false);
    sdcKafkaValidationUtil = SdcKafkaValidationUtilFactory.getInstance().create();
  }

  @After
  public void tearDown() {
    kafkaServer.shutdown();
    zookeeper.shutdown();
  }

  @Test
  public void testKafkaProducer09Version() throws IOException {
    Assert.assertEquals(Kafka09Constants.KAFKA_VERSION, sdcKafkaValidationUtil.getVersion());
  }

  @Test
  public void testGetPartitionCount() throws IOException, StageException {

    final String topic1 = "TestKafkaValidationUtil09_1";
    final String topic2 = "TestKafkaValidationUtil09_2";
    final String topic3 = "TestKafkaValidationUtil09_3";

    createTopic(zkUtils, topic1, 1, kafkaServer);
    createTopic(zkUtils, topic2, 2, kafkaServer);
    createTopic(zkUtils, topic3, 3, kafkaServer);

    Assert.assertEquals(1, sdcKafkaValidationUtil.getPartitionCount(
        "localhost:" + port,
        topic1,
        new HashMap<String, Object>(),
        1,
        2000
    ));
    Assert.assertEquals(2, sdcKafkaValidationUtil.getPartitionCount(
        "localhost:" + port,
        topic2,
        new HashMap<String, Object>(),
        1,
        2000
    ));
    Assert.assertEquals(3, sdcKafkaValidationUtil.getPartitionCount(
        "localhost:" + port,
        topic3,
        new HashMap<String, Object>(),
        1,
        2000
    ));

    AdminUtils.deleteTopic(
      zkUtils,
      topic1
    );
    AdminUtils.deleteTopic(
      zkUtils,
      topic2
    );
    AdminUtils.deleteTopic(
      zkUtils,
      topic3
    );
  }

  @Test(timeout = 10000)
  public void testTopicExists() throws IOException, StageException {

    final String topic1 = "TestKafkaValidationUtil09_1";
    final String topicX = "TestKafkaValidationUtil09_X";

    createTopic(zkUtils, topic1, 1, kafkaServer);

    Source.Context sourceContext = ContextInfoCreator.createSourceContext(
        "s",
        false,
        OnRecordError.TO_ERROR,
        ImmutableList.of("a")
    );
    ArrayList<Stage.ConfigIssue> configIssues = new ArrayList<>();
    boolean valid = sdcKafkaValidationUtil.validateTopicExistence(
      sourceContext,
      "KAFKA",
      "topic",
      new ArrayList<KafkaBroker>(),
      "localhost:" + port,
      topic1,
      new HashMap<String, Object>(),
      configIssues
    );

    Assert.assertEquals(true, valid);
    Assert.assertEquals(0, configIssues.size());

    valid = sdcKafkaValidationUtil.validateTopicExistence(
      sourceContext,
      "KAFKA",
      "topic",
      new ArrayList<KafkaBroker>(),
      "localhost:" + port,
      topicX,
      new HashMap<String, Object>(),
      configIssues
    );

    Assert.assertEquals(false, valid);
    Assert.assertEquals(1, configIssues.size());

    AdminUtils.deleteTopic(
      zkUtils,
      topic1
    );
  }

  private void createTopic(ZkUtils zkUtils, String topic, int partitionCount, KafkaServer kafkaServer) {
    AdminUtils.createTopic(zkUtils, topic, partitionCount, 1, new Properties());
    TestUtils.waitUntilMetadataIsPropagated(
      scala.collection.JavaConversions.asScalaBuffer(Arrays.asList(kafkaServer)), topic, 0, 3000);
  }
}
