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

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.kafka.api.MessageAndOffset;
import com.streamsets.pipeline.kafka.api.SdcKafkaConsumer;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.testing.NetworkUtils;
import kafka.admin.AdminUtils;
import kafka.server.KafkaServer;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;
import org.apache.kafka.common.security.JaasUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class KafkaConsumer09IT extends KafkaNewConsumerITBase {

  @Test
  public void testKafkaConsumer09Version() throws IOException {
    Source.Context sourceContext = ContextInfoCreator.createSourceContext(
        "s",
        false,
        OnRecordError.TO_ERROR,
        ImmutableList.of("a")
    );

    SdcKafkaConsumer sdcKafkaConsumer = createSdcKafkaConsumer("", "", 0, sourceContext, Collections.emptyMap(), "");
    Assert.assertEquals(Kafka09Constants.KAFKA_VERSION, sdcKafkaConsumer.getVersion());
  }

  @Test
  public void testKafkaConsumer09Read() throws IOException, StageException {
    int zkConnectionTimeout = 6000;
    int zkSessionTimeout = 6000;

    EmbeddedZookeeper zookeeper = new EmbeddedZookeeper();
    String zkConnect = String.format("127.0.0.1:%d", zookeeper.port());
    ZkUtils zkUtils = ZkUtils.apply(
      zkConnect, zkSessionTimeout, zkConnectionTimeout,
      JaasUtils.isZkSecurityEnabled());

    int port = NetworkUtils.getRandomPort();
    KafkaServer kafkaServer = TestUtil09.createKafkaServer(port, zkConnect);

    final String topic = "TestKafkaConsumer09_1";
    final String message = "Hello StreamSets";

    Source.Context sourceContext = ContextInfoCreator.createSourceContext(
      "s",
      false,
      OnRecordError.TO_ERROR,
      ImmutableList.of("a")
    );

    SdcKafkaConsumer sdcKafkaConsumer = createKafkaConsumer(port, topic, sourceContext);

    // produce some messages to topic
    produce(topic, "localhost:" + port, message);

    // read
    List<MessageAndOffset> read = new ArrayList<>();
    while(read.size() < NUM_MESSAGES) {
      MessageAndOffset messageAndOffset = sdcKafkaConsumer.read();
      if(messageAndOffset != null) {
        read.add(messageAndOffset);
      }
    }
    // verify
    Assert.assertNotNull(read);
    Assert.assertEquals(NUM_MESSAGES, read.size());
    verify(read, message);

    // delete topic and shutdown
    AdminUtils.deleteTopic(
      zkUtils,
      topic
    );
    kafkaServer.shutdown();
    zookeeper.shutdown();
  }

  @Override
  protected KafkaServer buildKafkaServer(int port, String zkConnect, int numPartitions) {
    return TestUtil09.createKafkaServer(port, zkConnect, true, numPartitions);
  }


  private void verify(List<MessageAndOffset> read, String message) {
    for(int i = 0; i < read.size(); i++) {
      Assert.assertEquals(message+i, new String((byte[])read.get(i).getPayload()));
    }
  }
}
