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
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.kafka.api.ConsumerFactorySettings;
import com.streamsets.pipeline.kafka.api.MessageAndOffset;
import com.streamsets.pipeline.kafka.api.PartitionStrategy;
import com.streamsets.pipeline.kafka.api.ProducerFactorySettings;
import com.streamsets.pipeline.kafka.api.SdcKafkaConsumer;
import com.streamsets.pipeline.kafka.api.SdcKafkaConsumerFactory;
import com.streamsets.pipeline.kafka.api.SdcKafkaProducer;
import com.streamsets.pipeline.kafka.api.SdcKafkaProducerFactory;
import com.streamsets.pipeline.lib.kafka.KafkaAutoOffsetReset;
import com.streamsets.pipeline.lib.kafka.KafkaConstants;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.testing.SingleForkNoReuseTest;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.SystemTime$;
import kafka.utils.TestUtils;
import kafka.zk.EmbeddedZookeeper;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Category(SingleForkNoReuseTest.class)
public abstract class SecureKafkaBase {

  private static EmbeddedZookeeper zookeeper;
  private static KafkaServer server;
  private static String zkConnect;

  protected abstract void addBrokerSecurityConfig(Properties props);

  protected abstract void addClientSecurityConfig(Map<String, Object> props);

  protected abstract int getPlainTextPort();

  protected abstract int getSecurePort();

  protected abstract String getTopic();

  @BeforeClass
  public static void beforeClass() throws Exception {
    zookeeper = new EmbeddedZookeeper();
    zkConnect = String.format("127.0.0.1:%d", zookeeper.port());
  }

  @AfterClass
  public static void afterClass() {
    server.shutdown();
    zookeeper.shutdown();
  }

  @Test
  public void testSecureKafkaReadWrite() throws IOException, StageException, URISyntaxException, GeneralSecurityException {
    final String message = "Hello StreamSets";

    // create broker config with auto create topic option enabled
    Properties props = TestUtil09.createKafkaConfig(getPlainTextPort(), zkConnect, true, 1);
    // add SSL configuration properties to broker
    addBrokerSecurityConfig(props);
    // create server
    KafkaConfig kafkaConfig = new KafkaConfig(props);
    server = TestUtils.createServer(kafkaConfig, SystemTime$.MODULE$);

    // create and init consumer
    Source.Context sourceContext = ContextInfoCreator.createSourceContext(
      "s",
      false,
      OnRecordError.TO_ERROR,
      ImmutableList.of("a")
    );
    Map<String, Object> consumerConfig = new HashMap<>();
    consumerConfig.put("auto.commit.interval.ms", "1000");
    consumerConfig.put("auto.offset.reset", "earliest");
    consumerConfig.put("session.timeout.ms", "30000");
    consumerConfig.put(KafkaConstants.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfig.put(KafkaConstants.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    addClientSecurityConfig(consumerConfig);
    ConsumerFactorySettings consumerFactorySettings = new ConsumerFactorySettings(
      zkConnect,
      String.format("localhost:%d", getSecurePort()),
      getTopic(),
      1000,
      sourceContext,
      consumerConfig,
      "test",
      100,
      false,
      KafkaAutoOffsetReset.EARLIEST.name().toLowerCase(),
      0
    );
    SdcKafkaConsumerFactory sdcKafkaConsumerFactory = SdcKafkaConsumerFactory.create(consumerFactorySettings);
    SdcKafkaConsumer sdcKafkaConsumer = sdcKafkaConsumerFactory.create();
    sdcKafkaConsumer.validate(new ArrayList<Stage.ConfigIssue>(), sourceContext);
    sdcKafkaConsumer.init();

    // create and init producer
    HashMap<String, Object> kafkaProducerConfigs = new HashMap<>();
    kafkaProducerConfigs.put("retries", 0);
    kafkaProducerConfigs.put("batch.size", 10);
    kafkaProducerConfigs.put("linger.ms", 1000);
    addClientSecurityConfig(kafkaProducerConfigs);
    ProducerFactorySettings settings = new ProducerFactorySettings(
      kafkaProducerConfigs,
      PartitionStrategy.DEFAULT,
      "localhost:" + getSecurePort(),
      DataFormat.JSON,
      false
    );
    SdcKafkaProducerFactory sdcKafkaProducerFactory = SdcKafkaProducerFactory.create(settings);
    SdcKafkaProducer sdcKafkaProducer = sdcKafkaProducerFactory.create();
    sdcKafkaProducer.init();

    // write 10 messages
    for(int i = 0; i < 10; i++) {
      sdcKafkaProducer.enqueueMessage(getTopic(), message.getBytes(), "0");
    }
    sdcKafkaProducer.write(null);

    // read 10 messages
    List<MessageAndOffset> read = new ArrayList<>();
    while(read.size() < 10) {
      MessageAndOffset messageAndOffset = sdcKafkaConsumer.read();
      if(messageAndOffset != null) {
        read.add(messageAndOffset);
      }
    }

    // verify
    Assert.assertNotNull(read);
    Assert.assertEquals(10, read.size());
  }

}
