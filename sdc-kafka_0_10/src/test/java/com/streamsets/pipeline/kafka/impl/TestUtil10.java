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

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.SystemTime$;
import kafka.utils.TestUtils;
import org.apache.kafka.common.protocol.SecurityProtocol;
import scala.Option;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Map;
import java.util.Properties;

public class TestUtil10 {

  public static KafkaServer createKafkaServer(int port, String zkConnect) {
    return createKafkaServer(port, zkConnect, true);
  }

  public static KafkaServer createKafkaServer(int port, String zkConnect, boolean autoCreateTopic) {
    KafkaConfig config = new KafkaConfig(createKafkaConfig(port, zkConnect, autoCreateTopic, 1));
    return TestUtils.createServer(config, SystemTime$.MODULE$);
  }

  public static KafkaServer createKafkaServer(int port, String zkConnect, boolean autoCreateTopic, int numPartitions) {
    KafkaConfig config = new KafkaConfig(createKafkaConfig(port, zkConnect, autoCreateTopic, numPartitions));
    return TestUtils.createServer(config, SystemTime$.MODULE$);
  }


  public static Properties createKafkaConfig(int port, String zkConnect, boolean autoCreateTopic, int numPartitions) {
    final Option<File> noFile = scala.Option.apply(null);
    final Option<SecurityProtocol> noInterBrokerSecurityProtocol = scala.Option.apply(null);
    // new params for kafka 0.10.0
    Option<Properties> saslProperties = scala.Option.apply(null);
    Option<String> rack = scala.Option.apply(RackAwareMode.Enforced$.MODULE$.toString());

    Properties props = TestUtils.createBrokerConfig(
        0, zkConnect, false, false, port, noInterBrokerSecurityProtocol,
        noFile, saslProperties, true, false, TestUtils.RandomPort(), false, TestUtils.RandomPort(), false,
        TestUtils.RandomPort(), rack);
    props.setProperty("auto.create.topics.enable", String.valueOf(autoCreateTopic));
    props.setProperty("num.partitions", String.valueOf(numPartitions));
    props.setProperty("offsets.topic.replication.factor", "1");
    props.setProperty("message.max.bytes", "500");
    return props;
  }

  public static void addBrokerSslConfig(Properties props) {
    try {
      URL resource = TestUtil10.class.getClassLoader().getResource("server.keystore.jks");
      String serverKeystore = new File(resource.toURI()).getAbsolutePath();
      resource = TestUtil10.class.getClassLoader().getResource("server.truststore.jks");
      String serverTruststore = new File(resource.toURI()).getAbsolutePath();
      props.setProperty("ssl.keystore.location", serverKeystore);
      props.setProperty("ssl.keystore.password", "hnayak");
      props.setProperty("ssl.key.password", "hnayak");
      props.setProperty("ssl.truststore.location", serverTruststore);
      props.setProperty("ssl.truststore.password", "hnayak");
      props.setProperty("ssl.client.auth", "required");
      props.setProperty("security.inter.broker.protocol", "SSL");
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  public static void addClientSslConfig(Map<String, Object> props) {
    try {
      URL resource = TestUtil10.class.getClassLoader().getResource("client.keystore.jks");
      String clientKeystore = new File(resource.toURI()).getAbsolutePath();
      resource = TestUtil10.class.getClassLoader().getResource("client.truststore.jks");
      String clientTruststore = new File(resource.toURI()).getAbsolutePath();
      props.put("security.protocol", "SSL");
      props.put("ssl.truststore.location", clientTruststore);
      props.put("ssl.truststore.password", "hnayak");
      props.put("ssl.keystore.location", clientKeystore);
      props.put("ssl.keystore.password", "hnayak");
      props.put("ssl.key.password", "hnayak");
      props.put("ssl.enabled.protocols", "TLSv1.2,TLSv1.1,TLSv1");
      props.put("ssl.keystore.type", "JKS");
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  public static void createTopic(kafka.utils.ZkUtils zkUtils, String topic, int partitions, int replicationFactor) {
    // new param for kafka 0.10
    RackAwareMode rackAwareMode = RackAwareMode.Enforced$.MODULE$;
    AdminUtils.createTopic(zkUtils, topic, partitions, replicationFactor, new Properties(), rackAwareMode);
  }
}
