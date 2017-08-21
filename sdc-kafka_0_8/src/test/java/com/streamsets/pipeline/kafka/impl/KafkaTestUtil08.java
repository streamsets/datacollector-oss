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

import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.kafka.common.SdcKafkaTestUtil;
import kafka.admin.AdminUtils;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.TestZKUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaTestUtil08 extends SdcKafkaTestUtil {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaTestUtil08.class);
  private static final long TIME_OUT = 5000;

  private static ZkClient zkClient;
  private static List<KafkaServer> kafkaServers;
  private static Map<String, String> kafkaProps;
  private static String metadataBrokerURI;
  private static String zkConnect;
  private static EmbeddedZookeeper zkServer;

  public KafkaTestUtil08() {
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

  public EmbeddedZookeeper getZkServer() {
    return zkServer;
  }

  public void startZookeeper() {
    zkConnect = TestZKUtils.zookeeperConnect();
    try {
      zkServer = new EmbeddedZookeeper(zkConnect);
    } catch (Exception ex) {
      String msg = Utils.format("Error starting zookeeper {}: {}", zkConnect, ex);
      throw new RuntimeException(msg, ex);
    }
    zkClient = new ZkClient(zkServer.connectString(), 30000, 30000, ZKStringSerializer$.MODULE$);
  }

  public void startKafkaBrokers(int numberOfBrokers) {
    kafkaServers = new ArrayList<>(numberOfBrokers);
    kafkaProps = new HashMap<>();
    // setup Broker
    StringBuilder sb = new StringBuilder();
    for(int i = 0; i < numberOfBrokers; i ++) {
      int port = TestUtils.choosePort();
      Properties props = createBrokerConfig(i, port);
      props.put("auto.create.topics.enable", "false");
      kafkaServers.add(TestUtils.createServer(new KafkaConfig(props), new MockTime()));
      sb.append("localhost:").append(port).append(",");
    }
    metadataBrokerURI = sb.deleteCharAt(sb.length()-1).toString();
    LOG.info("Setting metadataBrokerList and auto.offset.reset for test case");
    kafkaProps.put("auto.offset.reset", "smallest");
  }

  private static Properties createBrokerConfig(int i, int port) {
    Properties props = new Properties();
    props.put("broker.id", String.valueOf(i));
    props.put("host.name", "localhost");
    props.put("port", String.valueOf(port));
    props.put("log.dir", TestUtils.tempDir().getAbsolutePath());
    props.put("zookeeper.connect", TestZKUtils.zookeeperConnect());
    props.put("replica.socket.timeout.ms", "1500");
    props.put("controlled.shutdown.enable", "true");
    return props;
  }

  public void createTopic(String topic, int partitions, int replicationFactor) {
    AdminUtils.createTopic(zkClient, topic, partitions, replicationFactor, new Properties());
  }

  public void shutdown() {
    for(KafkaServer kafkaServer : kafkaServers) {
      kafkaServer.shutdown();
    }
    zkClient.close();
    zkServer.shutdown();
    metadataBrokerURI = null;
    zkConnect = null;
    kafkaProps = null;
  }

  @Override
  public void setAutoOffsetReset(Map<String, String> kafkaConsumerConfigs) {
    kafkaConsumerConfigs.put("auto.offset.reset", "smallest");
  }

  @Override
  public Map<String, String> setMaxAcks(Map<String, String> producerConfigs) {
    producerConfigs.put("request.required.acks", "-1");
    return producerConfigs;
  }

}
