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
package com.streamsets.pipeline.stage.origin.udptokafka;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.kafka.common.SdcKafkaTestUtil;
import com.streamsets.pipeline.kafka.common.SdcKafkaTestUtilFactory;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.destination.kafka.KafkaTargetConfig;
import com.streamsets.pipeline.stage.origin.lib.UDPDataFormat;
import com.streamsets.testing.NetworkUtils;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.utils.TestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

  public class TestUDPToKafkaSource {

  private static List<KafkaStream<byte[], byte[]>> kafkaStreams1;

  private static final int PARTITIONS = 3;
  private static final int REPLICATION_FACTOR = 2;
  private static final String TOPIC1 = "TestUDPToKafkaSource1";

  private static final SdcKafkaTestUtil sdcKafkaTestUtil = SdcKafkaTestUtilFactory.getInstance().create();

  @BeforeClass
  public static void setUp() throws Exception {
    sdcKafkaTestUtil.startZookeeper();
    sdcKafkaTestUtil.startKafkaBrokers(3);
    // create topic
    sdcKafkaTestUtil.createTopic(TOPIC1, PARTITIONS, REPLICATION_FACTOR);

    for (int i = 1; i <= 1 ; i++) {
      for (int j = 0; j < PARTITIONS; j++) {
        TestUtils.waitUntilMetadataIsPropagated(
            scala.collection.JavaConversions.asScalaBuffer(sdcKafkaTestUtil.getKafkaServers()),
            "TestUDPToKafkaSource" + String.valueOf(i), j, 5000);
      }
    }
    kafkaStreams1 = sdcKafkaTestUtil.createKafkaStream(sdcKafkaTestUtil.getZkConnect(), TOPIC1, PARTITIONS);
  }

  @AfterClass
  public static void tearDown() {
    sdcKafkaTestUtil.shutdown();
  }

  private byte[] createData(int size) {
    byte[] data = new byte[size];
    for (int i = 0; i < size; i++) {
      data[i] = 1;
    }
    return data;
  }

  private void sendDatagram(byte[] data, int port) throws Exception {
    DatagramSocket clientSocket = new DatagramSocket();
    InetAddress address = InetAddress.getLoopbackAddress();
    DatagramPacket sendPacket = new DatagramPacket(data, data.length, address, port);
    clientSocket.send(sendPacket);
    clientSocket.close();
  }

    @Test
    public void testProcess() throws Exception {
      int udpPort = NetworkUtils.getRandomPort();

      UDPConfigBean udpConfigBean = new UDPConfigBean();
      udpConfigBean.ports = ImmutableList.of("" + udpPort);
      udpConfigBean.dataFormat = UDPDataFormat.NETFLOW;
      udpConfigBean.acceptThreads = 1;
      udpConfigBean.concurrency = 10;
      KafkaTargetConfig kafkaTargetConfig = new KafkaTargetConfig();
      kafkaTargetConfig.kafkaProducerConfigs = new HashMap<>();
      kafkaTargetConfig.topic = TOPIC1;
      kafkaTargetConfig.metadataBrokerList = sdcKafkaTestUtil.getMetadataBrokerURI();


      final UDPToKafkaSource source = new UDPToKafkaSource(udpConfigBean, kafkaTargetConfig);

      // create source runner
      final SourceRunner sourceRunner = new SourceRunner.Builder(UDPToKafkaDSource.class, source).addOutputLane("lane")
          .build();


      try {
        sourceRunner.runInit();

        sendDatagram(createData(1000), udpPort);
        sendDatagram(createData(1000), udpPort);

        while (source.udpConsumer.kafkaMessagesMeter.getCount() + source.udpConsumer.errorPackagesMeter.getCount() <
            2) {
          Thread.sleep(10);
        }

        Assert.assertEquals(2, source.udpConsumer.kafkaMessagesMeter.getCount());

        StageRunner.Output output = sourceRunner.runProduce(null, 1);
        Assert.assertEquals(0, output.getRecords().get("lane").size());
        Assert.assertTrue(sourceRunner.getErrorRecords().isEmpty());
        System.out.println(sourceRunner.getErrors());
        Assert.assertEquals(0, sourceRunner.getErrors().size());

        // check if kafka has received a message
        List<byte[]> messages = new ArrayList<>();
        Assert.assertTrue(kafkaStreams1.size() == PARTITIONS);

        for (KafkaStream<byte[], byte[]> kafkaStream : kafkaStreams1) {
          ConsumerIterator<byte[], byte[]> it = kafkaStream.iterator();
          try {
            while (it.hasNext()) {
              messages.add(it.next().message());
            }
          } catch (kafka.consumer.ConsumerTimeoutException e) {
            //no-op
          }
        }

        // there should be 2 messages
        Assert.assertEquals(2, messages.size());

      } finally {
        sourceRunner.runDestroy();
      }
    }
}
