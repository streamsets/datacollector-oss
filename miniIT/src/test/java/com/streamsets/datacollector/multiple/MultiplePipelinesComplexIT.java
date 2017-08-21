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
package com.streamsets.datacollector.multiple;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.streamsets.datacollector.base.MultiplePipelinesBaseIT;
import com.streamsets.datacollector.util.TestUtil;
import com.streamsets.pipeline.kafka.common.KafkaTestUtil;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.source.AvroSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileOutputStream;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Ignore
public class MultiplePipelinesComplexIT extends MultiplePipelinesBaseIT {

  private static final String TOPIC1 = "KafkaToFlume";
  private static final String TOPIC2 = "KafkaToHDFS";
  private static final String TOPIC3 = "randomToKafka";

  //Flume destination related
  private static AvroSource source;
  private static Channel ch;
  private static Producer<String, String> producer1;
  private static Producer<String, String> producer2;
  private static int flumePort;
  private static ExecutorService e;

  //HDFS
  private static MiniDFSCluster miniDFS;

  private static List<String> getPipelineJson() throws URISyntaxException, IOException {
    //random to kafka
    URI uri = Resources.getResource("kafka_destination_pipeline_operations.json").toURI();
    String randomToKafka =  new String(Files.readAllBytes(Paths.get(uri)), StandardCharsets.UTF_8);
    randomToKafka = randomToKafka.replace("topicName", TOPIC3);
    randomToKafka = randomToKafka.replaceAll("localhost:9092", KafkaTestUtil.getMetadataBrokerURI());
    randomToKafka = randomToKafka.replaceAll("localhost:2181", KafkaTestUtil.getZkServer().connectString());

    //kafka to flume pipeline
    uri = Resources.getResource("cluster_kafka_flume.json").toURI();
    String kafkaToFlume =  new String(Files.readAllBytes(Paths.get(uri)), StandardCharsets.UTF_8);
    kafkaToFlume = kafkaToFlume.replace("topicName", TOPIC1);
    kafkaToFlume = kafkaToFlume.replaceAll("localhost:9092", KafkaTestUtil.getMetadataBrokerURI());
    kafkaToFlume = kafkaToFlume.replaceAll("localhost:2181", KafkaTestUtil.getZkConnect());
    kafkaToFlume = kafkaToFlume.replaceAll("localhost:9050", "localhost:" + flumePort);
    kafkaToFlume = kafkaToFlume.replaceAll("CLUSTER", "STANDALONE");

    //kafka to hdfs pipeline
    uri = Resources.getResource("cluster_kafka_hdfs.json").toURI();
    String kafkaToHDFS =  new String(Files.readAllBytes(Paths.get(uri)), StandardCharsets.UTF_8);
    kafkaToHDFS = kafkaToHDFS.replace("topicName", TOPIC2);
    kafkaToHDFS = kafkaToHDFS.replaceAll("localhost:9092", KafkaTestUtil.getMetadataBrokerURI());
    kafkaToHDFS = kafkaToHDFS.replaceAll("localhost:2181", KafkaTestUtil.getZkConnect());
    kafkaToHDFS = kafkaToHDFS.replaceAll("CLUSTER", "STANDALONE");
    kafkaToHDFS = kafkaToHDFS.replaceAll("/uri", miniDFS.getURI().toString());

    return ImmutableList.of(randomToKafka, kafkaToFlume, kafkaToHDFS);
  }

  @Override
  protected Map<String, String> getPipelineNameAndRev() {
    return ImmutableMap.of("kafka_destination_pipeline", "0", "kafka_origin_pipeline_cluster", "0", "cluster_kafka_hdfs", "0");
  }

  /**
   * The extending test must call this method in the method scheduled to run before class
   * @throws Exception
   */
  @BeforeClass
  public static void beforeClass() throws Exception {

    //setup kafka to read from
    KafkaTestUtil.startZookeeper();
    KafkaTestUtil.startKafkaBrokers(1);

    KafkaTestUtil.createTopic(TOPIC1, 1, 1);
    KafkaTestUtil.createTopic(TOPIC2, 1, 1);
    KafkaTestUtil.createTopic(TOPIC3, 1, 1);

    producer1 = KafkaTestUtil.createProducer(KafkaTestUtil.getMetadataBrokerURI(), true);
    producer2 = KafkaTestUtil.createProducer(KafkaTestUtil.getMetadataBrokerURI(), true);

    e = Executors.newFixedThreadPool(2);
    e.submit(new Runnable() {
      @Override
      public void run() {
        int index = 0;
        while (true) {
          producer1.send(new KeyedMessage<>(TOPIC1, "0", "Hello Kafka" + index));
          ThreadUtil.sleep(200);
          index = (index+1)%10;
        }
      }
    });

    e.submit(new Runnable() {
      @Override
      public void run() {
        int index = 0;
        while (true) {
          producer2.send(new KeyedMessage<>(TOPIC2, "0", "Hello Kafka" + index));
          ThreadUtil.sleep(200);
          index = (index+1)%10;
        }
      }
    });

    //setup flume to write to
    source = new AvroSource();
    ch = new MemoryChannel();
    Configurables.configure(ch, new Context());

    Context context = new Context();
    //This should match whats present in the pipeline.json file
    flumePort = TestUtil.getFreePort();
    context.put("port", String.valueOf(flumePort));
    context.put("bind", "localhost");
    Configurables.configure(source, context);

    List<Channel> channels = new ArrayList<>();
    channels.add(ch);
    ChannelSelector rcs = new ReplicatingChannelSelector();
    rcs.setChannels(channels);
    source.setChannelProcessor(new ChannelProcessor(rcs));
    source.start();

    //HDFS settings
    // setting some dummy kerberos settings to be able to test a mis-setting
    System.setProperty("java.security.krb5.realm", "foo");
    System.setProperty("java.security.krb5.kdc", "localhost:0");

    File minidfsDir = new File("target/minidfs").getAbsoluteFile();
    if (!minidfsDir.exists()) {
      Assert.assertTrue(minidfsDir.mkdirs());
    }
    System.setProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA, minidfsDir.getPath());
    Configuration conf = new HdfsConfiguration();
    conf.set("hadoop.proxyuser." + System.getProperty("user.name") + ".hosts", "*");
    conf.set("hadoop.proxyuser." + System.getProperty("user.name") + ".groups", "*");
    UserGroupInformation.createUserForTesting("foo", new String[]{"all", "supergroup"});
    EditLogFileOutputStream.setShouldSkipFsyncForTesting(true);
    miniDFS = new MiniDFSCluster.Builder(conf).build();

    MultiplePipelinesBaseIT.beforeClass(getPipelineJson());
  }

  @AfterClass
  public static void afterClass() throws Exception {
    e.shutdownNow();
    if (miniDFS != null) {
      miniDFS.shutdown();
      miniDFS = null;
    }
    source.stop();
    ch.stop();
    KafkaTestUtil.shutdown();
    MultiplePipelinesBaseIT.afterClass();
  }
}
