/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.multiple;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.streamsets.datacollector.MiniSDC;
import com.streamsets.datacollector.MiniSDCTestingUtility;
import com.streamsets.datacollector.util.TestUtil;
import com.streamsets.datacollector.util.VerifyUtils;
import com.streamsets.pipeline.lib.KafkaTestUtil;
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
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

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
public class TestMultiplePipelines {

  private static MiniSDCTestingUtility miniSDCTestingUtility;
  private static URI serverURI;
  private static MiniSDC miniSDC;

  private static final String TOPIC1 = "KafkaToFlume";
  private static final String TOPIC2 = "KafkaToHDFS";
  private static final String TOPIC3 = "randomToKafka";

  //Flume destination related
  private static AvroSource source;
  private static Channel ch;
  private static Producer<String, String> producer1;
  private static Producer<String, String> producer2;
  private static int flumePort;

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

  private Map<String, String> getPipelineNameAndRev() {
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

    ExecutorService e = Executors.newFixedThreadPool(2);
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
    UserGroupInformation.createUserForTesting("foo", new String[]{ "all", "supergroup"});
    EditLogFileOutputStream.setShouldSkipFsyncForTesting(true);
    miniDFS = new MiniDFSCluster.Builder(conf).build();

    System.setProperty("sdc.testing-mode", "true");
    miniSDCTestingUtility = new MiniSDCTestingUtility();
    miniSDC = miniSDCTestingUtility.createMiniSDC(MiniSDC.ExecutionMode.STANDALONE);
    miniSDC.startSDC();
    serverURI = miniSDC.getServerURI();
    for (String pipelineJson : getPipelineJson()) {
      miniSDC.createPipeline(pipelineJson);
    }
  }

  /**
   * The extending test must call this method in the method scheduled to run after class
   * @throws Exception
   */
  @AfterClass
  public static void afterClass() throws Exception {
    miniSDCTestingUtility.stopMiniSDC();
  }

  @Before
  public void setUp() throws IOException, InterruptedException {
    //sequential pipeline starts
    for(Map.Entry<String, String> e : getPipelineNameAndRev().entrySet()) {
      VerifyUtils.startPipeline(serverURI, e.getKey(), e.getValue());
      VerifyUtils.waitForPipelineToStart(serverURI, e.getKey(), e.getValue());
    }
  }

  @After
  public void tearDown() throws IOException, InterruptedException {
    //stops
    for(Map.Entry<String, String> e : getPipelineNameAndRev().entrySet()) {
      VerifyUtils.stopPipeline(serverURI, e.getKey(), e.getValue());
      VerifyUtils.waitForPipelineToStop(serverURI, e.getKey(), e.getValue());
    }
  }

  /**********************************************************/
  /************************* tests **************************/
  /**********************************************************/

  //The following tests can be bumped up to the Base class to be available for cluster mode once we have a way to
  // detect that the pipeline is running in the worker nodes.
  //As of now even though the state says RUNNING the worker nodes may not have started processing the data.

  @Test
  public void testRestartAndHistory() throws Exception {

    //Almost-simultaneous stops
    for(Map.Entry<String, String> e : getPipelineNameAndRev().entrySet()) {
      VerifyUtils.stopPipeline(serverURI, e.getKey(), e.getValue());
    }
    for(Map.Entry<String, String> e : getPipelineNameAndRev().entrySet()) {
      VerifyUtils.waitForPipelineToStop(serverURI, e.getKey(), e.getValue());
      Assert.assertEquals("STOPPED", VerifyUtils.getPipelineState(serverURI, e.getKey(), e.getValue()));
    }

    //clear history
    for(Map.Entry<String, String> e : getPipelineNameAndRev().entrySet()) {
      VerifyUtils.deleteHistory(serverURI, e.getKey(), e.getValue());
      List<Map<String, Object>> history = VerifyUtils.getHistory(serverURI, e.getKey(), e.getValue());
      Assert.assertEquals(0, history.size());
    }

    //fresh almost-simultaneous starts
    for(Map.Entry<String, String> e : getPipelineNameAndRev().entrySet()) {
      VerifyUtils.startPipeline(serverURI, e.getKey(), e.getValue());
    }
    for(Map.Entry<String, String> e : getPipelineNameAndRev().entrySet()) {
      VerifyUtils.waitForPipelineToStart(serverURI, e.getKey(), e.getValue());
      Assert.assertEquals("RUNNING", VerifyUtils.getPipelineState(serverURI, e.getKey(), e.getValue()));
    }

    //Almost-simultaneous stops
    for(Map.Entry<String, String> e : getPipelineNameAndRev().entrySet()) {
      VerifyUtils.stopPipeline(serverURI, e.getKey(), e.getValue());
    }
    for(Map.Entry<String, String> e : getPipelineNameAndRev().entrySet()) {
      VerifyUtils.waitForPipelineToStop(serverURI, e.getKey(), e.getValue());
      Assert.assertEquals("STOPPED", VerifyUtils.getPipelineState(serverURI, e.getKey(), e.getValue()));
    }

    //fresh almost-simultaneous starts
    for(Map.Entry<String, String> e : getPipelineNameAndRev().entrySet()) {
      VerifyUtils.startPipeline(serverURI, e.getKey(), e.getValue());
    }
    for(Map.Entry<String, String> e : getPipelineNameAndRev().entrySet()) {
      VerifyUtils.waitForPipelineToStart(serverURI, e.getKey(), e.getValue());
      Assert.assertEquals("RUNNING", VerifyUtils.getPipelineState(serverURI, e.getKey(), e.getValue()));
    }

    //Almost-simultaneous stops
    for(Map.Entry<String, String> e : getPipelineNameAndRev().entrySet()) {
      VerifyUtils.stopPipeline(serverURI, e.getKey(), e.getValue());
    }
    for(Map.Entry<String, String> e : getPipelineNameAndRev().entrySet()) {
      VerifyUtils.waitForPipelineToStop(serverURI, e.getKey(), e.getValue());
      Assert.assertEquals("STOPPED", VerifyUtils.getPipelineState(serverURI, e.getKey(), e.getValue()));
    }

    for(Map.Entry<String, String> e : getPipelineNameAndRev().entrySet()) {
      List<Map<String, Object>> history = VerifyUtils.getHistory(serverURI, e.getKey(), e.getValue());
      Assert.assertEquals(8, history.size());
    }

    //sequential pipeline starts
    for(Map.Entry<String, String> e : getPipelineNameAndRev().entrySet()) {
      VerifyUtils.startPipeline(serverURI, e.getKey(), e.getValue());
      VerifyUtils.waitForPipelineToStart(serverURI, e.getKey(), e.getValue());
    }
  }

  @Test
  public void testCaptureSnapshot() throws Exception {
    for(Map.Entry<String, String> nameAndRev : getPipelineNameAndRev().entrySet()) {
      String snapShotName = "snapShot_" + nameAndRev.getKey() + "_" + nameAndRev.getValue();

      VerifyUtils.captureSnapshot(serverURI, nameAndRev.getKey(), nameAndRev.getValue(), snapShotName, 1);
      VerifyUtils.waitForSnapshot(serverURI, nameAndRev.getKey(), nameAndRev.getValue(), snapShotName);

      Map<String, List<List<Map<String, Object>>>> snapShot = VerifyUtils.getSnapShot(serverURI, nameAndRev.getKey(),
        nameAndRev.getValue(), snapShotName);

      List<Map<String, Object>> stageOutputs = snapShot.get("snapshotBatches").get(0);
      Assert.assertNotNull(stageOutputs);

      for (Map<String, Object> stageOutput : stageOutputs) {
        Map<String, Object> output = (Map<String, Object>) stageOutput.get("output");
        for (Map.Entry<String, Object> e : output.entrySet()) {
          Assert.assertTrue(e.getValue() instanceof List);
          List<Map<String, Object>> records = (List<Map<String, Object>>) e.getValue();
          Assert.assertFalse("No records for pipeline" + e.getKey(), records.isEmpty());
          //This is the list of records
          for (Map<String, Object> record : records) {
            //each record has header and value
            Map<String, Object> val = (Map<String, Object>) record.get("value");
            Assert.assertNotNull(val);
            //value has root field with path "", and Map with key "text" for the text field
            Assert.assertTrue(val.containsKey("value"));
            Map<String, Map<String, String>> value = (Map<String, Map<String, String>>) val.get("value");
            Assert.assertNotNull(value);
            //The text field in the record [/text]
            if(value.containsKey("text")) {
              //Kafka origin pipelines generate record with text data.
              //Additional tests for those
              Map<String, String> text = value.get("text");
              Assert.assertNotNull(text);
              //Field has type, path and value
              Assert.assertTrue(text.containsKey("value"));
              Assert.assertTrue(text.get("value").contains("Hello Kafka"));
              Assert.assertTrue(text.containsKey("path"));
              Assert.assertEquals("/text", text.get("path"));
              Assert.assertTrue(text.containsKey("type"));
              Assert.assertEquals("STRING", text.get("type"));
            }
          }
        }
      }
    }
  }

  @Test()
  public void testMetrics() throws Exception {
    for(Map.Entry<String, String> e : getPipelineNameAndRev().entrySet()) {
      Assert.assertEquals("RUNNING", VerifyUtils.getPipelineState(serverURI, e.getKey(), e.getValue()));
      Thread.sleep(2000);
      Map<String, Map<String, Object>> metrics = VerifyUtils.getCountersFromMetrics(serverURI, e.getKey(), e.getValue());

      Assert.assertTrue(VerifyUtils.getSourceOutputRecords(metrics) > 0);
      Assert.assertTrue(VerifyUtils.getSourceInputRecords(metrics) == 0);
      Assert.assertTrue(VerifyUtils.getSourceErrorRecords(metrics) == 0);
      Assert.assertTrue(VerifyUtils.getSourceStageErrors(metrics) == 0);

      Assert.assertTrue("No target output records for pipeline " + e.getKey(), VerifyUtils.getTargetOutputRecords(metrics) > 0);
      Assert.assertTrue(VerifyUtils.getTargetInputRecords(metrics) > 0);
      Assert.assertTrue(VerifyUtils.getTargetErrorRecords(metrics) == 0);
      Assert.assertTrue(VerifyUtils.getTargetStageErrors(metrics) == 0);
    }
  }

}
