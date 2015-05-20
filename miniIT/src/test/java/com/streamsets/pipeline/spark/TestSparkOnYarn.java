/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.spark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import kafka.admin.AdminUtils;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.TestZKUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.zk.EmbeddedZookeeper;

import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import com.streamsets.pipeline.MiniSDC;
import com.streamsets.pipeline.MiniSDC.ExecutionMode;
import com.streamsets.pipeline.MiniSDCTestingUtility;
import com.streamsets.pipeline.lib.KafkaTestUtil;
import com.streamsets.pipeline.util.UntarUtility;
import com.streamsets.pipeline.util.VerifyUtils;

public class TestSparkOnYarn {
  private static final Logger LOG = LoggerFactory.getLogger(TestSparkOnYarn.class);
  private static MiniYARNCluster miniYarnCluster;
  private static ZkClient zkClient;
  private static KafkaServer kafkaServer;
  private static int port;
  private Producer<String, String> producer;
  private static final String TESTNAME = "SparkOnYarnKafkaSource";
  private static MiniSDCTestingUtility miniSDCTestingUtility;
  private static String pipelineJson;
  private static final String SPARK_PROPERTY_FILE = "SPARK_PROPERTY_FILE";
  private static final String SPARK_TEST_HOME = "SPARK_TEST_HOME";
  // This should be the same topic as in cluster_pipeline.json
  private static final String TOPIC_NAME = "testProduceStringRecords";

  @BeforeClass
  public static void setup() throws Exception {
    System.setProperty(MiniSDCTestingUtility.PRESERVE_TEST_DIR, "true");
    miniSDCTestingUtility = new MiniSDCTestingUtility();
    File dataTestDir = miniSDCTestingUtility.getDataTestDir();
    File sparkDir = new File(new File(System.getProperty("user.dir"), "target"), "spark");
    if (!sparkDir.exists()) {
      throw new RuntimeException("'Cannot find spark assembly dir at location " + sparkDir.getAbsolutePath());
    }
    File sparkHome = new File(dataTestDir, "spark");
    System.setProperty(SPARK_TEST_HOME, sparkHome.getAbsolutePath());
    FileUtils.copyDirectory(sparkDir, sparkHome);

    miniYarnCluster = miniSDCTestingUtility.startMiniYarnCluster(TESTNAME, 1, 1, 1);

    Configuration config = miniYarnCluster.getConfig();
    long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10);
    while (config.get(YarnConfiguration.RM_ADDRESS).split(":")[1] == "0") {
      if (System.currentTimeMillis() > deadline) {
        throw new IllegalStateException("Timed out waiting for RM to come up.");
      }
      LOG.debug("RM address still not set in configuration, waiting...");
      TimeUnit.MILLISECONDS.sleep(100);
    }
    LOG.debug("RM at " + config.get(YarnConfiguration.RM_ADDRESS));

    Properties sparkHadoopProps = new Properties();

    for (Map.Entry<String, String> entry : config) {
      sparkHadoopProps.setProperty("spark.hadoop." + entry.getKey(), entry.getValue());
    }


    LOG.debug("Creating spark properties file at " + dataTestDir);
    File propertiesFile =  new File(dataTestDir, "spark.properties");
    propertiesFile.createNewFile();
    FileOutputStream sdcOutStream = new FileOutputStream(propertiesFile);
    sparkHadoopProps.store(sdcOutStream, null);
    sdcOutStream.flush();
    sdcOutStream.close();
    // Need to pass this property file to spark-submit for it pick up yarn confs
    System.setProperty(SPARK_PROPERTY_FILE, propertiesFile.getAbsolutePath());

    URI uri = Resources.getResource("cluster_pipeline.json").toURI();
    pipelineJson = new String(Files.readAllBytes(Paths.get(uri)), StandardCharsets.UTF_8);
    // TODO - Move setup of Kafka in separate class
    setupKafka();

    File sparkBin = new File(sparkHome, "bin");
    for (File file : sparkBin.listFiles()) {
      MiniSDCTestingUtility.setExecutePermission(file.toPath());
    }
  }

  private static void setupKafka() {
    String zkConnect = TestZKUtils.zookeeperConnect();
    EmbeddedZookeeper zkServer = new EmbeddedZookeeper(zkConnect);
    zkClient = new ZkClient(zkServer.connectString(), 30000, 30000, ZKStringSerializer$.MODULE$);
    // setup Broker
    port = TestUtils.choosePort();
    Properties props = TestUtils.createBrokerConfig(0, port);
    kafkaServer = TestUtils.createServer(new KafkaConfig(props), new MockTime());
    String metadataBrokerURI = "localhost" + ":" + port;
    LOG.info("Setting metadataBrokerList and auto.offset.reset for test case");

    // remove this hack once we bring in container classes for parsing json
    if (!pipelineJson.contains("localhost:9092"))  {
      throw new RuntimeException("Bailing out, default value of metadataBrokerlist must have changed in pipeline json file");
    }
    if (!pipelineJson.contains("localhost:2181")) {
      throw new RuntimeException("Bailing out, default value of zookeeperConnect must have changed in pipeline json file");
    }
    pipelineJson  = pipelineJson.replaceAll("localhost:9092", metadataBrokerURI);
    pipelineJson  = pipelineJson.replaceAll("localhost:2181", zkConnect);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (miniSDCTestingUtility != null) {
      miniSDCTestingUtility.stopMiniSDC();
      killYarnApp();
      miniSDCTestingUtility.stopMiniYarnCluster();
      miniSDCTestingUtility.cleanupTestDir();
      cleanUpYarnDirs();
    }
    if (kafkaServer != null) {
      kafkaServer.shutdown();
    }
    if (zkClient != null) {
      zkClient.close();
    }

  }

  private static void cleanUpYarnDirs() throws IOException {
    if (!Boolean.getBoolean(MiniSDCTestingUtility.PRESERVE_TEST_DIR)) {
      MiniSDCTestingUtility.deleteDir(new File(new File(System.getProperty("user.dir"), "target"), TESTNAME));
    }
  }

  private static void killYarnApp() throws Exception {
    // TODO - remove this hack
    // We dont know app id, but yarn creates its data dir under $HOME/target/TESTNAME, so kill the process by
    // grep for the yarn testname
    String killCmd = signalCommand(TESTNAME, "SIGKILL");
    LOG.info("Signal kill command to yarn app " + killCmd);
    String[] killCommand = new String[] { "/usr/bin/env", "bash", "-c", killCmd };
    Process p = Runtime.getRuntime().exec(killCommand);
    p.waitFor();
    BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
    String line = "";
    LOG.info("Process output is ");
    while ((line = reader.readLine()) != null) {
      LOG.debug(line + "\n");
    }
  }

  private static String findPidCommand(String service) {
    return String.format("ps aux | grep %s | grep -v grep | tr -s ' ' | cut -d ' ' -f2", service);
  }

  private static String signalCommand(String service, String signal) {
    return String.format("%s | xargs kill -s %s", findPidCommand(service), signal);
  }

  @Test (timeout=240000)
  public void testSparkOnYarnWithKafkaProducer() throws Exception {
    System.setProperty("sdc.testing-mode", "true");

    // Produce records in kafka
    int expectedRecords = 30;
    produceRecords(expectedRecords);

    MiniSDC miniSDC = null;
    try {
      miniSDC = miniSDCTestingUtility.startMiniSDC(pipelineJson, ExecutionMode.CLUSTER);
      URI serverURI = miniSDC.getServerURI();
      LOG.info("Starting on URI " + serverURI);
      // TODO - Start a new thread listening for slave metrics
      Thread.sleep(60000);
      List<URI> list = miniSDC.getListOfSlaveSDCURI();
      assertTrue(list != null && !list.isEmpty());
      Map<String, Map<String, Integer>> countersMap = VerifyUtils.getCounters(list);
      assertNotNull(countersMap);
      assertEquals("Output records counters for source should be equal to " + expectedRecords, expectedRecords,
        VerifyUtils.getSourceCounters(countersMap));
      assertEquals("Output records counters for target should be equal to " + expectedRecords, expectedRecords,
        VerifyUtils.getTargetCounters(countersMap));
    } finally {
      if (miniSDC != null) {
        miniSDC.stop();
      }
    }
  }

  private void produceRecords(int records) throws InterruptedException {
    producer = KafkaTestUtil.createProducer("localhost", port, false);
    AdminUtils.createTopic(zkClient, TOPIC_NAME, 1, 1, new Properties());
    TestUtils.waitUntilMetadataIsPropagated(
      scala.collection.JavaConversions.asScalaBuffer(ImmutableList.of(kafkaServer)), TOPIC_NAME, 0,
      2000);
    LOG.info("Start producing records");
    int i = 0;
    while (i < records) {
      producer.send(new KeyedMessage<>(TOPIC_NAME, "0", "Hello Kafka"));
      i++;
    }
  }
}
