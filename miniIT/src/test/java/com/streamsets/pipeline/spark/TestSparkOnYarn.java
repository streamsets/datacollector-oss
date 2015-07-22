/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.spark;

import com.google.common.io.Resources;
import com.streamsets.datacollector.MiniSDC;
import com.streamsets.pipeline.MiniSDCTestingUtility;
import com.streamsets.pipeline.lib.KafkaTestUtil;
import com.streamsets.pipeline.util.ClusterUtil;
import com.streamsets.pipeline.util.VerifyUtils;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class TestSparkOnYarn {
  private static final Logger LOG = LoggerFactory.getLogger(TestSparkOnYarn.class);
  private static MiniYARNCluster miniYarnCluster;
  private Producer<String, String> producer;
  private static final String TEST_NAME = "SparkOnYarnKafkaSource";
  private static MiniSDCTestingUtility miniSDCTestingUtility;
  private static String pipelineJson;
  private static final String SPARK_PROPERTY_FILE = "SPARK_PROPERTY_FILE";
  // This should be the same topic as in cluster_pipeline.json
  private static final String TOPIC_NAME = "testProduceStringRecords";

  @BeforeClass
  public static void setup() throws Exception {
    System.setProperty(MiniSDCTestingUtility.PRESERVE_TEST_DIR, "true");
    miniSDCTestingUtility = new MiniSDCTestingUtility();
    File dataTestDir = miniSDCTestingUtility.getDataTestDir();

    File sparkHome = ClusterUtil.createSparkHome(dataTestDir);

    YarnConfiguration entries = new YarnConfiguration();
    miniYarnCluster = miniSDCTestingUtility.startMiniYarnCluster(TEST_NAME, 1, 1, 1, entries);

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
    KafkaTestUtil.startZookeeper();
    KafkaTestUtil.startKafkaBrokers(1);
    pipelineJson  = pipelineJson.replaceAll("localhost:9092", KafkaTestUtil.getMetadataBrokerURI());
    pipelineJson  = pipelineJson.replaceAll("localhost:2181", KafkaTestUtil.getZkConnect());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (miniSDCTestingUtility != null) {
      miniSDCTestingUtility.stopMiniSDC();
      ClusterUtil.killYarnApp(TEST_NAME);
      miniSDCTestingUtility.stopMiniYarnCluster();
      miniSDCTestingUtility.cleanupTestDir();
      ClusterUtil.cleanUpYarnDirs(TEST_NAME);
    }
    KafkaTestUtil.shutdown();
  }

  @Test (timeout=240000)
  public void testSparkOnYarnWithKafkaProducer() throws Exception {
    System.setProperty("sdc.testing-mode", "true");

    // Produce records in kafka
    int expectedRecords = 30;
    produceRecords(expectedRecords);

    MiniSDC miniSDC = null;
    try {
      miniSDC = miniSDCTestingUtility.createMiniSDC(MiniSDC.ExecutionMode.CLUSTER);
      miniSDC.startSDC();
      miniSDC.createAndStartPipeline(pipelineJson);
      URI serverURI = miniSDC.getServerURI();
      LOG.info("Starting on URI " + serverURI);
      // TODO - Start a new thread listening for slave metrics
      Thread.sleep(60000);
      List<URI> list = miniSDC.getListOfSlaveSDCURI();
      assertTrue(list != null && !list.isEmpty());
      Map<String, Map<String, Integer>> countersMap = VerifyUtils.getCounters(list);
      assertNotNull(countersMap);
      assertEquals("Output records counters for source should be equal to " + expectedRecords, expectedRecords,
        VerifyUtils.getSourceOutputRecords(countersMap));
      assertEquals("Output records counters for target should be equal to " + expectedRecords, expectedRecords,
        VerifyUtils.getTargetOutputRecords(countersMap));
    } finally {
      if (miniSDC != null) {
        miniSDC.stop();
      }
    }
  }

  private void produceRecords(int records) throws InterruptedException {
    producer = KafkaTestUtil.createProducer(KafkaTestUtil.getMetadataBrokerURI(), false);
    KafkaTestUtil.createTopic(TOPIC_NAME, 1, 1);
    LOG.info("Start producing records");
    int i = 0;
    while (i < records) {
      producer.send(new KeyedMessage<>(TOPIC_NAME, "0", "Hello Kafka"));
      i++;
    }
  }
}
