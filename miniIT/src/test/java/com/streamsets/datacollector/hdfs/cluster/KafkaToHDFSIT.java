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
package com.streamsets.datacollector.hdfs.cluster;

import com.google.common.io.Resources;
import com.streamsets.datacollector.MiniSDC;
import com.streamsets.datacollector.util.ClusterUtil;
import com.streamsets.datacollector.util.TestUtil;
import com.streamsets.datacollector.util.VerifyUtils;
import com.streamsets.pipeline.kafka.common.KafkaTestUtil;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileOutputStream;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

/**
 * The pipeline and data provider for this test case should make sure that the pipeline runs continuously and the origin
 *  in the pipeline keeps producing records until stopped.
 *
 * Origin has to be Kafka as of now. Make sure that there is continuous supply of data for the pipeline to keep running.
 * For example to test kafka Origin, a background thread could keep writing to the kafka topic from which the
 * kafka origin reads.
 *
 */
public class KafkaToHDFSIT {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaToHDFSIT.class);
  //Kafka messages contain text "Hello Kafka<i>" i in the range [0-29]
  private static int RECORDS_PRODUCED = 30;
  //Based on the expression parser and stream selector target ends up with messages which have even first digit of i.
  //i.e., Hello Kafka<0,2,4,6,8,20,21,22,23,24,25,26,27,28,29>
  private static int RECORDS_REACHING_TARGET= 15;

  protected static URI serverURI;
  protected static MiniSDC miniSDC;
  private static final String TOPIC = "KafkaToHDFSOnCluster";
  private static Producer<String, String> producer;

  private static MiniDFSCluster miniDFS;

  private static final String TEST_NAME = "KafkaToHDFSOnCluster";

  @BeforeClass
  public static void beforeClass() throws Exception {
    //setup kafka to read from
    KafkaTestUtil.startZookeeper();
    KafkaTestUtil.startKafkaBrokers(1);
    KafkaTestUtil.createTopic(TOPIC, 1, 1);
    producer = KafkaTestUtil.createProducer(KafkaTestUtil.getMetadataBrokerURI(), true);
    produceRecords(RECORDS_PRODUCED);

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

    //setup Cluster and start pipeline
    YarnConfiguration entries = new YarnConfiguration();
    //TODO: Investigate why this is required for test to pass. Is yarn messing with the miniDFS cluster configuration?
    entries.set("hadoop.proxyuser." + System.getProperty("user.name") + ".hosts", "*");
    entries.set("hadoop.proxyuser." + System.getProperty("user.name") + ".groups", "*");
    ClusterUtil.setupCluster(TEST_NAME, getPipelineJson(), entries);
    serverURI = ClusterUtil.getServerURI();
    miniSDC = ClusterUtil.getMiniSDC();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    KafkaTestUtil.shutdown();
    if (miniDFS != null) {
      miniDFS.shutdown();
      miniDFS = null;
    }
    ClusterUtil.tearDownCluster(TEST_NAME);
  }

  private static String getPipelineJson() throws Exception {
    URI uri = Resources.getResource("cluster_kafka_hdfs.json").toURI();
    String pipelineJson =  new String(Files.readAllBytes(Paths.get(uri)), StandardCharsets.UTF_8);
    pipelineJson = pipelineJson.replace("topicName", TOPIC);
    pipelineJson = pipelineJson.replaceAll("localhost:9092", KafkaTestUtil.getMetadataBrokerURI());
    pipelineJson = pipelineJson.replaceAll("localhost:2181", KafkaTestUtil.getZkConnect());
    pipelineJson = pipelineJson.replaceAll("/uri", miniDFS.getURI().toString());
    return pipelineJson;
  }

  private static void produceRecords(int records) throws InterruptedException {
    int i = 0;
    while (i < records) {
      producer.send(new KeyedMessage<>(TOPIC, "0", "Hello Kafka" + i));
      i++;
    }
  }

  @Test(timeout=120000)
  public void testKafkaToHDFSOnCluster() throws Exception {
    List<URI> list = miniSDC.getListOfSlaveSDCURI();
    Assert.assertTrue(list != null && !list.isEmpty());

    Map<String, Map<String, Object>> countersMap = VerifyUtils.getCounters(list, "cluster_kafka_hdfs", "0");
    Assert.assertNotNull(countersMap);
    while (VerifyUtils.getSourceOutputRecords(countersMap) != RECORDS_PRODUCED) {
      LOG.debug("Source output records are not equal to " + RECORDS_PRODUCED + " retrying again");
      Thread.sleep(500);
      countersMap = VerifyUtils.getCounters(list, "cluster_kafka_hdfs", "0");
      Assert.assertNotNull(countersMap);
    }
    while (VerifyUtils.getTargetInputRecords(countersMap) != RECORDS_REACHING_TARGET) {
      LOG.debug("Target Input records are not equal to " + RECORDS_REACHING_TARGET + " retrying again");
      Thread.sleep(500);
      countersMap = VerifyUtils.getCounters(list, "cluster_kafka_hdfs", "0");
      Assert.assertNotNull(countersMap);
    }
    //HDFS configuration is set to roll file after 15 records.
    int recordsRead = 0;
    DistributedFileSystem fileSystem = miniDFS.getFileSystem();
    FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/tmp/out/" + TestUtil.getCurrentYear()));
    for(FileStatus f : fileStatuses) {
      BufferedReader br=new BufferedReader(new InputStreamReader(fileSystem.open(f.getPath())));
      String line = br.readLine();
      while (line != null) {
        Assert.assertTrue(line.contains("Hello Kafka"));
        int j = Integer.parseInt(line.substring(11, 12));
        Assert.assertTrue(j%2 == 0);
        recordsRead++;
        line=br.readLine();
      }
    }

    Assert.assertEquals(RECORDS_REACHING_TARGET, recordsRead);
  }

}
