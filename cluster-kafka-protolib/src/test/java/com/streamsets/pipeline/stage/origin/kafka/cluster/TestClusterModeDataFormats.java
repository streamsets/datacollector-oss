/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.kafka.cluster;

import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.CsvRecordType;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.impl.Pair;
import com.streamsets.pipeline.kafka.common.DataType;
import com.streamsets.pipeline.kafka.common.KafkaTestUtil;
import com.streamsets.pipeline.lib.json.StreamingJsonParser;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.origin.kafka.BaseKafkaSource;
import com.streamsets.pipeline.stage.origin.kafka.ClusterKafkaSourceFactory;
import com.streamsets.pipeline.stage.origin.kafka.KafkaConfigBean;
import com.streamsets.pipeline.stage.origin.kafka.KafkaSourceFactory;
import kafka.admin.AdminUtils;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.TestZKUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;


public class TestClusterModeDataFormats {
  private static final Logger LOG = LoggerFactory.getLogger(TestClusterModeDataFormats.class);

  private static List<KafkaServer> kafkaServers;
  private static ZkClient zkClient;
  private static EmbeddedZookeeper zkServer;
  private static int port;
  private static String zkConnect;

  private static final String HOST = "localhost";
  private static final int BROKER_1_ID = 0;
  private static final int SINGLE_PARTITION = 1;
  private static final int SINGLE_REPLICATION_FACTOR = 1;
  private static final String TOPIC1 = "TestKafkaSource1";
  private static final int TIME_OUT = 5000;

  private static String metadataBrokerList;
  private static String originalTmpDir;

  @BeforeClass
  public static void setUp() throws Exception {
    //Init zookeeper
    originalTmpDir = System.getProperty("java.io.tmpdir");
    File testDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(testDir.mkdirs());
    System.setProperty("java.io.tmpdir", testDir.getAbsolutePath());

    zkConnect = TestZKUtils.zookeeperConnect();
    zkServer = new EmbeddedZookeeper(zkConnect);

    LOG.info("ZooKeeper log dir : " + zkServer.logDir().getAbsolutePath());
    zkClient = new ZkClient(zkServer.connectString(), 30000, 30000, ZKStringSerializer$.MODULE$);
    // setup Broker
    port = TestUtils.choosePort();
    kafkaServers = new ArrayList<>(3);
    Properties props1 = TestUtilsBridge.createBrokerConfig(BROKER_1_ID, port, true, false);

    kafkaServers.add(TestUtils.createServer(new KafkaConfig(props1), new MockTime()));
    // create topic
    AdminUtils.createTopic(zkClient, TOPIC1, SINGLE_PARTITION, SINGLE_REPLICATION_FACTOR, new Properties());
    TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(kafkaServers), TOPIC1, 0, TIME_OUT);

    metadataBrokerList = "localhost:" + port;

  }

  @AfterClass
  public static void tearDown() {
    for(KafkaServer kafkaServer : kafkaServers) {
      kafkaServer.shutdown();
    }
    zkClient.close();
    zkServer.shutdown();
    System.setProperty("java.io.tmpdir", originalTmpDir);
  }

  private BaseKafkaSource createSource(KafkaConfigBean conf) {
    KafkaSourceFactory factory = new ClusterKafkaSourceFactory(conf);
    return factory.create();
  }

  @Test
  public void testProduceStringRecords() throws Exception {
    KafkaConfigBean conf = new KafkaConfigBean();
    conf.metadataBrokerList = metadataBrokerList;
    conf.topic = TOPIC1;
    conf.consumerGroup = "dummyGroup";
    conf.zookeeperConnect = zkConnect;
    conf.maxBatchSize = 1000;
    conf.maxWaitTime = 10000;
    conf.kafkaConsumerConfigs = null;
    conf.produceSingleRecordPerMessage = false;
    conf.dataFormat = DataFormat.TEXT;
    conf.dataFormatConfig.charset = "UTF-8";
    conf.dataFormatConfig.removeCtrlChars = false;
    conf.dataFormatConfig.textMaxLineLen = 4096;

    SourceRunner sourceRunner = new SourceRunner.Builder(ClusterKafkaSource.class, createSource(conf))
      .addOutputLane("lane")
      .setExecutionMode(ExecutionMode.CLUSTER_YARN_STREAMING)
      .build();
    Thread th = null;
    try {
      sourceRunner.runInit();
      List<Map.Entry> list = new ArrayList<Map.Entry>();
      list.add(new Pair("1".getBytes(), "aaa".getBytes()));
      list.add(new Pair("2".getBytes(), "bbb".getBytes()));
      list.add(new Pair("1".getBytes(), "ccc".getBytes()));
      th = createThreadForAddingBatch(sourceRunner, list);

      StageRunner.Output output = sourceRunner.runProduce(null, 5);

      String newOffset = output.getNewOffset();
      Assert.assertEquals("kafka::TestKafkaSource1::2", newOffset);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertEquals(3, records.size());

      for (int i = 0; i < records.size(); i++) {
        Assert.assertNotNull(records.get(i).get("/text"));
        LOG.info("Header " + records.get(i).getHeader().getSourceId());
        Assert.assertTrue(!records.get(i).get("/text").getValueAsString().isEmpty());
        Assert.assertEquals(new String((byte[])list.get(i).getValue()), records.get(i).get("/text").getValueAsString());
      }

    if (sourceRunner != null) {
      sourceRunner.runDestroy();
    }}
    finally {
      if (th != null) {
        th.interrupt();
      }
    }
  }

  @Test
  public void testProduceJsonRecordsMultipleObjectsMultipleRecord() throws StageException, IOException {
    KafkaConfigBean conf = new KafkaConfigBean();
    conf.metadataBrokerList = metadataBrokerList;
    conf.topic = TOPIC1;
    conf.consumerGroup = "dummyGroup";
    conf.zookeeperConnect = zkConnect;
    conf.maxBatchSize = 1000;
    conf.maxWaitTime = 10000;
    conf.kafkaConsumerConfigs = null;
    conf.produceSingleRecordPerMessage = false;
    conf.dataFormat = DataFormat.JSON;
    conf.dataFormatConfig.charset = "UTF-8";
    conf.dataFormatConfig.removeCtrlChars = false;
    conf.dataFormatConfig.jsonContent = JsonMode.MULTIPLE_OBJECTS;
    conf.dataFormatConfig.jsonMaxObjectLen = 4096;

    SourceRunner sourceRunner = new SourceRunner.Builder(ClusterKafkaSource.class, createSource(conf))
      .addOutputLane("lane")
      .setExecutionMode(ExecutionMode.CLUSTER_YARN_STREAMING)
      .build();
    Thread th = null;
    try {
      sourceRunner.runInit();
      String jsonData = KafkaTestUtil.generateTestData(DataType.JSON, StreamingJsonParser.Mode.MULTIPLE_OBJECTS);
      th =
        createThreadForAddingBatch(sourceRunner,
          new ArrayList<Map.Entry>(Arrays.asList(new Pair("1".getBytes(), jsonData.getBytes()))));
      StageRunner.Output output = sourceRunner.runProduce(null, 10);

      String newOffset = output.getNewOffset();
      Assert.assertEquals("kafka::TestKafkaSource1::0", newOffset);
      List<Record> records = output.getRecords().get("lane");
      // 4 records in jsonData
      Assert.assertEquals(4, records.size());
    } finally {
      if (sourceRunner != null) {
        sourceRunner.runDestroy();
      }
      if (th != null) {
        th.interrupt();
      }
    }
  }

  @Test
  public void testProduceJsonRecordsArrayObjectsMultipleRecord() throws StageException, IOException {
    KafkaConfigBean conf = new KafkaConfigBean();
    conf.metadataBrokerList = metadataBrokerList;
    conf.topic = TOPIC1;
    conf.consumerGroup = "dummyGroup";
    conf.zookeeperConnect = zkConnect;
    conf.maxBatchSize = 1000;
    conf.maxWaitTime = 10000;
    conf.kafkaConsumerConfigs = null;
    conf.produceSingleRecordPerMessage = false;
    conf.dataFormat = DataFormat.JSON;
    conf.dataFormatConfig.charset = "UTF-8";
    conf.dataFormatConfig.removeCtrlChars = false;
    conf.dataFormatConfig.jsonContent = JsonMode.ARRAY_OBJECTS;
    conf.dataFormatConfig.jsonMaxObjectLen = 4096;

    SourceRunner sourceRunner = new SourceRunner.Builder(ClusterKafkaSource.class, createSource(conf))
      .addOutputLane("lane")
      .setExecutionMode(ExecutionMode.CLUSTER_YARN_STREAMING)
      .build();
    Thread th = null;
    boolean error = true;
    try {
      sourceRunner.runInit();
      String jsonData = KafkaTestUtil.generateTestData(DataType.JSON, StreamingJsonParser.Mode.ARRAY_OBJECTS);
      th =
        createThreadForAddingBatch(sourceRunner,
          new ArrayList<Map.Entry>(Arrays.asList(new Pair("1".getBytes(), jsonData.getBytes()))));
      StageRunner.Output output = sourceRunner.runProduce(null, 10);

      String newOffset = output.getNewOffset();
      Assert.assertEquals("kafka::TestKafkaSource1::0", newOffset);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertEquals(4, records.size());
      error = false;
    } finally {
      if (sourceRunner != null) {
        try {
          sourceRunner.runDestroy();
        } catch(Exception ex) {
          LOG.error("Error during destroy: " + ex, ex);
          if (!error) {
            throw ex;
          }
        }
      }
      if (th != null) {
        th.interrupt();
      }
    }
  }

  @Test
  public void testProduceJsonRecordsArrayObjectsSingleRecord() throws StageException, IOException {
    KafkaConfigBean conf = new KafkaConfigBean();
    conf.metadataBrokerList = metadataBrokerList;
    conf.topic = TOPIC1;
    conf.consumerGroup = "dummyGroup";
    conf.zookeeperConnect = zkConnect;
    conf.maxBatchSize = 1000;
    conf.maxWaitTime = 10000;
    conf.kafkaConsumerConfigs = null;
    conf.produceSingleRecordPerMessage = true;
    conf.dataFormat = DataFormat.JSON;
    conf.dataFormatConfig.charset = "UTF-8";
    conf.dataFormatConfig.removeCtrlChars = false;
    conf.dataFormatConfig.jsonContent = JsonMode.ARRAY_OBJECTS;
    conf.dataFormatConfig.jsonMaxObjectLen = 4096;

    SourceRunner sourceRunner = new SourceRunner.Builder(ClusterKafkaSource.class, createSource(conf))
      .addOutputLane("lane")
      .setExecutionMode(ExecutionMode.CLUSTER_YARN_STREAMING)
      .build();
    Thread th = null;
    boolean error = true;
    try {
      sourceRunner.runInit();
      String jsonData = KafkaTestUtil.generateTestData(DataType.JSON, StreamingJsonParser.Mode.ARRAY_OBJECTS);
      th = createThreadForAddingBatch(
        sourceRunner,
        new ArrayList<Map.Entry>(Arrays.asList(new Pair("1"
          .getBytes(), jsonData.getBytes()))));
      StageRunner.Output output = sourceRunner.runProduce(null, 10);

      String newOffset = output.getNewOffset();
      Assert.assertEquals("kafka::TestKafkaSource1::0", newOffset);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertEquals(1, records.size());
      error = false;
    } finally {
      try {
        sourceRunner.runDestroy();
      } catch(Exception ex) {
        LOG.error("Error during destroy: " + ex, ex);
        if (!error) {
          throw ex;
        }
      }
      if (th != null) {
        th.interrupt();
      }
    }
  }

  @Test
  public void testProduceXmlRecordsNoRecordElement() throws Exception {
    KafkaConfigBean conf = new KafkaConfigBean();
    conf.metadataBrokerList = metadataBrokerList;
    conf.topic = TOPIC1;
    conf.consumerGroup = "dummyGroup";
    conf.zookeeperConnect = zkConnect;
    conf.maxBatchSize = 1000;
    conf.maxWaitTime = 10000;
    conf.kafkaConsumerConfigs = null;
    conf.produceSingleRecordPerMessage = false;
    conf.dataFormat = DataFormat.XML;
    conf.dataFormatConfig.charset = "UTF-8";
    conf.dataFormatConfig.removeCtrlChars = false;
    conf.dataFormatConfig.xmlRecordElement = "";
    conf.dataFormatConfig.xmlMaxObjectLen = 4096;

    SourceRunner sourceRunner = new SourceRunner.Builder(ClusterKafkaSource.class, createSource(conf))
      .addOutputLane("lane")
      .setExecutionMode(ExecutionMode.CLUSTER_YARN_STREAMING)
      .build();
    Thread th = null;
    boolean error = true;
    try {
      sourceRunner.runInit();
      String xmlData = KafkaTestUtil.generateTestData(DataType.XML, null);
      th =
        createThreadForAddingBatch(sourceRunner,
          new ArrayList<Map.Entry>(Arrays.asList(new Pair("1".getBytes(), xmlData.getBytes()))));
      StageRunner.Output output = sourceRunner.runProduce(null, 10);

      String newOffset = output.getNewOffset();
      Assert.assertEquals("kafka::TestKafkaSource1::0", newOffset);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertEquals(1, records.size());
      error = false;
    } finally {
      try {
        sourceRunner.runDestroy();
      } catch(Exception ex) {
        LOG.error("Error during destroy: " + ex, ex);
        if (!error) {
          throw ex;
        }
      }
      if (th != null) {
        th.interrupt();
      }
    }
  }


  @Test
  public void testProduceXmlRecordsWithRecordElement() throws Exception {
    KafkaConfigBean conf = new KafkaConfigBean();
    conf.metadataBrokerList = metadataBrokerList;
    conf.topic = TOPIC1;
    conf.consumerGroup = "dummyGroup";
    conf.zookeeperConnect = zkConnect;
    conf.maxBatchSize = 1000;
    conf.maxWaitTime = 10000;
    conf.kafkaConsumerConfigs = null;
    conf.produceSingleRecordPerMessage = false;
    conf.dataFormat = DataFormat.XML;
    conf.dataFormatConfig.charset = "UTF-8";
    conf.dataFormatConfig.removeCtrlChars = false;
    conf.dataFormatConfig.xmlRecordElement = "author";
    conf.dataFormatConfig.xmlMaxObjectLen = 4096;

    SourceRunner sourceRunner = new SourceRunner.Builder(ClusterKafkaSource.class, createSource(conf))
      .addOutputLane("lane")
      .setExecutionMode(ExecutionMode.CLUSTER_YARN_STREAMING)
      .build();
    Thread th = null;
    boolean error = true;
    try {
      sourceRunner.runInit();
      String xmlData = KafkaTestUtil.generateTestData(DataType.XML, null);
      th =
        createThreadForAddingBatch(sourceRunner,
          new ArrayList<Map.Entry>(Arrays.asList(new Pair("1".getBytes(), xmlData.getBytes()))));
      StageRunner.Output output = sourceRunner.runProduce(null, 10);

      String newOffset = output.getNewOffset();
      Assert.assertEquals("kafka::TestKafkaSource1::0", newOffset);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertEquals(2, records.size());
      error = false;
    } finally {
      try {
        sourceRunner.runDestroy();
      } catch(Exception ex) {
        LOG.error("Error during destroy: " + ex, ex);
        if (!error) {
          throw ex;
        }
      }
      if (th != null) {
        th.interrupt();
      }
    }
  }

  @Test
  public void testProduceCsvRecords() throws Exception {
    KafkaConfigBean conf = new KafkaConfigBean();
    conf.metadataBrokerList = metadataBrokerList;
    conf.topic = TOPIC1;
    conf.consumerGroup = "dummyGroup";
    conf.zookeeperConnect = zkConnect;
    conf.maxBatchSize = 1000;
    conf.maxWaitTime = 10000;
    conf.kafkaConsumerConfigs = null;
    conf.produceSingleRecordPerMessage = true;
    conf.dataFormat = DataFormat.DELIMITED;
    conf.dataFormatConfig.charset = "UTF-8";
    conf.dataFormatConfig.removeCtrlChars = false;
    conf.dataFormatConfig.csvFileFormat = CsvMode.CSV;
    conf.dataFormatConfig.csvHeader = CsvHeader.NO_HEADER;
    conf.dataFormatConfig.csvMaxObjectLen = 4096;
    conf.dataFormatConfig.csvRecordType = CsvRecordType.LIST;
    conf.dataFormatConfig.csvSkipStartLines = 0;

    SourceRunner sourceRunner = new SourceRunner.Builder(ClusterKafkaSource.class, createSource(conf))
      .addOutputLane("lane")
      .setExecutionMode(ExecutionMode.CLUSTER_YARN_STREAMING)
      .build();
    Thread th = null;
    boolean error = true;
    try {
      sourceRunner.runInit();
      String csvData = KafkaTestUtil.generateTestData(DataType.CSV, null);
      th =
        createThreadForAddingBatch(sourceRunner,
          new ArrayList<Map.Entry>(Arrays.asList(new Pair("1".getBytes(), csvData.getBytes()))));
      StageRunner.Output output = sourceRunner.runProduce(null, 10);

      String newOffset = output.getNewOffset();
      Assert.assertEquals("kafka::TestKafkaSource1::0", newOffset);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertEquals(1, records.size());
      error = false;
    } finally {
      try {
        sourceRunner.runDestroy();
      } catch(Exception ex) {
        LOG.error("Error during destroy: " + ex, ex);
        if (!error) {
          throw ex;
        }
      }
      if (th != null) {
        th.interrupt();
      }
    }
  }

  private Thread createThreadForAddingBatch(final SourceRunner sourceRunner, final List<Map.Entry> list) {
    Thread sourceThread = new Thread() {
      @Override
      public void run() {
        try {
          ClusterKafkaSource source = (ClusterKafkaSource) sourceRunner.getStage();
          source.put(list);
        } catch (Exception ex) {
          LOG.error("Error in waiter thread: " + ex, ex);
        }
      }
    };
    sourceThread.setName(getClass().getName() + "-sourceThread");
    sourceThread.setDaemon(true);
    sourceThread.start();
    return sourceThread;
  }

}
