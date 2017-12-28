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
package com.streamsets.pipeline.stage.origin.kafka.cluster;

import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ext.json.Mode;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.CsvRecordType;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.impl.Pair;
import com.streamsets.pipeline.kafka.common.DataType;
import com.streamsets.pipeline.kafka.common.KafkaTestUtil;
import com.streamsets.pipeline.kafka.common.SdcKafkaTestUtil;
import com.streamsets.pipeline.kafka.common.SdcKafkaTestUtilFactory;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;
import com.streamsets.pipeline.stage.origin.kafka.BaseKafkaSource;
import com.streamsets.pipeline.stage.origin.kafka.ClusterKafkaSourceFactory;
import com.streamsets.pipeline.stage.origin.kafka.KafkaConfigBean;
import com.streamsets.pipeline.stage.origin.kafka.KafkaDSource;
import com.streamsets.pipeline.stage.origin.kafka.KafkaSourceFactory;
import kafka.utils.TestUtils;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;


public class TestClusterModeDataFormats {
  private static final Logger LOG = LoggerFactory.getLogger(TestClusterModeDataFormats.class);

  private static String zkConnect;
  private static final int SINGLE_PARTITION = 1;
  private static final int SINGLE_REPLICATION_FACTOR = 1;
  private static final String TOPIC1 = "TestKafkaSource1";
  private static final int TIME_OUT = 5000;

  private static String metadataBrokerList;
  private static String originalTmpDir;
  private static final SdcKafkaTestUtil sdcKafkaTestUtil = SdcKafkaTestUtilFactory.getInstance().create();

  @BeforeClass
  public static void setUp() throws Exception {
    //Init zookeeper
    originalTmpDir = System.getProperty("java.io.tmpdir");
    File testDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(testDir.mkdirs());
    System.setProperty("java.io.tmpdir", testDir.getAbsolutePath());
    sdcKafkaTestUtil.startZookeeper();
    zkConnect = sdcKafkaTestUtil.getZkConnect();
    sdcKafkaTestUtil.startKafkaBrokers(1);
    sdcKafkaTestUtil.createTopic(TOPIC1, SINGLE_PARTITION, SINGLE_REPLICATION_FACTOR);
    TestUtils.waitUntilMetadataIsPropagated(
        scala.collection.JavaConversions.asScalaBuffer(sdcKafkaTestUtil.getKafkaServers()),
        TOPIC1,
        0,
        TIME_OUT
    );
    metadataBrokerList = sdcKafkaTestUtil.getMetadataBrokerURI();
  }

  @AfterClass
  public static void tearDown() {
    System.setProperty("java.io.tmpdir", originalTmpDir);
    sdcKafkaTestUtil.shutdown();
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
    conf.produceSingleRecordPerMessage = false;
    conf.dataFormat = DataFormat.TEXT;
    conf.dataFormatConfig.charset = "UTF-8";
    conf.dataFormatConfig.removeCtrlChars = false;
    conf.dataFormatConfig.textMaxLineLen = 4096;

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class, createSource(conf))
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

        Assert.assertEquals("For record: " + i, TOPIC1, records.get(i).getHeader().getAttribute(HeaderAttributeConstants.TOPIC));
        Assert.assertEquals("For record: " + i, "" + (i % 2 + 1), records.get(i).getHeader().getAttribute(HeaderAttributeConstants.PARTITION));
        Assert.assertEquals("For record: " + i, "" + i, records.get(i).getHeader().getAttribute(HeaderAttributeConstants.OFFSET));
      }

      sourceRunner.runDestroy();
    }
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
    conf.produceSingleRecordPerMessage = false;
    conf.dataFormat = DataFormat.JSON;
    conf.dataFormatConfig.charset = "UTF-8";
    conf.dataFormatConfig.removeCtrlChars = false;
    conf.dataFormatConfig.jsonContent = JsonMode.MULTIPLE_OBJECTS;
    conf.dataFormatConfig.jsonMaxObjectLen = 4096;

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class, createSource(conf))
      .addOutputLane("lane")
      .setExecutionMode(ExecutionMode.CLUSTER_YARN_STREAMING)
      .build();
    Thread th = null;
    try {
      sourceRunner.runInit();
      String jsonData = KafkaTestUtil.generateTestData(DataType.JSON, Mode.MULTIPLE_OBJECTS);
      th =
        createThreadForAddingBatch(sourceRunner, new ArrayList<>(Collections.singletonList(new Pair("1".getBytes(),
            jsonData.getBytes()
        ))));
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
    conf.produceSingleRecordPerMessage = false;
    conf.dataFormat = DataFormat.JSON;
    conf.dataFormatConfig.charset = "UTF-8";
    conf.dataFormatConfig.removeCtrlChars = false;
    conf.dataFormatConfig.jsonContent = JsonMode.ARRAY_OBJECTS;
    conf.dataFormatConfig.jsonMaxObjectLen = 4096;

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class, createSource(conf))
      .addOutputLane("lane")
      .setExecutionMode(ExecutionMode.CLUSTER_YARN_STREAMING)
      .build();
    Thread th = null;
    boolean error = true;
    try {
      sourceRunner.runInit();
      String jsonData = KafkaTestUtil.generateTestData(DataType.JSON, Mode.ARRAY_OBJECTS);
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
    conf.produceSingleRecordPerMessage = true;
    conf.dataFormat = DataFormat.JSON;
    conf.dataFormatConfig.charset = "UTF-8";
    conf.dataFormatConfig.removeCtrlChars = false;
    conf.dataFormatConfig.jsonContent = JsonMode.ARRAY_OBJECTS;
    conf.dataFormatConfig.jsonMaxObjectLen = 4096;

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class, createSource(conf))
      .addOutputLane("lane")
      .setExecutionMode(ExecutionMode.CLUSTER_YARN_STREAMING)
      .build();
    Thread th = null;
    boolean error = true;
    try {
      sourceRunner.runInit();
      String jsonData = KafkaTestUtil.generateTestData(DataType.JSON, Mode.ARRAY_OBJECTS);
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
    conf.produceSingleRecordPerMessage = false;
    conf.dataFormat = DataFormat.XML;
    conf.dataFormatConfig.charset = "UTF-8";
    conf.dataFormatConfig.removeCtrlChars = false;
    conf.dataFormatConfig.xmlRecordElement = "";
    conf.dataFormatConfig.xmlMaxObjectLen = 4096;

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class, createSource(conf))
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
    conf.produceSingleRecordPerMessage = false;
    conf.dataFormat = DataFormat.XML;
    conf.dataFormatConfig.charset = "UTF-8";
    conf.dataFormatConfig.removeCtrlChars = false;
    conf.dataFormatConfig.xmlRecordElement = "author";
    conf.dataFormatConfig.xmlMaxObjectLen = 4096;

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class, createSource(conf))
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
    conf.produceSingleRecordPerMessage = true;
    conf.dataFormat = DataFormat.DELIMITED;
    conf.dataFormatConfig.charset = "UTF-8";
    conf.dataFormatConfig.removeCtrlChars = false;
    conf.dataFormatConfig.csvFileFormat = CsvMode.CSV;
    conf.dataFormatConfig.csvHeader = CsvHeader.NO_HEADER;
    conf.dataFormatConfig.csvMaxObjectLen = 4096;
    conf.dataFormatConfig.csvRecordType = CsvRecordType.LIST;
    conf.dataFormatConfig.csvSkipStartLines = 0;

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class, createSource(conf))
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
