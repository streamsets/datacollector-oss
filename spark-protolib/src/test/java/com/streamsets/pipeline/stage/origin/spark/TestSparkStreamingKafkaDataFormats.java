/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.spark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.configurablestage.DSource;
import com.streamsets.pipeline.lib.DataType;
import com.streamsets.pipeline.lib.KafkaTestUtil;
import com.streamsets.pipeline.lib.json.StreamingJsonParser;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.origin.kafka.KafkaDSource;
import com.streamsets.pipeline.stage.origin.spark.MessageAndPartition;
import com.streamsets.pipeline.stage.origin.spark.SparkStreamingKafkaSource;


public class TestSparkStreamingKafkaDataFormats {
  private static final Logger LOG = LoggerFactory.getLogger(TestSparkStreamingKafkaDataFormats.class);

  @BeforeClass
  public static void setup() {
    System.setProperty("sdc.clustermode", "true");
  }

  @Test
  public void testProduceStringRecords() throws Exception {
    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class)
      .addOutputLane("lane")
      .addConfiguration("metadataBrokerList", "dummyhost:1281")
      .addConfiguration("zookeeperConnect", null)
      .addConfiguration("consumerGroup", null)
      .addConfiguration("topic", "testProduceStringRecords")
      .addConfiguration("maxWaitTime", 10000)
      .addConfiguration("dataFormat", DataFormat.TEXT)
      .addConfiguration("charset", "UTF-8")
      .addConfiguration("textMaxLineLen", 4096)
      .addConfiguration("maxBatchSize",1000)
      .addConfiguration("kafkaConsumerConfigs", null)
      .addConfiguration("produceSingleRecordPerMessage", false)
      .addConfiguration("regex", null)
      .addConfiguration("grokPatternDefinition", null)
      .addConfiguration("enableLog4jCustomLogFormat", false)
      .addConfiguration("customLogFormat", null)
      .addConfiguration("fieldPathsToGroupName", null)
      .addConfiguration("log4jCustomLogFormat", null)
      .addConfiguration("grokPattern", null)
      .addConfiguration("onParseError", null)
      .addConfiguration("maxStackTraceLines", -1)
      .build();
    Thread th = null;
    try {
      sourceRunner.runInit();
      List<MessageAndPartition> list = new ArrayList<MessageAndPartition>();
      list.add(new MessageAndPartition("aaa".getBytes(), "1".getBytes()));
      list.add(new MessageAndPartition("bbb".getBytes(), "2".getBytes()));
      list.add(new MessageAndPartition("ccc".getBytes(), "1".getBytes()));
      th = createThreadForAddingBatch(sourceRunner, list);

      StageRunner.Output output = sourceRunner.runProduce(null, 5);

      String newOffset = output.getNewOffset();
      Assert.assertNull(newOffset);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertEquals(3, records.size());

      for (int i = 0; i < records.size(); i++) {
        Assert.assertNotNull(records.get(i).get("/text"));
        LOG.info("Header " + records.get(i).getHeader().getSourceId());
        Assert.assertTrue(!records.get(i).get("/text").getValueAsString().isEmpty());
        Assert.assertEquals(new String(list.get(i).getPayload()), records.get(i).get("/text").getValueAsString());
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
    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class)
      .addOutputLane("lane")
      .addConfiguration("metadataBrokerList", "dummyhost:1281")
      .addConfiguration("zookeeperConnect", null)
      .addConfiguration("consumerGroup", null)
      .addConfiguration("maxBatchSize",1000)
      .addConfiguration("kafkaConsumerConfigs", null)
      .addConfiguration("topic", "testProduceStringRecords")
      .addConfiguration("maxWaitTime", 10000)
      .addConfiguration("dataFormat", DataFormat.JSON)
      .addConfiguration("jsonContent", JsonMode.MULTIPLE_OBJECTS)
      .addConfiguration("charset", "UTF-8")
      .addConfiguration("textMaxLineLen", 4096)
      .addConfiguration("jsonMaxObjectLen", 4096)
      .addConfiguration("produceSingleRecordPerMessage", false)
      .addConfiguration("regex", null)
      .addConfiguration("grokPatternDefinition", null)
      .addConfiguration("enableLog4jCustomLogFormat", false)
      .addConfiguration("customLogFormat", null)
      .addConfiguration("fieldPathsToGroupName", null)
      .addConfiguration("log4jCustomLogFormat", null)
      .addConfiguration("grokPattern", null)
      .addConfiguration("onParseError", null)
      .addConfiguration("maxStackTraceLines", -1)
      .build();
    Thread th = null;
    try {
      sourceRunner.runInit();
      String jsonData = KafkaTestUtil.generateTestData(DataType.JSON, StreamingJsonParser.Mode.MULTIPLE_OBJECTS);
      th =
        createThreadForAddingBatch(sourceRunner,
          new ArrayList<MessageAndPartition>(Arrays.asList(new MessageAndPartition(jsonData.getBytes(), "1".getBytes()))));
      StageRunner.Output output = sourceRunner.runProduce(null, 10);

      String newOffset = output.getNewOffset();
      Assert.assertNull(newOffset);
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
    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class)
      .addOutputLane("lane")
      .addConfiguration("metadataBrokerList", "dummyhost:1281")
      .addConfiguration("zookeeperConnect", null)
      .addConfiguration("consumerGroup", null)
      .addConfiguration("maxBatchSize",1000)
      .addConfiguration("kafkaConsumerConfigs", null)
      .addConfiguration("topic", "testProduceStringRecords")
      .addConfiguration("maxWaitTime", 10000)
      .addConfiguration("dataFormat", DataFormat.JSON)
      .addConfiguration("jsonContent", JsonMode.ARRAY_OBJECTS)
      .addConfiguration("charset", "UTF-8")
      .addConfiguration("textMaxLineLen", 4096)
      .addConfiguration("jsonMaxObjectLen", 4096)
      .addConfiguration("produceSingleRecordPerMessage", false)
      .addConfiguration("regex", null)
      .addConfiguration("grokPatternDefinition", null)
      .addConfiguration("enableLog4jCustomLogFormat", false)
      .addConfiguration("customLogFormat", null)
      .addConfiguration("fieldPathsToGroupName", null)
      .addConfiguration("log4jCustomLogFormat", null)
      .addConfiguration("grokPattern", null)
      .addConfiguration("onParseError", null)
      .addConfiguration("maxStackTraceLines", -1)
      .build();
    Thread th = null;
    try {
      sourceRunner.runInit();
      String jsonData = KafkaTestUtil.generateTestData(DataType.JSON, StreamingJsonParser.Mode.ARRAY_OBJECTS);
      th =
        createThreadForAddingBatch(sourceRunner,
          new ArrayList<MessageAndPartition>(Arrays.asList(new MessageAndPartition(jsonData.getBytes(), "1".getBytes()))));
      StageRunner.Output output = sourceRunner.runProduce(null, 10);

      String newOffset = output.getNewOffset();
      Assert.assertNull(newOffset);
      List<Record> records = output.getRecords().get("lane");
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
  public void testProduceJsonRecordsArrayObjectsSingleRecord() throws StageException, IOException {
    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class)
      .addOutputLane("lane")
      .addConfiguration("metadataBrokerList", "dummyhost:1281")
       .addConfiguration("zookeeperConnect", null)
      .addConfiguration("consumerGroup", null)
      .addConfiguration("maxBatchSize",1000)
      .addConfiguration("kafkaConsumerConfigs", null)
      .addConfiguration("topic", "testProduceStringRecords")
      .addConfiguration("maxWaitTime", 10000)
      .addConfiguration("dataFormat", DataFormat.JSON)
      .addConfiguration("jsonContent", JsonMode.ARRAY_OBJECTS)
      .addConfiguration("charset", "UTF-8")
      .addConfiguration("textMaxLineLen", 4096)
      .addConfiguration("jsonMaxObjectLen", 4096)
      .addConfiguration("produceSingleRecordPerMessage", true)
      .addConfiguration("regex", null)
      .addConfiguration("grokPatternDefinition", null)
      .addConfiguration("enableLog4jCustomLogFormat", false)
      .addConfiguration("customLogFormat", null)
      .addConfiguration("fieldPathsToGroupName", null)
      .addConfiguration("log4jCustomLogFormat", null)
      .addConfiguration("grokPattern", null)
      .addConfiguration("onParseError", null)
      .addConfiguration("maxStackTraceLines", -1)
      .build();
    Thread th = null;
    try {
      sourceRunner.runInit();
      String jsonData = KafkaTestUtil.generateTestData(DataType.JSON, StreamingJsonParser.Mode.ARRAY_OBJECTS);
      th =
        createThreadForAddingBatch(sourceRunner,
          new ArrayList<MessageAndPartition>(Arrays.asList(new MessageAndPartition(jsonData.getBytes(), "1".getBytes()))));
      StageRunner.Output output = sourceRunner.runProduce(null, 10);

      String newOffset = output.getNewOffset();
      Assert.assertNull(newOffset);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertEquals(1, records.size());
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
  public void testProduceXmlRecordsNoRecordElement() throws Exception {
    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class)
      .addOutputLane("lane")
      .addConfiguration("metadataBrokerList", "dummyhost:1281")
      .addConfiguration("zookeeperConnect", null)
      .addConfiguration("consumerGroup", null)
      .addConfiguration("maxBatchSize",1000)
      .addConfiguration("kafkaConsumerConfigs", null)
      .addConfiguration("topic", "testProduceStringRecords")
      .addConfiguration("maxWaitTime", 10000)
      .addConfiguration("dataFormat", DataFormat.XML)
      .addConfiguration("charset", "UTF-8")
      .addConfiguration("textMaxLineLen", 4096)
      .addConfiguration("jsonMaxObjectLen", 4096)
      .addConfiguration("produceSingleRecordPerMessage", false)
      .addConfiguration("xmlRecordElement", "")
      .addConfiguration("xmlMaxObjectLen", 4096)
      .addConfiguration("regex", null)
      .addConfiguration("grokPatternDefinition", null)
      .addConfiguration("enableLog4jCustomLogFormat", false)
      .addConfiguration("customLogFormat", null)
      .addConfiguration("fieldPathsToGroupName", null)
      .addConfiguration("log4jCustomLogFormat", null)
      .addConfiguration("grokPattern", null)
      .addConfiguration("onParseError", null)
      .addConfiguration("maxStackTraceLines", -1)
      .build();
    Thread th = null;
    try {
      sourceRunner.runInit();
      String xmlData = KafkaTestUtil.generateTestData(DataType.XML, null);
      th =
        createThreadForAddingBatch(sourceRunner,
          new ArrayList<MessageAndPartition>(Arrays.asList(new MessageAndPartition(xmlData.getBytes(), "1".getBytes()))));
      StageRunner.Output output = sourceRunner.runProduce(null, 10);

      String newOffset = output.getNewOffset();
      Assert.assertNull(newOffset);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertEquals(1, records.size());
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
  public void testProduceXmlRecordsWithRecordElement() throws Exception {
    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class)
      .addOutputLane("lane")
      .addConfiguration("metadataBrokerList", "dummyhost:1281")
      .addConfiguration("zookeeperConnect", null)
      .addConfiguration("consumerGroup", null)
      .addConfiguration("maxBatchSize",1000)
      .addConfiguration("kafkaConsumerConfigs", null)
      .addConfiguration("topic", "testProduceStringRecords")
      .addConfiguration("maxWaitTime", 10000)
      .addConfiguration("dataFormat", DataFormat.XML)
      .addConfiguration("charset", "UTF-8")
      .addConfiguration("textMaxLineLen", 4096)
      .addConfiguration("jsonMaxObjectLen", 4096)
      .addConfiguration("produceSingleRecordPerMessage", false)
      .addConfiguration("xmlRecordElement", "author")
      .addConfiguration("xmlMaxObjectLen", 4096)
      .addConfiguration("regex", null)
      .addConfiguration("grokPatternDefinition", null)
      .addConfiguration("enableLog4jCustomLogFormat", false)
      .addConfiguration("customLogFormat", null)
      .addConfiguration("fieldPathsToGroupName", null)
      .addConfiguration("log4jCustomLogFormat", null)
      .addConfiguration("grokPattern", null)
      .addConfiguration("onParseError", null)
      .addConfiguration("maxStackTraceLines", -1)
      .build();
    Thread th = null;
    try {
      sourceRunner.runInit();
      String xmlData = KafkaTestUtil.generateTestData(DataType.XML, null);
      th =
        createThreadForAddingBatch(sourceRunner,
          new ArrayList<MessageAndPartition>(Arrays.asList(new MessageAndPartition(xmlData.getBytes(), "1".getBytes()))));
      StageRunner.Output output = sourceRunner.runProduce(null, 10);

      String newOffset = output.getNewOffset();
      Assert.assertNull(newOffset);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertEquals(2, records.size());
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
  public void testProduceCsvRecords() throws Exception {
    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class)
      .addOutputLane("lane")
      .addConfiguration("metadataBrokerList", "dummyhost:1281")
      .addConfiguration("zookeeperConnect", null)
      .addConfiguration("consumerGroup", null)
      .addConfiguration("maxBatchSize",1000)
      .addConfiguration("kafkaConsumerConfigs", null)
      .addConfiguration("topic", "testProduceStringRecords")
      .addConfiguration("consumerGroup", null)
      .addConfiguration("zookeeperConnect", "dummy")
      .addConfiguration("maxWaitTime", 10000)
      .addConfiguration("dataFormat", DataFormat.DELIMITED)
      .addConfiguration("charset", "UTF-8")
      .addConfiguration("textMaxLineLen", 4096)
      .addConfiguration("jsonMaxObjectLen", 4096)
      .addConfiguration("produceSingleRecordPerMessage", true)
      .addConfiguration("csvFileFormat", CsvMode.CSV)
      .addConfiguration("csvHeader", CsvHeader.NO_HEADER)
      .addConfiguration("csvMaxObjectLen", 4096)
      .addConfiguration("regex", null)
      .addConfiguration("grokPatternDefinition", null)
      .addConfiguration("enableLog4jCustomLogFormat", false)
      .addConfiguration("customLogFormat", null)
      .addConfiguration("fieldPathsToGroupName", null)
      .addConfiguration("log4jCustomLogFormat", null)
      .addConfiguration("grokPattern", null)
      .addConfiguration("onParseError", null)
      .addConfiguration("maxStackTraceLines", -1)
      .build();
    Thread th = null;
    try {
      sourceRunner.runInit();
      String csvData = KafkaTestUtil.generateTestData(DataType.CSV, null);
      th =
        createThreadForAddingBatch(sourceRunner,
          new ArrayList<MessageAndPartition>(Arrays.asList(new MessageAndPartition(csvData.getBytes(), "1".getBytes()))));
      StageRunner.Output output = sourceRunner.runProduce(null, 10);

      String newOffset = output.getNewOffset();
      Assert.assertNull(newOffset);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertEquals(1, records.size());
    } finally {
      if (sourceRunner != null) {
        sourceRunner.runDestroy();
      }
      if (th != null) {
        th.interrupt();
      }
    }
  }

  private Thread createThreadForAddingBatch(final SourceRunner sourceRunner, final List<MessageAndPartition> list) {
    Thread sourceThread = new Thread() {
      @Override
      public void run() {
        try {
          SparkStreamingKafkaSource source =
            ((SparkStreamingKafkaSource) ((DSource) sourceRunner.getStage()).getSource());
          source.put(list);
        } catch (IllegalStateException ex) {
          // ignored
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
