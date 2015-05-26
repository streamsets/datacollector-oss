/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.kafka;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.config.LogMode;
import com.streamsets.pipeline.config.OnParseError;
import com.streamsets.pipeline.lib.DataType;
import com.streamsets.pipeline.lib.KafkaTestUtil;
import com.streamsets.pipeline.lib.ProducerRunnable;
import com.streamsets.pipeline.lib.json.StreamingJsonParser;
import com.streamsets.pipeline.lib.parser.log.Constants;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import kafka.javaapi.producer.Producer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TestKafkaSource {

  private static final int SINGLE_PARTITION = 1;
  private static final int MULTIPLE_PARTITIONS = 5;
  private static final int SINGLE_REPLICATION_FACTOR = 1;
  private static final int MULTIPLE_REPLICATION_FACTOR = 1;
  private static final String TOPIC1 = "TestKafkaSource1";
  private static final String TOPIC2 = "TestKafkaSource2";
  private static final String TOPIC3 = "TestKafkaSource3";
  private static final String TOPIC4 = "TestKafkaSource4";
  private static final String TOPIC5 = "TestKafkaSource5";
  private static final String TOPIC6 = "TestKafkaSource6";
  private static final String TOPIC7 = "TestKafkaSource7";
  private static final String TOPIC8 = "TestKafkaSource8";
  private static final String TOPIC9 = "TestKafkaSource9";
  private static final String TOPIC10 = "TestKafkaSource10";
  private static final String TOPIC11 = "TestKafkaSource11";
  private static final String CONSUMER_GROUP = "SDC";

  private static Producer<String, String> producer;
  private static String zkConnect;

  @BeforeClass
  public static void setUp() {
    KafkaTestUtil.startZookeeper();
    KafkaTestUtil.startKafkaBrokers(3);

    zkConnect = KafkaTestUtil.getZkConnect();

    KafkaTestUtil.createTopic(TOPIC1, SINGLE_PARTITION, SINGLE_REPLICATION_FACTOR);
    KafkaTestUtil.createTopic(TOPIC2, MULTIPLE_PARTITIONS, MULTIPLE_REPLICATION_FACTOR);
    KafkaTestUtil.createTopic(TOPIC3, SINGLE_PARTITION, SINGLE_REPLICATION_FACTOR);
    KafkaTestUtil.createTopic(TOPIC4, SINGLE_PARTITION, SINGLE_REPLICATION_FACTOR);
    KafkaTestUtil.createTopic(TOPIC5, SINGLE_PARTITION, SINGLE_REPLICATION_FACTOR);
    KafkaTestUtil.createTopic(TOPIC6, SINGLE_PARTITION, SINGLE_REPLICATION_FACTOR);
    KafkaTestUtil.createTopic(TOPIC7, SINGLE_PARTITION, SINGLE_REPLICATION_FACTOR);
    KafkaTestUtil.createTopic(TOPIC8, SINGLE_PARTITION, SINGLE_REPLICATION_FACTOR);
    KafkaTestUtil.createTopic(TOPIC9, SINGLE_PARTITION, SINGLE_REPLICATION_FACTOR);
    KafkaTestUtil.createTopic(TOPIC10, SINGLE_PARTITION, SINGLE_REPLICATION_FACTOR);
    KafkaTestUtil.createTopic(TOPIC11, SINGLE_PARTITION, SINGLE_REPLICATION_FACTOR);

    producer = KafkaTestUtil.createProducer(KafkaTestUtil.getMetadataBrokerURI(), true);

    // remove this
    System.setProperty("sdc.clustermode", "false");
  }

  @AfterClass
  public static void tearDown() {
    KafkaTestUtil.shutdown();
  }

  @Test
  public void testProduceStringRecords() throws StageException, InterruptedException {

    CountDownLatch startLatch = new CountDownLatch(1);

    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new ProducerRunnable( TOPIC1, SINGLE_PARTITION, producer, startLatch, DataType.TEXT, null, -1,
      null));

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class)
      .addOutputLane("lane")
      .addConfiguration("metadataBrokerList", KafkaTestUtil.getMetadataBrokerURI())
      .addConfiguration("topic", TOPIC1)
      .addConfiguration("consumerGroup", CONSUMER_GROUP)
      .addConfiguration("zookeeperConnect", zkConnect)
      .addConfiguration("maxBatchSize", 9)
      .addConfiguration("maxWaitTime", 5000)
      .addConfiguration("dataFormat", DataFormat.TEXT)
      .addConfiguration("charset", "UTF-8")
      .addConfiguration("textMaxLineLen", 4096)
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
    sourceRunner.runInit();

    startLatch.countDown();
    StageRunner.Output output = sourceRunner.runProduce(null, 5);
    shutDownExecutorService(executorService);

    String newOffset = output.getNewOffset();
    Assert.assertNull(newOffset);
    List<Record> records = output.getRecords().get("lane");
    Assert.assertEquals(5, records.size());

    for(int i = 0; i < records.size(); i++) {
      Assert.assertNotNull(records.get(i).get("/text"));
      Assert.assertTrue(!records.get(i).get("/text").getValueAsString().isEmpty());
      Assert.assertEquals(KafkaTestUtil.generateTestData(DataType.TEXT, null),
        records.get(i).get("/text").getValueAsString());
    }

    sourceRunner.runDestroy();
  }

  @Test
  public void testProduceStringRecordsMultiplePartitions() throws StageException, InterruptedException {

    CountDownLatch startProducing = new CountDownLatch(1);

    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new ProducerRunnable( TOPIC2, MULTIPLE_PARTITIONS, producer, startProducing, DataType.TEXT,
      null, -1, null));

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class)
      .addOutputLane("lane")
      .addConfiguration("metadataBrokerList", KafkaTestUtil.getMetadataBrokerURI())
      .addConfiguration("topic", TOPIC2)
      .addConfiguration("consumerGroup", CONSUMER_GROUP)
      .addConfiguration("zookeeperConnect", zkConnect)
      .addConfiguration("maxBatchSize", 9)
      .addConfiguration("maxWaitTime", 5000)
      .addConfiguration("dataFormat", DataFormat.TEXT)
      .addConfiguration("charset", "UTF-8")
      .addConfiguration("textMaxLineLen", 4096)
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

    sourceRunner.runInit();

    startProducing.countDown();
    StageRunner.Output output = sourceRunner.runProduce(null, 9);
    shutDownExecutorService(executorService);

    String newOffset = output.getNewOffset();
    Assert.assertNull(newOffset);
    List<Record> records = output.getRecords().get("lane");
    Assert.assertEquals(9, records.size());

    for(int i = 0; i < records.size(); i++) {
      Assert.assertNotNull(records.get(i).get("/text").getValueAsString());
      Assert.assertTrue(!records.get(i).get("/text").getValueAsString().isEmpty());
      Assert.assertEquals(KafkaTestUtil.generateTestData(DataType.TEXT, null), records.get(i).get("/text").getValueAsString());
    }

    sourceRunner.runDestroy();
  }

  @Test
  public void testProduceJsonRecordsMultipleObjectsSingleRecord() throws StageException, IOException, InterruptedException {

    CountDownLatch startLatch = new CountDownLatch(1);
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new ProducerRunnable( TOPIC3, SINGLE_PARTITION, producer, startLatch, DataType.JSON,
      StreamingJsonParser.Mode.MULTIPLE_OBJECTS, -1, null));

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class)
      .addOutputLane("lane")
      .addConfiguration("metadataBrokerList", KafkaTestUtil.getMetadataBrokerURI())
      .addConfiguration("topic", TOPIC3)
      .addConfiguration("consumerGroup", CONSUMER_GROUP)
      .addConfiguration("zookeeperConnect", zkConnect)
      .addConfiguration("maxBatchSize", 9)
      .addConfiguration("maxWaitTime", 5000)
      .addConfiguration("dataFormat", DataFormat.JSON)
      .addConfiguration("charset", "UTF-8")
      .addConfiguration("jsonContent", JsonMode.MULTIPLE_OBJECTS)
      .addConfiguration("jsonMaxObjectLen", 4096)
      .addConfiguration("produceSingleRecordPerMessage", true)
      .addConfiguration("kafkaConsumerConfigs", null)
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

    sourceRunner.runInit();

    startLatch.countDown();
    StageRunner.Output output = sourceRunner.runProduce(null, 9);
    shutDownExecutorService(executorService);

    String newOffset = output.getNewOffset();
    Assert.assertNull(newOffset);

    List<Record> records = output.getRecords().get("lane");
    Assert.assertEquals(9, records.size());

    sourceRunner.runDestroy();
  }

  @Test
  public void testProduceJsonRecordsMultipleObjectsMultipleRecord() throws StageException, IOException, InterruptedException {

    CountDownLatch startLatch = new CountDownLatch(1);
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new ProducerRunnable(TOPIC4, SINGLE_PARTITION, producer, startLatch, DataType.JSON,
      StreamingJsonParser.Mode.MULTIPLE_OBJECTS, -1, null));

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class)
      .addOutputLane("lane")
      .addConfiguration("topic", TOPIC4)
      .addConfiguration("metadataBrokerList", KafkaTestUtil.getMetadataBrokerURI())
      .addConfiguration("consumerGroup", CONSUMER_GROUP)
      .addConfiguration("zookeeperConnect", zkConnect)
      .addConfiguration("maxBatchSize", 9)
      .addConfiguration("maxWaitTime", 5000)
      .addConfiguration("dataFormat", DataFormat.JSON)
      .addConfiguration("charset", "UTF-8")
      .addConfiguration("jsonContent", JsonMode.MULTIPLE_OBJECTS)
      .addConfiguration("jsonMaxObjectLen", 4096)
      .addConfiguration("produceSingleRecordPerMessage", false)
      .addConfiguration("kafkaConsumerConfigs", null)
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

    sourceRunner.runInit();

    startLatch.countDown();
    StageRunner.Output output = sourceRunner.runProduce(null, 12);
    shutDownExecutorService(executorService);

    String newOffset = output.getNewOffset();
    Assert.assertNull(newOffset);

    List<Record> records = output.getRecords().get("lane");
    Assert.assertEquals(12, records.size());

    sourceRunner.runDestroy();
  }

  @Test
  public void testProduceJsonRecordsArrayObjects() throws StageException, IOException, InterruptedException {

    CountDownLatch startLatch = new CountDownLatch(1);
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new ProducerRunnable( TOPIC5, SINGLE_PARTITION, producer, startLatch, DataType.JSON,
      StreamingJsonParser.Mode.ARRAY_OBJECTS, -1, null));

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class)
      .addOutputLane("lane")
      .addConfiguration("topic", TOPIC5)
      .addConfiguration("metadataBrokerList", KafkaTestUtil.getMetadataBrokerURI())
      .addConfiguration("consumerGroup", CONSUMER_GROUP)
      .addConfiguration("zookeeperConnect", zkConnect)
      .addConfiguration("maxBatchSize", 9)
      .addConfiguration("maxWaitTime", 5000)
      .addConfiguration("dataFormat", DataFormat.JSON)
      .addConfiguration("charset", "UTF-8")
      .addConfiguration("jsonContent", JsonMode.ARRAY_OBJECTS)
      .addConfiguration("jsonMaxObjectLen", 4096)
      .addConfiguration("kafkaConsumerConfigs", null)
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

    sourceRunner.runInit();

    startLatch.countDown();
    StageRunner.Output output = sourceRunner.runProduce(null, 9);
    shutDownExecutorService(executorService);

    String newOffset = output.getNewOffset();
    Assert.assertNull(newOffset);

    List<Record> records = output.getRecords().get("lane");
    Assert.assertEquals(9, records.size());

    sourceRunner.runDestroy();
  }


  @Test
  public void testProduceXmlRecordsNoRecordElement() throws StageException, IOException, InterruptedException {

    CountDownLatch startLatch = new CountDownLatch(1);
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new ProducerRunnable(TOPIC6, SINGLE_PARTITION, producer, startLatch, DataType.XML, null, -1,
      null));

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class)
      .addOutputLane("lane")
      .addConfiguration("topic", TOPIC6)
      .addConfiguration("metadataBrokerList", KafkaTestUtil.getMetadataBrokerURI())
      .addConfiguration("consumerGroup", CONSUMER_GROUP)
      .addConfiguration("zookeeperConnect", zkConnect)
      .addConfiguration("maxBatchSize", 9)
      .addConfiguration("maxWaitTime", 5000)
      .addConfiguration("dataFormat", DataFormat.XML)
      .addConfiguration("charset", "UTF-8")
      .addConfiguration("jsonContent", null)
      .addConfiguration("kafkaConsumerConfigs", null)
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

    sourceRunner.runInit();

    startLatch.countDown();
    StageRunner.Output output = sourceRunner.runProduce(null, 9);
    shutDownExecutorService(executorService);

    String newOffset = output.getNewOffset();
    Assert.assertNull(newOffset);

    List<Record> records = output.getRecords().get("lane");
    Assert.assertEquals(9, records.size());

    sourceRunner.runDestroy();
  }

  @Test
  public void testProduceXmlRecordsRecordElement() throws StageException, IOException, InterruptedException {

    CountDownLatch startLatch = new CountDownLatch(1);
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new ProducerRunnable(TOPIC7, SINGLE_PARTITION, producer, startLatch, DataType.XML, null, -1,
      null));

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class)
      .addOutputLane("lane")
      .addConfiguration("topic", TOPIC7)
      .addConfiguration("metadataBrokerList", KafkaTestUtil.getMetadataBrokerURI())
      .addConfiguration("consumerGroup", CONSUMER_GROUP)
      .addConfiguration("zookeeperConnect", zkConnect)
      .addConfiguration("maxBatchSize", 9)
      .addConfiguration("maxWaitTime", 5000)
      .addConfiguration("dataFormat", DataFormat.XML)
      .addConfiguration("charset", "UTF-8")
      .addConfiguration("jsonContent", null)
      .addConfiguration("kafkaConsumerConfigs", null)
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

    sourceRunner.runInit();

    startLatch.countDown();
    StageRunner.Output output = sourceRunner.runProduce(null, 9);
    shutDownExecutorService(executorService);

    String newOffset = output.getNewOffset();
    Assert.assertNull(newOffset);

    List<Record> records = output.getRecords().get("lane");
    // we stop at 10 because each message has an XML with 2 authors (one record each)
    Assert.assertEquals(10, records.size());

    sourceRunner.runDestroy();
  }

  @Test(expected = StageException.class)
  public void testProduceXmlRecordsRecordElementSingleRecordPerMessage() throws StageException, IOException {
    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class)
      .addOutputLane("lane")
      .addConfiguration("topic", TOPIC8)
      .addConfiguration("consumerGroup", CONSUMER_GROUP)
      .addConfiguration("metadataBrokerList", KafkaTestUtil.getMetadataBrokerURI())
      .addConfiguration("zookeeperConnect", zkConnect)
      .addConfiguration("maxBatchSize", 9)
      .addConfiguration("maxWaitTime", 5000)
      .addConfiguration("dataFormat", DataFormat.XML)
      .addConfiguration("charset", "UTF-8")
      .addConfiguration("jsonContent", null)
      .addConfiguration("kafkaConsumerConfigs", null)
      .addConfiguration("produceSingleRecordPerMessage", true)
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

    sourceRunner.runInit();
  }

  @Test
  public void testProduceCsvRecords() throws StageException, IOException, InterruptedException {
    CountDownLatch startLatch = new CountDownLatch(1);
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new ProducerRunnable(TOPIC9, SINGLE_PARTITION,
      producer, startLatch, DataType.CSV, null, -1, null));

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class)
      .addOutputLane("lane")
      .addConfiguration("topic", TOPIC9)
      .addConfiguration("metadataBrokerList", KafkaTestUtil.getMetadataBrokerURI())
      .addConfiguration("consumerGroup", CONSUMER_GROUP)
      .addConfiguration("zookeeperConnect", zkConnect)
      .addConfiguration("maxBatchSize", 9)
      .addConfiguration("maxWaitTime", 5000)
      .addConfiguration("dataFormat", DataFormat.DELIMITED)
      .addConfiguration("charset", "UTF-8")
      .addConfiguration("csvFileFormat", CsvMode.CSV)
      .addConfiguration("csvHeader", CsvHeader.NO_HEADER)
      .addConfiguration("csvMaxObjectLen", 4096)
      .addConfiguration("kafkaConsumerConfigs", null)
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

    sourceRunner.runInit();

    startLatch.countDown();
    StageRunner.Output output = sourceRunner.runProduce(null, 9);
    shutDownExecutorService(executorService);

    String newOffset = output.getNewOffset();
    Assert.assertNull(newOffset);
    List<Record> records = output.getRecords().get("lane");
    Assert.assertEquals(9, records.size());

    sourceRunner.runDestroy();
  }

  @Test
  public void testProduceLogRecords() throws StageException, IOException, InterruptedException {

    CountDownLatch startLatch = new CountDownLatch(1);
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new ProducerRunnable(TOPIC10, SINGLE_PARTITION, producer, startLatch, DataType.LOG, null,
      -1, null));

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class)
      .addOutputLane("lane")
      .addConfiguration("topic", TOPIC10)
      .addConfiguration("metadataBrokerList", KafkaTestUtil.getMetadataBrokerURI())
      .addConfiguration("consumerGroup", CONSUMER_GROUP)
      .addConfiguration("zookeeperConnect", zkConnect)
      .addConfiguration("maxBatchSize", 9)
      .addConfiguration("maxWaitTime", 5000)
      .addConfiguration("dataFormat", DataFormat.LOG)
      .addConfiguration("charset", "UTF-8")
      .addConfiguration("jsonContent", null)
      .addConfiguration("kafkaConsumerConfigs", null)
      .addConfiguration("produceSingleRecordPerMessage", false)
      .addConfiguration("xmlRecordElement", "")
      .addConfiguration("xmlMaxObjectLen", null)
      .addConfiguration("logMode", LogMode.LOG4J)
      .addConfiguration("logMaxObjectLen", 1024)
      .addConfiguration("regex", null)
      .addConfiguration("grokPatternDefinition", null)
      .addConfiguration("enableLog4jCustomLogFormat", false)
      .addConfiguration("customLogFormat", null)
      .addConfiguration("fieldPathsToGroupName", null)
      .addConfiguration("log4jCustomLogFormat", null)
      .addConfiguration("grokPattern", null)
      .addConfiguration("onParseError", OnParseError.INCLUDE_AS_STACK_TRACE)
      .addConfiguration("maxStackTraceLines", 10)
      .addConfiguration("retainOriginalLine", true)
      .build();

    sourceRunner.runInit();

    startLatch.countDown();
    StageRunner.Output output = sourceRunner.runProduce(null, 9);
    shutDownExecutorService(executorService);

    String newOffset = output.getNewOffset();
    Assert.assertNull(newOffset);

    List<Record> records = output.getRecords().get("lane");
    Assert.assertEquals(9, records.size());

    for(Record record : records) {
      Assert.assertEquals(KafkaTestUtil.generateTestData(DataType.LOG, null),
        record.get().getValueAsMap().get("originalLine").getValueAsString());

      Assert.assertFalse(record.has("/truncated"));

      Assert.assertTrue(record.has("/" + Constants.TIMESTAMP));
      Assert.assertEquals("2015-03-20 15:53:31,161", record.get("/" + Constants.TIMESTAMP).getValueAsString());

      Assert.assertTrue(record.has("/" + Constants.SEVERITY));
      Assert.assertEquals("DEBUG", record.get("/" + Constants.SEVERITY).getValueAsString());

      Assert.assertTrue(record.has("/" + Constants.CATEGORY));
      Assert.assertEquals("PipelineConfigurationValidator", record.get("/" + Constants.CATEGORY).getValueAsString());

      Assert.assertTrue(record.has("/" + Constants.MESSAGE));
      Assert.assertEquals("Pipeline 'test:preview' validation. valid=true, canPreview=true, issuesCount=0",
        record.get("/" + Constants.MESSAGE).getValueAsString());
    }
    sourceRunner.runDestroy();
  }

  @Test
  public void testProduceLogRecordsWithStackTraceSameMessage() throws StageException, IOException, InterruptedException {

    CountDownLatch startLatch = new CountDownLatch(1);
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new ProducerRunnable(TOPIC11, SINGLE_PARTITION, producer, startLatch,
      DataType.LOG_STACK_TRACE, null, -1, null));

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class)
      .addOutputLane("lane")
      .addConfiguration("topic", TOPIC11)
      .addConfiguration("metadataBrokerList", KafkaTestUtil.getMetadataBrokerURI())
      .addConfiguration("consumerGroup", CONSUMER_GROUP)
      .addConfiguration("zookeeperConnect", zkConnect)
      .addConfiguration("maxBatchSize", 9)
      .addConfiguration("maxWaitTime", 10000)
      .addConfiguration("dataFormat", DataFormat.LOG)
      .addConfiguration("charset", "UTF-8")
      .addConfiguration("jsonContent", null)
      .addConfiguration("kafkaConsumerConfigs", null)
      .addConfiguration("produceSingleRecordPerMessage", false)
      .addConfiguration("xmlRecordElement", "")
      .addConfiguration("xmlMaxObjectLen", null)
      .addConfiguration("logMode", LogMode.LOG4J)
      .addConfiguration("logMaxObjectLen", 10000)
      .addConfiguration("regex", null)
      .addConfiguration("grokPatternDefinition", null)
      .addConfiguration("enableLog4jCustomLogFormat", false)
      .addConfiguration("customLogFormat", null)
      .addConfiguration("fieldPathsToGroupName", null)
      .addConfiguration("log4jCustomLogFormat", null)
      .addConfiguration("grokPattern", null)
      .addConfiguration("onParseError", OnParseError.INCLUDE_AS_STACK_TRACE)
      .addConfiguration("maxStackTraceLines", 100)
      .addConfiguration("retainOriginalLine", true)
      .build();

    sourceRunner.runInit();

    startLatch.countDown();
    StageRunner.Output output = sourceRunner.runProduce(null, 9);
    shutDownExecutorService(executorService);

    String newOffset = output.getNewOffset();
    Assert.assertNull(newOffset);

    List<Record> records = output.getRecords().get("lane");
    Assert.assertEquals(9, records.size());

    for(Record record : records) {
      Assert.assertEquals(KafkaTestUtil.generateTestData(DataType.LOG_STACK_TRACE, null),
        record.get().getValueAsMap().get("originalLine").getValueAsString());

      Assert.assertFalse(record.has("/truncated"));

      Assert.assertTrue(record.has("/" + Constants.TIMESTAMP));
      Assert.assertEquals("2015-03-24 17:49:16,808", record.get("/" + Constants.TIMESTAMP).getValueAsString());

      Assert.assertTrue(record.has("/" + Constants.SEVERITY));
      Assert.assertEquals("ERROR", record.get("/" + Constants.SEVERITY).getValueAsString());

      Assert.assertTrue(record.has("/" + Constants.CATEGORY));
      Assert.assertEquals("ExceptionToHttpErrorProvider", record.get("/" + Constants.CATEGORY).getValueAsString());

      Assert.assertTrue(record.has("/" + Constants.MESSAGE));
      Assert.assertEquals(KafkaTestUtil.ERROR_MSG_WITH_STACK_TRACE,
        record.get("/" + Constants.MESSAGE).getValueAsString());
    }

    sourceRunner.runDestroy();
  }

  // Check whether auto.offset.reset config set to smallest works for preview or not
  @Test
  public void testAutoOffsetResetSmallestConfig() throws Exception {
      CountDownLatch startLatch = new CountDownLatch(1);
      ExecutorService executorService = Executors.newSingleThreadExecutor();
      CountDownLatch countDownLatch = new CountDownLatch(1);
      executorService.submit(new ProducerRunnable(TOPIC11, SINGLE_PARTITION, producer, startLatch,
        DataType.LOG_STACK_TRACE, null, 10, countDownLatch));
      // produce all 10 records first before starting the source(KafkaConsumer)
      startLatch.countDown();
      countDownLatch.await();

      SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class)
        .addOutputLane("lane")
        .addConfiguration("topic", TOPIC11)
        .addConfiguration("metadataBrokerList", KafkaTestUtil.getMetadataBrokerURI())
        .addConfiguration("consumerGroup", CONSUMER_GROUP)
        .addConfiguration("zookeeperConnect", zkConnect)
        .addConfiguration("maxBatchSize", 100)
        .addConfiguration("maxWaitTime", 10000)
        .addConfiguration("dataFormat", DataFormat.LOG)
        .addConfiguration("charset", "UTF-8")
        .addConfiguration("jsonContent", null)
        .addConfiguration("kafkaConsumerConfigs", null)
        .addConfiguration("produceSingleRecordPerMessage", false)
        .addConfiguration("xmlRecordElement", "")
        .addConfiguration("xmlMaxObjectLen", null)
        .addConfiguration("logMode", LogMode.LOG4J)
        .addConfiguration("logMaxObjectLen", 10000)
        .addConfiguration("regex", null)
        .addConfiguration("grokPatternDefinition", null)
        .addConfiguration("enableLog4jCustomLogFormat", false)
        .addConfiguration("customLogFormat", null)
        .addConfiguration("fieldPathsToGroupName", null)
        .addConfiguration("log4jCustomLogFormat", null)
        .addConfiguration("grokPattern", null)
        .addConfiguration("onParseError", OnParseError.INCLUDE_AS_STACK_TRACE)
        .addConfiguration("maxStackTraceLines", 100)
        .addConfiguration("retainOriginalLine", true)
        // Set mode to preview
        .setPreview(true)
        .build();

      sourceRunner.runInit();

      StageRunner.Output output = sourceRunner.runProduce(null, 10);
      shutDownExecutorService(executorService);

      String newOffset = output.getNewOffset();
      Assert.assertNull(newOffset);

      List<Record> records = output.getRecords().get("lane");
      Assert.assertEquals(10, records.size());
  }


  private void shutDownExecutorService(ExecutorService executorService) throws InterruptedException {
    executorService.shutdownNow();
    if(!executorService.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
      //If it cant be stopped then throw exception
      throw new RuntimeException("Could not shutdown Executor service");
    }
  }

}
