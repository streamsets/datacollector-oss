/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.kafka;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
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
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TestKafkaSource {

  private static final String AVRO_SCHEMA = "{\n"
    +"\"type\": \"record\",\n"
    +"\"name\": \"Employee\",\n"
    +"\"fields\": [\n"
    +" {\"name\": \"name\", \"type\": \"string\"},\n"
    +" {\"name\": \"age\", \"type\": \"int\"},\n"
    +" {\"name\": \"emails\", \"type\": {\"type\": \"array\", \"items\": \"string\"}},\n"
    +" {\"name\": \"boss\", \"type\": [\"Employee\",\"null\"]}\n"
    +"]}";

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
  private static final String TOPIC12 = "TestKafkaSource12";
  private static final String TOPIC13 = "TestKafkaSource13";
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
    KafkaTestUtil.createTopic(TOPIC12, SINGLE_PARTITION, SINGLE_REPLICATION_FACTOR);
    KafkaTestUtil.createTopic(TOPIC13, SINGLE_PARTITION, SINGLE_REPLICATION_FACTOR);

    producer = KafkaTestUtil.createProducer(KafkaTestUtil.getMetadataBrokerURI(), true);

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
      .addConfiguration("removeCtrlChars", false)
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
      .addConfiguration("removeCtrlChars", false)
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
      .addConfiguration("removeCtrlChars", false)
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
      .addConfiguration("removeCtrlChars", false)
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
      .addConfiguration("removeCtrlChars", false)
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
      .addConfiguration("removeCtrlChars", false)
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
      .addConfiguration("removeCtrlChars", false)
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
      .addConfiguration("removeCtrlChars", false)
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
      .addConfiguration("removeCtrlChars", false)
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
      .addConfiguration("removeCtrlChars", false)
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
      .addConfiguration("removeCtrlChars", false)
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
        .addConfiguration("removeCtrlChars", false)
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

  @Test
  public void testProduceAvroRecordsWithSchema() throws Exception {

    //create Producer and send messages
    Schema schema = new Schema.Parser().parse(AVRO_SCHEMA);
    GenericRecord boss = new GenericData.Record(schema);
    boss.put("name", "boss");
    boss.put("age", 60);
    boss.put("emails", ImmutableList.of("boss@company.com", "boss2@company.com"));
    boss.put("boss", null);

    GenericRecord e3 = new GenericData.Record(schema);
    e3.put("name", "c");
    e3.put("age", 50);
    e3.put("emails", ImmutableList.of("c@company.com", "c2@company.com"));
    e3.put("boss", boss);

    GenericRecord e2 = new GenericData.Record(schema);
    e2.put("name", "b");
    e2.put("age", 40);
    e2.put("emails", ImmutableList.of("b@company.com", "b2@company.com"));
    e2.put("boss", boss);

    GenericRecord e1 = new GenericData.Record(schema);
    e1.put("name", "a");
    e1.put("age", 30);
    e1.put("emails", ImmutableList.of("a@company.com", "a2@company.com"));
    e1.put("boss", boss);


    Properties props = new Properties();
    props.put("metadata.broker.list", KafkaTestUtil.getMetadataBrokerURI());
    props.put("serializer.class", "kafka.serializer.DefaultEncoder");
    props.put("key.serializer.class", "kafka.serializer.StringEncoder");
    props.put("request.required.acks", "1");
    ProducerConfig config = new ProducerConfig(props);
    Producer<String, byte[]> producer = new Producer<>(config);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.create(schema, baos);
    dataFileWriter.append(e1);
    dataFileWriter.flush();
    dataFileWriter.close();
    producer.send(new KeyedMessage<>(TOPIC12, "0", baos.toByteArray()));

    baos.reset();
    dataFileWriter.create(schema, baos);
    dataFileWriter.append(e2);
    dataFileWriter.flush();
    dataFileWriter.close();
    producer.send(new KeyedMessage<>(TOPIC12, "0", baos.toByteArray()));

    baos.reset();
    dataFileWriter.create(schema, baos);
    dataFileWriter.append(e3);
    dataFileWriter.flush();
    dataFileWriter.close();
    producer.send(new KeyedMessage<>(TOPIC12, "0", baos.toByteArray()));


    Map<String, String> kafkaConsumerConfigs = new HashMap<>();
    kafkaConsumerConfigs.put("auto.offset.reset", "smallest");

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class)
      .addOutputLane("lane")
      .addConfiguration("topic", TOPIC12)
      .addConfiguration("metadataBrokerList", KafkaTestUtil.getMetadataBrokerURI())
      .addConfiguration("consumerGroup", CONSUMER_GROUP)
      .addConfiguration("zookeeperConnect", zkConnect)
      .addConfiguration("maxBatchSize", 100)
      .addConfiguration("maxWaitTime", 10000)
      .addConfiguration("dataFormat", DataFormat.AVRO)
      .addConfiguration("charset", "UTF-8")
      .addConfiguration("jsonContent", null)
      .addConfiguration("kafkaConsumerConfigs", kafkaConsumerConfigs)
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
      .addConfiguration("avroSchema", AVRO_SCHEMA)
      .addConfiguration("schemaInMessage", true)
      .addConfiguration("removeCtrlChars", false)
      .build();

    sourceRunner.runInit();

    StageRunner.Output output = sourceRunner.runProduce(null, 10);

    String newOffset = output.getNewOffset();
    Assert.assertNull(newOffset);

    List<Record> records = output.getRecords().get("lane");
    Assert.assertEquals(3, records.size());

    Record e3Record = records.get(2);
    Assert.assertTrue(e3Record.has("/name"));
    Assert.assertEquals("c", e3Record.get("/name").getValueAsString());
    Assert.assertTrue(e3Record.has("/age"));
    Assert.assertEquals(50, e3Record.get("/age").getValueAsInteger());
    Assert.assertTrue(e3Record.has("/emails"));
    Assert.assertTrue(e3Record.get("/emails").getValueAsList() instanceof List);
    List<Field> emails = e3Record.get("/emails").getValueAsList();
    Assert.assertEquals(2, emails.size());
    Assert.assertEquals("c@company.com", emails.get(0).getValueAsString());
    Assert.assertEquals("c2@company.com", emails.get(1).getValueAsString());
    Assert.assertTrue(e3Record.has("/boss"));
    Assert.assertTrue(e3Record.get("/boss").getValueAsMap() instanceof Map);
    Assert.assertTrue(e3Record.has("/boss/name"));
    Assert.assertEquals("boss", e3Record.get("/boss/name").getValueAsString());
    Assert.assertTrue(e3Record.has("/boss/age"));
    Assert.assertEquals(60, e3Record.get("/boss/age").getValueAsInteger());
    Assert.assertTrue(e3Record.has("/boss/emails"));
    Assert.assertTrue(e3Record.get("/boss/emails").getValueAsList() instanceof List);
    emails = e3Record.get("/boss/emails").getValueAsList();
    Assert.assertEquals(2, emails.size());
    Assert.assertEquals("boss@company.com", emails.get(0).getValueAsString());
    Assert.assertEquals("boss2@company.com", emails.get(1).getValueAsString());

    Record e2Record = records.get(1);
    Assert.assertTrue(e2Record.has("/name"));
    Assert.assertEquals("b", e2Record.get("/name").getValueAsString());
    Assert.assertTrue(e2Record.has("/age"));
    Assert.assertEquals(40, e2Record.get("/age").getValueAsInteger());
    Assert.assertTrue(e2Record.has("/emails"));
    Assert.assertTrue(e2Record.get("/emails").getValueAsList() instanceof List);
    emails = e2Record.get("/emails").getValueAsList();
    Assert.assertEquals(2, emails.size());
    Assert.assertEquals("b@company.com", emails.get(0).getValueAsString());
    Assert.assertEquals("b2@company.com", emails.get(1).getValueAsString());
    Assert.assertTrue(e2Record.has("/boss"));
    Assert.assertTrue(e2Record.get("/boss").getValueAsMap() instanceof Map);
    Assert.assertTrue(e2Record.has("/boss/name"));
    Assert.assertEquals("boss", e2Record.get("/boss/name").getValueAsString());
    Assert.assertTrue(e2Record.has("/boss/age"));
    Assert.assertEquals(60, e2Record.get("/boss/age").getValueAsInteger());
    Assert.assertTrue(e2Record.has("/boss/emails"));
    Assert.assertTrue(e2Record.get("/boss/emails").getValueAsList() instanceof List);
    emails = e2Record.get("/boss/emails").getValueAsList();
    Assert.assertEquals(2, emails.size());
    Assert.assertEquals("boss@company.com", emails.get(0).getValueAsString());
    Assert.assertEquals("boss2@company.com", emails.get(1).getValueAsString());

    Record e1Record = records.get(0);
    Assert.assertTrue(e1Record.has("/name"));
    Assert.assertEquals("a", e1Record.get("/name").getValueAsString());
    Assert.assertTrue(e1Record.has("/age"));
    Assert.assertEquals(30, e1Record.get("/age").getValueAsInteger());
    Assert.assertTrue(e1Record.has("/emails"));
    Assert.assertTrue(e1Record.get("/emails").getValueAsList() instanceof List);
    emails = e1Record.get("/emails").getValueAsList();
    Assert.assertEquals(2, emails.size());
    Assert.assertEquals("a@company.com", emails.get(0).getValueAsString());
    Assert.assertEquals("a2@company.com", emails.get(1).getValueAsString());
    Assert.assertTrue(e1Record.has("/boss"));
    Assert.assertTrue(e1Record.get("/boss").getValueAsMap() instanceof Map);
    Assert.assertTrue(e1Record.has("/boss/name"));
    Assert.assertEquals("boss", e1Record.get("/boss/name").getValueAsString());
    Assert.assertTrue(e1Record.has("/boss/age"));
    Assert.assertEquals(60, e1Record.get("/boss/age").getValueAsInteger());
    Assert.assertTrue(e1Record.has("/boss/emails"));
    Assert.assertTrue(e1Record.get("/boss/emails").getValueAsList() instanceof List);
    emails = e1Record.get("/boss/emails").getValueAsList();
    Assert.assertEquals(2, emails.size());
    Assert.assertEquals("boss@company.com", emails.get(0).getValueAsString());
    Assert.assertEquals("boss2@company.com", emails.get(1).getValueAsString());

  }

  @Test
  public void testProduceAvroRecordsWithOutSchema() throws Exception {

    //create Producer and send messages
    Schema schema = new Schema.Parser().parse(AVRO_SCHEMA);
    GenericRecord boss = new GenericData.Record(schema);
    boss.put("name", "boss");
    boss.put("age", 60);
    boss.put("emails", ImmutableList.of("boss@company.com", "boss2@company.com"));
    boss.put("boss", null);

    GenericRecord e3 = new GenericData.Record(schema);
    e3.put("name", "c");
    e3.put("age", 50);
    e3.put("emails", ImmutableList.of("c@company.com", "c2@company.com"));
    e3.put("boss", boss);

    GenericRecord e2 = new GenericData.Record(schema);
    e2.put("name", "b");
    e2.put("age", 40);
    e2.put("emails", ImmutableList.of("b@company.com", "b2@company.com"));
    e2.put("boss", boss);

    GenericRecord e1 = new GenericData.Record(schema);
    e1.put("name", "a");
    e1.put("age", 30);
    e1.put("emails", ImmutableList.of("a@company.com", "a2@company.com"));
    e1.put("boss", boss);


    Properties props = new Properties();
    props.put("metadata.broker.list", KafkaTestUtil.getMetadataBrokerURI());
    props.put("serializer.class", "kafka.serializer.DefaultEncoder");
    props.put("key.serializer.class", "kafka.serializer.StringEncoder");
    props.put("request.required.acks", "1");
    ProducerConfig config = new ProducerConfig(props);
    Producer<String, byte[]> producer = new Producer<>(config);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    GenericDatumWriter<GenericRecord> genericDatumWriter = new GenericDatumWriter<>(schema);
    BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(baos, null);
    genericDatumWriter.write(e1, binaryEncoder);
    binaryEncoder.flush();
    producer.send(new KeyedMessage<>(TOPIC13, "0", baos.toByteArray()));

    baos.reset();
    genericDatumWriter.write(e2, binaryEncoder);
    binaryEncoder.flush();
    producer.send(new KeyedMessage<>(TOPIC13, "0", baos.toByteArray()));

    baos.reset();
    genericDatumWriter.write(e3, binaryEncoder);
    binaryEncoder.flush();
    producer.send(new KeyedMessage<>(TOPIC13, "0", baos.toByteArray()));

    Map<String, String> kafkaConsumerConfigs = new HashMap<>();
    kafkaConsumerConfigs.put("auto.offset.reset", "smallest");

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class)
      .addOutputLane("lane")
      .addConfiguration("topic", TOPIC13)
      .addConfiguration("metadataBrokerList", KafkaTestUtil.getMetadataBrokerURI())
      .addConfiguration("consumerGroup", CONSUMER_GROUP)
      .addConfiguration("zookeeperConnect", zkConnect)
      .addConfiguration("maxBatchSize", 100)
      .addConfiguration("maxWaitTime", 10000)
      .addConfiguration("dataFormat", DataFormat.AVRO)
      .addConfiguration("charset", "UTF-8")
      .addConfiguration("jsonContent", null)
      .addConfiguration("removeCtrlChars", false)
      .addConfiguration("kafkaConsumerConfigs", kafkaConsumerConfigs)
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
      .addConfiguration("avroSchema", AVRO_SCHEMA)
      .addConfiguration("schemaInMessage", false)
      .build();

    sourceRunner.runInit();
    StageRunner.Output output = sourceRunner.runProduce(null, 10);

    String newOffset = output.getNewOffset();
    Assert.assertNull(newOffset);

    List<Record> records = output.getRecords().get("lane");
    Assert.assertEquals(3, records.size());

    Record e3Record = records.get(2);
    Assert.assertTrue(e3Record.has("/name"));
    Assert.assertEquals("c", e3Record.get("/name").getValueAsString());
    Assert.assertTrue(e3Record.has("/age"));
    Assert.assertEquals(50, e3Record.get("/age").getValueAsInteger());
    Assert.assertTrue(e3Record.has("/emails"));
    Assert.assertTrue(e3Record.get("/emails").getValueAsList() instanceof List);
    List<Field> emails = e3Record.get("/emails").getValueAsList();
    Assert.assertEquals(2, emails.size());
    Assert.assertEquals("c@company.com", emails.get(0).getValueAsString());
    Assert.assertEquals("c2@company.com", emails.get(1).getValueAsString());
    Assert.assertTrue(e3Record.has("/boss"));
    Assert.assertTrue(e3Record.get("/boss").getValueAsMap() instanceof Map);
    Assert.assertTrue(e3Record.has("/boss/name"));
    Assert.assertEquals("boss", e3Record.get("/boss/name").getValueAsString());
    Assert.assertTrue(e3Record.has("/boss/age"));
    Assert.assertEquals(60, e3Record.get("/boss/age").getValueAsInteger());
    Assert.assertTrue(e3Record.has("/boss/emails"));
    Assert.assertTrue(e3Record.get("/boss/emails").getValueAsList() instanceof List);
    emails = e3Record.get("/boss/emails").getValueAsList();
    Assert.assertEquals(2, emails.size());
    Assert.assertEquals("boss@company.com", emails.get(0).getValueAsString());
    Assert.assertEquals("boss2@company.com", emails.get(1).getValueAsString());

    Record e2Record = records.get(1);
    Assert.assertTrue(e2Record.has("/name"));
    Assert.assertEquals("b", e2Record.get("/name").getValueAsString());
    Assert.assertTrue(e2Record.has("/age"));
    Assert.assertEquals(40, e2Record.get("/age").getValueAsInteger());
    Assert.assertTrue(e2Record.has("/emails"));
    Assert.assertTrue(e2Record.get("/emails").getValueAsList() instanceof List);
    emails = e2Record.get("/emails").getValueAsList();
    Assert.assertEquals(2, emails.size());
    Assert.assertEquals("b@company.com", emails.get(0).getValueAsString());
    Assert.assertEquals("b2@company.com", emails.get(1).getValueAsString());
    Assert.assertTrue(e2Record.has("/boss"));
    Assert.assertTrue(e2Record.get("/boss").getValueAsMap() instanceof Map);
    Assert.assertTrue(e2Record.has("/boss/name"));
    Assert.assertEquals("boss", e2Record.get("/boss/name").getValueAsString());
    Assert.assertTrue(e2Record.has("/boss/age"));
    Assert.assertEquals(60, e2Record.get("/boss/age").getValueAsInteger());
    Assert.assertTrue(e2Record.has("/boss/emails"));
    Assert.assertTrue(e2Record.get("/boss/emails").getValueAsList() instanceof List);
    emails = e2Record.get("/boss/emails").getValueAsList();
    Assert.assertEquals(2, emails.size());
    Assert.assertEquals("boss@company.com", emails.get(0).getValueAsString());
    Assert.assertEquals("boss2@company.com", emails.get(1).getValueAsString());

    Record e1Record = records.get(0);
    Assert.assertTrue(e1Record.has("/name"));
    Assert.assertEquals("a", e1Record.get("/name").getValueAsString());
    Assert.assertTrue(e1Record.has("/age"));
    Assert.assertEquals(30, e1Record.get("/age").getValueAsInteger());
    Assert.assertTrue(e1Record.has("/emails"));
    Assert.assertTrue(e1Record.get("/emails").getValueAsList() instanceof List);
    emails = e1Record.get("/emails").getValueAsList();
    Assert.assertEquals(2, emails.size());
    Assert.assertEquals("a@company.com", emails.get(0).getValueAsString());
    Assert.assertEquals("a2@company.com", emails.get(1).getValueAsString());
    Assert.assertTrue(e1Record.has("/boss"));
    Assert.assertTrue(e1Record.get("/boss").getValueAsMap() instanceof Map);
    Assert.assertTrue(e1Record.has("/boss/name"));
    Assert.assertEquals("boss", e1Record.get("/boss/name").getValueAsString());
    Assert.assertTrue(e1Record.has("/boss/age"));
    Assert.assertEquals(60, e1Record.get("/boss/age").getValueAsInteger());
    Assert.assertTrue(e1Record.has("/boss/emails"));
    Assert.assertTrue(e1Record.get("/boss/emails").getValueAsList() instanceof List);
    emails = e1Record.get("/boss/emails").getValueAsList();
    Assert.assertEquals(2, emails.size());
    Assert.assertEquals("boss@company.com", emails.get(0).getValueAsString());
    Assert.assertEquals("boss2@company.com", emails.get(1).getValueAsString());

  }

  private void shutDownExecutorService(ExecutorService executorService) throws InterruptedException {
    executorService.shutdownNow();
    if(!executorService.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
      //If it cant be stopped then throw exception
      throw new RuntimeException("Could not shutdown Executor service");
    }
  }

}
