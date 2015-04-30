/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.kafka;

import com.google.common.collect.ImmutableList;
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
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestKafkaSource {

  private static KafkaServer kafkaServer;
  private static ZkClient zkClient;
  private static EmbeddedZookeeper zkServer;
  private static int port;
  private static String zkConnect;

  private static Producer<String, String> producer;

  private static final String HOST = "localhost";
  private static final int BROKER_ID = 0;
  private static final int SINGLE_PARTITION = 1;
  private static final int MULTIPLE_PARTITIONS = 5;
  private static final int REPLICATION_FACTOR = 1;
  private static final String CONSUMER_GROUP = "SDC";
  private static final int TIME_OUT = 5000;

  private static String originalTmpDir;

  @BeforeClass
  public static void setUp() {
    //Init zookeeper
    originalTmpDir = System.getProperty("java.io.tmpdir");
    File testDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(testDir.mkdirs());
    System.setProperty("java.io.tmpdir", testDir.getAbsolutePath());

    zkConnect = TestZKUtils.zookeeperConnect();
    zkServer = new EmbeddedZookeeper(zkConnect);

    System.out.println("ZooKeeper log dir : " + zkServer.logDir().getAbsolutePath());
    zkClient = new ZkClient(zkServer.connectString(), 30000, 30000, ZKStringSerializer$.MODULE$);
    // setup Broker
    port = TestUtils.choosePort();

    Properties props = TestUtils.createBrokerConfig(BROKER_ID, port);
    List<KafkaServer> servers = new ArrayList<>();
    kafkaServer = TestUtils.createServer(new KafkaConfig(props), new MockTime());
    servers.add(kafkaServer);

    producer = KafkaTestUtil.createProducer(HOST, port, true);
    // remove this
    System.setProperty("sdc.clustermode", "false");
  }

  @AfterClass
  public static void tearDown() {
    kafkaServer.shutdown();
    zkClient.close();
    zkServer.shutdown();
    System.setProperty("java.io.tmpdir", originalTmpDir);
  }

  @Test
  @Ignore
  public void testProduceStringRecords() throws StageException {

    CountDownLatch startLatch = new CountDownLatch(1);
    KafkaTestUtil.createTopic(zkClient, ImmutableList.of(kafkaServer), "testProduceStringRecords", SINGLE_PARTITION,
      REPLICATION_FACTOR, TIME_OUT);
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new ProducerRunnable( "testProduceStringRecords", SINGLE_PARTITION,
      producer, startLatch, DataType.TEXT, null, -1, null));

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class)
      .addOutputLane("lane")
      .addConfiguration("metadataBrokerList", "dummyhost:1000")
      .addConfiguration("topic", "testProduceStringRecords")
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
    executorService.shutdown();

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
  @Ignore
  public void testProduceStringRecordsMultiplePartitions() throws StageException {

    CountDownLatch startProducing = new CountDownLatch(1);
    KafkaTestUtil.createTopic(zkClient, ImmutableList.of(kafkaServer), "testProduceStringRecordsMultiplePartitions",
      MULTIPLE_PARTITIONS, REPLICATION_FACTOR, TIME_OUT);
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new ProducerRunnable("testProduceStringRecordsMultiplePartitions",
      MULTIPLE_PARTITIONS, producer, startProducing, DataType.TEXT, null, -1, null));

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class)
      .addOutputLane("lane")
      .addConfiguration("metadataBrokerList", "dummyhost:1000")
      .addConfiguration("topic", "testProduceStringRecordsMultiplePartitions")
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
    executorService.shutdown();

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
  @Ignore
  public void testProduceJsonRecordsMultipleObjectsSingleRecord() throws StageException, IOException {

    CountDownLatch startLatch = new CountDownLatch(1);
    KafkaTestUtil.createTopic(zkClient, ImmutableList.of(kafkaServer),
      "testProduceJsonRecordsMultipleObjectsSingleRecord", SINGLE_PARTITION, REPLICATION_FACTOR, TIME_OUT);
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new ProducerRunnable("testProduceJsonRecordsMultipleObjectsSingleRecord", SINGLE_PARTITION,
      producer, startLatch, DataType.JSON, StreamingJsonParser.Mode.MULTIPLE_OBJECTS, -1, null));

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class)
        .addOutputLane("lane")
        .addConfiguration("metadataBrokerList", "dummyhost:1000")
        .addConfiguration("topic", "testProduceJsonRecordsMultipleObjectsSingleRecord")
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
    executorService.shutdown();

    String newOffset = output.getNewOffset();
    Assert.assertNull(newOffset);

    List<Record> records = output.getRecords().get("lane");
    Assert.assertEquals(9, records.size());

    sourceRunner.runDestroy();
  }

  @Test
  @Ignore
  public void testProduceJsonRecordsMultipleObjectsMultipleRecord() throws StageException, IOException {

    CountDownLatch startLatch = new CountDownLatch(1);
    KafkaTestUtil.createTopic(zkClient, ImmutableList.of(kafkaServer), "testProduceJsonRecordsMultipleObjectsMultipleRecord", SINGLE_PARTITION,
      REPLICATION_FACTOR, TIME_OUT);
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new ProducerRunnable("testProduceJsonRecordsMultipleObjectsMultipleRecord", SINGLE_PARTITION,
      producer, startLatch, DataType.JSON, StreamingJsonParser.Mode.MULTIPLE_OBJECTS, -1, null));

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class)
        .addOutputLane("lane")
        .addConfiguration("topic", "testProduceJsonRecordsMultipleObjectsMultipleRecord")
      .addConfiguration("metadataBrokerList", "dummyhost:1000")
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
    executorService.shutdown();

    String newOffset = output.getNewOffset();
    Assert.assertNull(newOffset);

    List<Record> records = output.getRecords().get("lane");
    Assert.assertEquals(12, records.size());

    sourceRunner.runDestroy();
  }

  @Test
  @Ignore
  public void testProduceJsonRecordsArrayObjects() throws StageException, IOException {

    CountDownLatch startLatch = new CountDownLatch(1);
    KafkaTestUtil.createTopic(zkClient, ImmutableList.of(kafkaServer), "testProduceJsonRecordsArrayObjects", SINGLE_PARTITION,
      REPLICATION_FACTOR, TIME_OUT);
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new ProducerRunnable("testProduceJsonRecordsArrayObjects", SINGLE_PARTITION,
      producer, startLatch, DataType.JSON, StreamingJsonParser.Mode.ARRAY_OBJECTS, -1, null));

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class)
        .addOutputLane("lane")
        .addConfiguration("topic", "testProduceJsonRecordsArrayObjects")
      .addConfiguration("metadataBrokerList", "dummyhost:1000")
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
    executorService.shutdown();

    String newOffset = output.getNewOffset();
    Assert.assertNull(newOffset);

    List<Record> records = output.getRecords().get("lane");
    Assert.assertEquals(9, records.size());

    sourceRunner.runDestroy();
  }

  @Test
  @Ignore
  public void testProduceXmlRecordsNoRecordElement() throws StageException, IOException {

    CountDownLatch startLatch = new CountDownLatch(1);
    KafkaTestUtil.createTopic(zkClient, ImmutableList.of(kafkaServer), "testProduceXmlRecords1", SINGLE_PARTITION,
      REPLICATION_FACTOR, TIME_OUT);
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new ProducerRunnable("testProduceXmlRecords1", SINGLE_PARTITION,
      producer, startLatch, DataType.XML, null, -1, null));

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class)
      .addOutputLane("lane")
      .addConfiguration("topic", "testProduceXmlRecords1")
      .addConfiguration("metadataBrokerList", "dummyhost:1000")
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
    executorService.shutdown();

    String newOffset = output.getNewOffset();
    Assert.assertNull(newOffset);

    List<Record> records = output.getRecords().get("lane");
    Assert.assertEquals(9, records.size());

    sourceRunner.runDestroy();
  }

  @Test
  @Ignore
  public void testProduceXmlRecordsRecordElement() throws StageException, IOException {

    CountDownLatch startLatch = new CountDownLatch(1);
    KafkaTestUtil.createTopic(zkClient, ImmutableList.of(kafkaServer), "testProduceXmlRecords2", SINGLE_PARTITION,
                              REPLICATION_FACTOR, TIME_OUT);
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new ProducerRunnable("testProduceXmlRecords2", SINGLE_PARTITION,
      producer, startLatch, DataType.XML, null, -1, null));

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class)
        .addOutputLane("lane")
        .addConfiguration("topic", "testProduceXmlRecords2")
        .addConfiguration("metadataBrokerList", "dummyhost:1000")
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
    executorService.shutdown();

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
        .addConfiguration("topic", "testProduceXmlRecords3")
        .addConfiguration("consumerGroup", CONSUMER_GROUP)
        .addConfiguration("metadataBrokerList", "dummyhost:1000")
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
  @Ignore
  public void testProduceCsvRecords() throws StageException, IOException {
    CountDownLatch startLatch = new CountDownLatch(1);
    KafkaTestUtil.createTopic(zkClient, ImmutableList.of(kafkaServer), "testProduceCsvRecords", SINGLE_PARTITION,
      REPLICATION_FACTOR, TIME_OUT);
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new ProducerRunnable("testProduceCsvRecords", SINGLE_PARTITION,
      producer, startLatch, DataType.CSV, null, -1, null));

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class)
      .addOutputLane("lane")
      .addConfiguration("topic", "testProduceCsvRecords")
      .addConfiguration("metadataBrokerList", "dummyhost:1000")
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
    executorService.shutdown();

    String newOffset = output.getNewOffset();
    Assert.assertNull(newOffset);
    List<Record> records = output.getRecords().get("lane");
    Assert.assertEquals(9, records.size());

    sourceRunner.runDestroy();
  }

  @Test
  @Ignore
  public void testProduceLogRecords() throws StageException, IOException {

    CountDownLatch startLatch = new CountDownLatch(1);
    KafkaTestUtil.createTopic(zkClient, ImmutableList.of(kafkaServer), "testProduceLogRecords", SINGLE_PARTITION,
      REPLICATION_FACTOR, TIME_OUT);
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new ProducerRunnable("testProduceLogRecords", SINGLE_PARTITION,
      producer, startLatch, DataType.LOG, null, -1, null));

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class)
      .addOutputLane("lane")
      .addConfiguration("topic", "testProduceLogRecords")
      .addConfiguration("metadataBrokerList", "dummyhost:1000")
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
    executorService.shutdown();

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
  @Ignore
  public void testProduceLogRecordsWithStackTraceSameMessage() throws StageException, IOException {

    CountDownLatch startLatch = new CountDownLatch(1);
    KafkaTestUtil.createTopic(zkClient, ImmutableList.of(kafkaServer), "testProduceLogRecordsWithStackTraceSameMessage", SINGLE_PARTITION,
      REPLICATION_FACTOR, TIME_OUT);
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new ProducerRunnable("testProduceLogRecordsWithStackTraceSameMessage", SINGLE_PARTITION,
      producer, startLatch, DataType.LOG_STACK_TRACE, null, -1, null));

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class)
      .addOutputLane("lane")
      .addConfiguration("topic", "testProduceLogRecordsWithStackTraceSameMessage")
      .addConfiguration("metadataBrokerList", "dummyhost:1000")
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
    executorService.shutdown();

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

}
