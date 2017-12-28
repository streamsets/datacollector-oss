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
package com.streamsets.pipeline.stage.origin.kafka;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ext.json.Mode;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.CsvRecordType;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.DatagramMode;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.config.LogMode;
import com.streamsets.pipeline.config.OnParseError;
import com.streamsets.pipeline.config.OriginAvroSchemaSource;
import com.streamsets.pipeline.kafka.common.DataType;
import com.streamsets.pipeline.kafka.common.ProducerRunnable;
import com.streamsets.pipeline.kafka.common.SdcKafkaTestUtil;
import com.streamsets.pipeline.kafka.common.SdcKafkaTestUtilFactory;
import com.streamsets.pipeline.lib.parser.log.Constants;
import com.streamsets.pipeline.lib.udp.UDPConstants;
import com.streamsets.pipeline.lib.util.ProtobufTestUtil;
import com.streamsets.pipeline.lib.util.UDPTestUtil;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;
import com.streamsets.testing.SingleForkNoReuseTest;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.utils.TestUtils;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Category(SingleForkNoReuseTest.class)
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

  private static final Date timestamp = new Date(1372392896000L);
  private static String SYSLOG;
  private static final String TEN_PACKETS = "netflow-v5-file-1";

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
  private static final String TOPIC14 = "TestKafkaSource14";
  private static final String TOPIC15 = "TestKafkaSource15";
  private static final String TOPIC16 = "TestKafkaSource16";
  private static final String TOPIC17 = "TestKafkaSource17";
  private static final String TOPIC18 = "TestKafkaSource18";
  private static final String TOPIC19 = "TestKafkaSource19";
  private static final String CONSUMER_GROUP = "SDC";
  public static final String COLLECTD_SIGNED_BIN = "collectd_signed.bin";
  public static final String COLLECTD_AUTH_TXT = "collectd_auth.txt";

  private static Producer<String, String> producer;
  private static String zkConnect;

  private static File tempDir;
  private static File protoDescFile;
  private static SdcKafkaTestUtil sdcKafkaTestUtil;


  @BeforeClass
  public static void setUp() throws IOException, InterruptedException {
    sdcKafkaTestUtil = SdcKafkaTestUtilFactory.getInstance().create();
    sdcKafkaTestUtil.startZookeeper();
    sdcKafkaTestUtil.startKafkaBrokers(3);

    zkConnect = sdcKafkaTestUtil.getZkConnect();

    sdcKafkaTestUtil.createTopic(TOPIC1, SINGLE_PARTITION, SINGLE_REPLICATION_FACTOR);
    sdcKafkaTestUtil.createTopic(TOPIC2, MULTIPLE_PARTITIONS, MULTIPLE_REPLICATION_FACTOR);
    sdcKafkaTestUtil.createTopic(TOPIC3, SINGLE_PARTITION, SINGLE_REPLICATION_FACTOR);
    sdcKafkaTestUtil.createTopic(TOPIC4, SINGLE_PARTITION, SINGLE_REPLICATION_FACTOR);
    sdcKafkaTestUtil.createTopic(TOPIC5, SINGLE_PARTITION, SINGLE_REPLICATION_FACTOR);
    sdcKafkaTestUtil.createTopic(TOPIC6, SINGLE_PARTITION, SINGLE_REPLICATION_FACTOR);
    sdcKafkaTestUtil.createTopic(TOPIC7, SINGLE_PARTITION, SINGLE_REPLICATION_FACTOR);
    sdcKafkaTestUtil.createTopic(TOPIC8, SINGLE_PARTITION, SINGLE_REPLICATION_FACTOR);
    sdcKafkaTestUtil.createTopic(TOPIC9, SINGLE_PARTITION, SINGLE_REPLICATION_FACTOR);
    sdcKafkaTestUtil.createTopic(TOPIC10, SINGLE_PARTITION, SINGLE_REPLICATION_FACTOR);
    sdcKafkaTestUtil.createTopic(TOPIC11, SINGLE_PARTITION, SINGLE_REPLICATION_FACTOR);
    sdcKafkaTestUtil.createTopic(TOPIC12, SINGLE_PARTITION, SINGLE_REPLICATION_FACTOR);
    sdcKafkaTestUtil.createTopic(TOPIC13, SINGLE_PARTITION, SINGLE_REPLICATION_FACTOR);
    sdcKafkaTestUtil.createTopic(TOPIC14, SINGLE_PARTITION, SINGLE_REPLICATION_FACTOR);
    sdcKafkaTestUtil.createTopic(TOPIC15, SINGLE_PARTITION, SINGLE_REPLICATION_FACTOR);
    sdcKafkaTestUtil.createTopic(TOPIC16, SINGLE_PARTITION, SINGLE_REPLICATION_FACTOR);
    sdcKafkaTestUtil.createTopic(TOPIC17, SINGLE_PARTITION, SINGLE_REPLICATION_FACTOR);
    sdcKafkaTestUtil.createTopic(TOPIC18, SINGLE_PARTITION, SINGLE_REPLICATION_FACTOR);
    sdcKafkaTestUtil.createTopic(TOPIC19, SINGLE_PARTITION, SINGLE_REPLICATION_FACTOR);

    for (int i = 1; i <= 16 ; i++) {
      TestUtils.waitUntilMetadataIsPropagated(
          scala.collection.JavaConversions.asScalaBuffer(sdcKafkaTestUtil.getKafkaServers()),
          "TestKafkaSource" + String.valueOf(i), 0, 5000);
      // For now, the only topic that needs more than one partition is topic 2. Eventually we should put all these into
      // a class and make sure we create the topics based on a list of Topic/Partition info, rather than this.
      if (i == 2) {
        for (int j = 0; j < MULTIPLE_PARTITIONS; j++) {
          TestUtils.waitUntilMetadataIsPropagated(
              scala.collection.JavaConversions.asScalaBuffer(sdcKafkaTestUtil.getKafkaServers()),
              "TestKafkaSource" + String.valueOf(i), j, 5000);
        }
      }
    }

    producer = sdcKafkaTestUtil.createProducer(sdcKafkaTestUtil.getMetadataBrokerURI(), true);
    tempDir = Files.createTempDir();
    protoDescFile = new File(tempDir, "Employee.desc");
    BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(protoDescFile));
    Resources.copy(Resources.getResource("Employee.desc"), out);
    out.flush();
    out.close();

    // Setup RFC5424 Syslog
    DateTime datetime = new DateTime(timestamp.getTime());
    String RFC5424_formatter = datetime.toDateTimeISO().toString();
    SYSLOG = "<34>1 " + RFC5424_formatter + " mymachine su: 'su root' failed for lonvick on /dev/pts/8";
  }

  @AfterClass
  public static void tearDown() {
    sdcKafkaTestUtil.shutdown();
    if (tempDir != null) {
      FileUtils.deleteQuietly(tempDir);
    }
  }

  private BaseKafkaSource createSource(KafkaConfigBean conf) {
    KafkaSourceFactory factory = new StandaloneKafkaSourceFactory(conf);
    return factory.create();
  }

  @Test
  public void testLibJarsRegex() throws Exception {
    StageDef sDef = KafkaDSource.class.getAnnotation(StageDef.class);
    Assert.assertEquals(
        Arrays.asList("spark-streaming-kafka.*", "kafka_\\d+.*", "kafka-clients-\\d+.*", "metrics-core-\\d+.*"),
        Arrays.asList(sDef.libJarsRegex())
    );
  }

  @Test
  public void testProduceStringRecords() throws StageException, InterruptedException {

    CountDownLatch startLatch = new CountDownLatch(1);

    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new ProducerRunnable(TOPIC1, SINGLE_PARTITION, producer, startLatch, DataType.TEXT, null, -1,
      null, sdcKafkaTestUtil));

    KafkaConfigBean conf = new KafkaConfigBean();
    conf.metadataBrokerList = sdcKafkaTestUtil.getMetadataBrokerURI();
    conf.topic = TOPIC1;
    conf.consumerGroup = CONSUMER_GROUP;
    conf.zookeeperConnect = zkConnect;
    conf.maxBatchSize = 9;
    conf.maxWaitTime = 5000;
    conf.produceSingleRecordPerMessage = false;
    conf.dataFormat = DataFormat.TEXT;
    conf.dataFormatConfig.charset = "UTF-8";
    conf.dataFormatConfig.removeCtrlChars = false;
    conf.dataFormatConfig.textMaxLineLen = 4096;

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class, createSource(conf))
      .addOutputLane("lane")
      .build();
    sourceRunner.runInit();

    startLatch.countDown();

    List<Record> records = new ArrayList<>();
    StageRunner.Output output = getOutputAndRecords(sourceRunner, 5, "lane", records);

    String newOffset = output.getNewOffset();
    Assert.assertNull(newOffset);

    shutDownExecutorService(executorService);
    Assert.assertEquals(5, records.size());

    long lastOffset = -1;
    for(int i = 0; i < records.size(); i++) {
      Assert.assertNotNull(records.get(i).get("/text"));
      Assert.assertTrue(!records.get(i).get("/text").getValueAsString().isEmpty());
      Assert.assertEquals(sdcKafkaTestUtil.generateTestData(DataType.TEXT, null),
        records.get(i).get("/text").getValueAsString());

      // Metadata about where the record originated
      Assert.assertEquals("For record: " + i, TOPIC1, records.get(i).getHeader().getAttribute(HeaderAttributeConstants.TOPIC));
      Assert.assertEquals("For record: " + i, "0", records.get(i).getHeader().getAttribute(HeaderAttributeConstants.PARTITION));

      if(lastOffset == -1) {
        lastOffset = Long.parseLong(records.get(i).getHeader().getAttribute(HeaderAttributeConstants.OFFSET));
      } else {
        Assert.assertEquals("For record: " + i, "" + ++lastOffset, records.get(i).getHeader().getAttribute(HeaderAttributeConstants.OFFSET));
      }
    }

    sourceRunner.runDestroy();
  }

  @Test
  public void testProduceStringRecordsMultiplePartitions() throws StageException, InterruptedException {

    CountDownLatch startProducing = new CountDownLatch(1);

    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new ProducerRunnable(TOPIC2, MULTIPLE_PARTITIONS, producer, startProducing, DataType.TEXT,
      null, -1, null, sdcKafkaTestUtil));

    KafkaConfigBean conf = new KafkaConfigBean();
    conf.metadataBrokerList = sdcKafkaTestUtil.getMetadataBrokerURI();
    conf.topic = TOPIC2;
    conf.consumerGroup = CONSUMER_GROUP;
    conf.zookeeperConnect = zkConnect;
    conf.maxBatchSize = 9;
    conf.maxWaitTime = 5000;
    conf.produceSingleRecordPerMessage = false;
    conf.dataFormat = DataFormat.TEXT;
    conf.dataFormatConfig.charset = "UTF-8";
    conf.dataFormatConfig.removeCtrlChars = false;
    conf.dataFormatConfig.textMaxLineLen = 4096;

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class, createSource(conf))
      .addOutputLane("lane")
      .build();

    sourceRunner.runInit();

    startProducing.countDown();

    List<Record> records = new ArrayList<>();
    StageRunner.Output output = getOutputAndRecords(sourceRunner, 9, "lane", records);

    shutDownExecutorService(executorService);

    String newOffset = output.getNewOffset();
    Assert.assertNull(newOffset);
    Assert.assertEquals(9, records.size());

    for(int i = 0; i < records.size(); i++) {
      Assert.assertNotNull(records.get(i).get("/text").getValueAsString());
      Assert.assertTrue(!records.get(i).get("/text").getValueAsString().isEmpty());
      Assert.assertEquals(sdcKafkaTestUtil.generateTestData(DataType.TEXT, null), records.get(i).get("/text").getValueAsString());
    }

    sourceRunner.runDestroy();
  }

  @Test
  public void testProduceJsonRecordsMultipleObjectsSingleRecord() throws StageException, IOException, InterruptedException {

    CountDownLatch startLatch = new CountDownLatch(1);
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new ProducerRunnable(TOPIC3, SINGLE_PARTITION, producer, startLatch, DataType.JSON,
      Mode.MULTIPLE_OBJECTS, -1, null, sdcKafkaTestUtil));

    KafkaConfigBean conf = new KafkaConfigBean();
    conf.metadataBrokerList = sdcKafkaTestUtil.getMetadataBrokerURI();
    conf.topic = TOPIC3;
    conf.consumerGroup = CONSUMER_GROUP;
    conf.zookeeperConnect = zkConnect;
    conf.maxBatchSize = 9;
    conf.maxWaitTime = 5000;
    conf.produceSingleRecordPerMessage = true;
    conf.dataFormat = DataFormat.JSON;
    conf.dataFormatConfig.charset = "UTF-8";
    conf.dataFormatConfig.removeCtrlChars = false;
    conf.dataFormatConfig.jsonContent = JsonMode.MULTIPLE_OBJECTS;
    conf.dataFormatConfig.jsonMaxObjectLen = 4096;

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class, createSource(conf))
      .addOutputLane("lane")
      .build();

    sourceRunner.runInit();

    startLatch.countDown();
    List<Record> records = new ArrayList<>();
    StageRunner.Output output = getOutputAndRecords(sourceRunner, 9, "lane", records);

    shutDownExecutorService(executorService);

    String newOffset = output.getNewOffset();
    Assert.assertNull(newOffset);

    Assert.assertEquals(9, records.size());

    sourceRunner.runDestroy();
  }

  @Test
  public void testProduceJsonRecordsMultipleObjectsMultipleRecord() throws StageException, IOException, InterruptedException {

    CountDownLatch startLatch = new CountDownLatch(1);
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new ProducerRunnable(TOPIC4, SINGLE_PARTITION, producer, startLatch, DataType.JSON,
      Mode.MULTIPLE_OBJECTS, -1, null, sdcKafkaTestUtil));

    KafkaConfigBean conf = new KafkaConfigBean();
    conf.metadataBrokerList = sdcKafkaTestUtil.getMetadataBrokerURI();
    conf.topic = TOPIC4;
    conf.consumerGroup = CONSUMER_GROUP;
    conf.zookeeperConnect = zkConnect;
    conf.maxBatchSize = 9;
    conf.maxWaitTime = 5000;
    conf.produceSingleRecordPerMessage = false;
    conf.dataFormat = DataFormat.JSON;
    conf.dataFormatConfig.charset = "UTF-8";
    conf.dataFormatConfig.removeCtrlChars = false;
    conf.dataFormatConfig.jsonContent = JsonMode.MULTIPLE_OBJECTS;
    conf.dataFormatConfig.jsonMaxObjectLen = 4096;

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class, createSource(conf))
      .addOutputLane("lane")
      .build();

    sourceRunner.runInit();

    startLatch.countDown();
    List<Record> records = new ArrayList<>();
    StageRunner.Output output = getOutputAndRecords(sourceRunner, 12, "lane", records);

    shutDownExecutorService(executorService);

    String newOffset = output.getNewOffset();
    Assert.assertNull(newOffset);
    Assert.assertEquals(12, records.size());

    sourceRunner.runDestroy();
  }

  @Test
  public void testProduceJsonRecordsArrayObjects() throws StageException, IOException, InterruptedException {

    CountDownLatch startLatch = new CountDownLatch(1);
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new ProducerRunnable(TOPIC5, SINGLE_PARTITION, producer, startLatch, DataType.JSON,
      Mode.ARRAY_OBJECTS, -1, null, sdcKafkaTestUtil));

    KafkaConfigBean conf = new KafkaConfigBean();
    conf.metadataBrokerList = sdcKafkaTestUtil.getMetadataBrokerURI();
    conf.topic = TOPIC5;
    conf.consumerGroup = CONSUMER_GROUP;
    conf.zookeeperConnect = zkConnect;
    conf.maxBatchSize = 9;
    conf.maxWaitTime = 5000;
    conf.produceSingleRecordPerMessage = true;
    conf.dataFormat = DataFormat.JSON;
    conf.dataFormatConfig.charset = "UTF-8";
    conf.dataFormatConfig.removeCtrlChars = false;
    conf.dataFormatConfig.jsonContent = JsonMode.ARRAY_OBJECTS;
    conf.dataFormatConfig.jsonMaxObjectLen = 4096;

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class, createSource(conf))
      .addOutputLane("lane")
      .build();

    sourceRunner.runInit();

    startLatch.countDown();
    List<Record> records = new ArrayList<>();
    StageRunner.Output output = getOutputAndRecords(sourceRunner, 9, "lane", records);

    shutDownExecutorService(executorService);

    String newOffset = output.getNewOffset();
    Assert.assertNull(newOffset);

    Assert.assertEquals(9, records.size());

    sourceRunner.runDestroy();
  }


  @Test
  public void testProduceXmlRecordsNoRecordElement() throws StageException, IOException, InterruptedException {

    CountDownLatch startLatch = new CountDownLatch(1);
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new ProducerRunnable(TOPIC6, SINGLE_PARTITION, producer, startLatch, DataType.XML, null, -1,
      null, sdcKafkaTestUtil));

    KafkaConfigBean conf = new KafkaConfigBean();
    conf.metadataBrokerList = sdcKafkaTestUtil.getMetadataBrokerURI();
    conf.topic = TOPIC6;
    conf.consumerGroup = CONSUMER_GROUP;
    conf.zookeeperConnect = zkConnect;
    conf.maxBatchSize = 9;
    conf.maxWaitTime = 5000;
    conf.produceSingleRecordPerMessage = false;
    conf.dataFormat = DataFormat.XML;
    conf.dataFormatConfig.charset = "UTF-8";
    conf.dataFormatConfig.removeCtrlChars = false;
    conf.dataFormatConfig.xmlRecordElement = "";
    conf.dataFormatConfig.xmlMaxObjectLen = 4096;

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class, createSource(conf))
      .addOutputLane("lane")
      .build();

    sourceRunner.runInit();

    startLatch.countDown();
    List<Record> records = new ArrayList<>();
    StageRunner.Output output = getOutputAndRecords(sourceRunner, 9, "lane", records);

    shutDownExecutorService(executorService);

    String newOffset = output.getNewOffset();
    Assert.assertNull(newOffset);
    Assert.assertEquals(9, records.size());

    sourceRunner.runDestroy();
  }

  @Test
  public void testProduceXmlRecordsRecordElement() throws StageException, IOException, InterruptedException {

    CountDownLatch startLatch = new CountDownLatch(1);
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new ProducerRunnable(TOPIC7, SINGLE_PARTITION, producer, startLatch, DataType.XML, null, -1,
      null, sdcKafkaTestUtil));

    KafkaConfigBean conf = new KafkaConfigBean();
    conf.metadataBrokerList = sdcKafkaTestUtil.getMetadataBrokerURI();
    conf.topic = TOPIC7;
    conf.consumerGroup = CONSUMER_GROUP;
    conf.zookeeperConnect = zkConnect;
    conf.maxBatchSize = 9;
    conf.maxWaitTime = 5000;
    conf.produceSingleRecordPerMessage = false;
    conf.dataFormat = DataFormat.XML;
    conf.dataFormatConfig.charset = "UTF-8";
    conf.dataFormatConfig.removeCtrlChars = false;
    conf.dataFormatConfig.xmlRecordElement = "author";
    conf.dataFormatConfig.xmlMaxObjectLen = 4096;

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class, createSource(conf))
      .addOutputLane("lane")
      .build();

    sourceRunner.runInit();

    startLatch.countDown();
    List<Record> records = new ArrayList<>();
    StageRunner.Output output = getOutputAndRecords(sourceRunner, 9, "lane", records);

    shutDownExecutorService(executorService);

    String newOffset = output.getNewOffset();
    Assert.assertNull(newOffset);

    // we stop at 10 because each message has an XML with 2 authors (one record each)
    Assert.assertEquals(10, records.size());

    sourceRunner.runDestroy();
  }

  @Test(expected = StageException.class)
  public void testProduceXmlRecordsRecordElementSingleRecordPerMessage() throws StageException, IOException {
    KafkaConfigBean conf = new KafkaConfigBean();
    conf.metadataBrokerList = sdcKafkaTestUtil.getMetadataBrokerURI();
    conf.topic = TOPIC8;
    conf.consumerGroup = CONSUMER_GROUP;
    conf.zookeeperConnect = zkConnect;
    conf.maxBatchSize = 9;
    conf.maxWaitTime = 5000;
    conf.produceSingleRecordPerMessage = true;
    conf.dataFormat = DataFormat.XML;
    conf.dataFormatConfig.charset = "UTF-8";
    conf.dataFormatConfig.removeCtrlChars = false;
    conf.dataFormatConfig.xmlRecordElement = "author";
    conf.dataFormatConfig.xmlMaxObjectLen = 4096;

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class, createSource(conf))
      .addOutputLane("lane")
      .build();

    sourceRunner.runInit();
  }

  @Test
  public void testProduceCsvRecords() throws StageException, IOException, InterruptedException {
    CountDownLatch startLatch = new CountDownLatch(1);
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new ProducerRunnable(TOPIC9, SINGLE_PARTITION,
      producer, startLatch, DataType.CSV, null, -1, null, sdcKafkaTestUtil));

    KafkaConfigBean conf = new KafkaConfigBean();
    conf.metadataBrokerList = sdcKafkaTestUtil.getMetadataBrokerURI();
    conf.topic = TOPIC9;
    conf.consumerGroup = CONSUMER_GROUP;
    conf.zookeeperConnect = zkConnect;
    conf.maxBatchSize = 9;
    conf.maxWaitTime = 5000;
    conf.produceSingleRecordPerMessage = false;
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
      .build();

    sourceRunner.runInit();

    startLatch.countDown();
    List<Record> records = new ArrayList<>();
    StageRunner.Output output = getOutputAndRecords(sourceRunner, 9, "lane", records);

    shutDownExecutorService(executorService);

    String newOffset = output.getNewOffset();
    Assert.assertNull(newOffset);
    Assert.assertEquals(9, records.size());

    sourceRunner.runDestroy();
  }

  @Test
  public void testProduceLogRecords() throws StageException, IOException, InterruptedException {

    CountDownLatch startLatch = new CountDownLatch(1);
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new ProducerRunnable(TOPIC10, SINGLE_PARTITION, producer, startLatch, DataType.LOG, null,
      -1, null, sdcKafkaTestUtil));

    KafkaConfigBean conf = new KafkaConfigBean();
    conf.metadataBrokerList = sdcKafkaTestUtil.getMetadataBrokerURI();
    conf.topic = TOPIC10;
    conf.consumerGroup = CONSUMER_GROUP;
    conf.zookeeperConnect = zkConnect;
    conf.maxBatchSize = 9;
    conf.maxWaitTime = 5000;
    conf.produceSingleRecordPerMessage = false;
    conf.dataFormat = DataFormat.LOG;
    conf.dataFormatConfig.charset = "UTF-8";
    conf.dataFormatConfig.removeCtrlChars = false;
    conf.dataFormatConfig.logMode = LogMode.LOG4J;
    conf.dataFormatConfig.logMaxObjectLen = 1024;
    conf.dataFormatConfig.retainOriginalLine = true;
    conf.dataFormatConfig.customLogFormat = null;
    conf.dataFormatConfig.regex = null;
    conf.dataFormatConfig.fieldPathsToGroupName = null;
    conf.dataFormatConfig.grokPatternDefinition = null;
    conf.dataFormatConfig.grokPattern = null;
    conf.dataFormatConfig.onParseError = OnParseError.INCLUDE_AS_STACK_TRACE;
    conf.dataFormatConfig.maxStackTraceLines = 10;
    conf.dataFormatConfig.enableLog4jCustomLogFormat = false;
    conf.dataFormatConfig.log4jCustomLogFormat = null;

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class, createSource(conf))
      .addOutputLane("lane")
      .build();

    sourceRunner.runInit();

    startLatch.countDown();
    List<Record> records = new ArrayList<>();
    StageRunner.Output output = getOutputAndRecords(sourceRunner, 9, "lane", records);

    shutDownExecutorService(executorService);

    String newOffset = output.getNewOffset();
    Assert.assertNull(newOffset);

    Assert.assertEquals(9, records.size());

    for(Record record : records) {
      Assert.assertEquals(sdcKafkaTestUtil.generateTestData(DataType.LOG, null),
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
      DataType.LOG_STACK_TRACE, null, -1, null, sdcKafkaTestUtil));

    KafkaConfigBean conf = new KafkaConfigBean();
    conf.metadataBrokerList = sdcKafkaTestUtil.getMetadataBrokerURI();
    conf.topic = TOPIC11;
    conf.consumerGroup = CONSUMER_GROUP;
    conf.zookeeperConnect = zkConnect;
    conf.maxBatchSize = 9;
    conf.maxWaitTime = 10000;
    conf.produceSingleRecordPerMessage = false;
    conf.dataFormat = DataFormat.LOG;
    conf.dataFormatConfig.charset = "UTF-8";
    conf.dataFormatConfig.removeCtrlChars = false;
    conf.dataFormatConfig.logMode = LogMode.LOG4J;
    conf.dataFormatConfig.logMaxObjectLen = 10000;
    conf.dataFormatConfig.retainOriginalLine = true;
    conf.dataFormatConfig.customLogFormat = null;
    conf.dataFormatConfig.regex = null;
    conf.dataFormatConfig.fieldPathsToGroupName = null;
    conf.dataFormatConfig.grokPatternDefinition = null;
    conf.dataFormatConfig.grokPattern = null;
    conf.dataFormatConfig.onParseError = OnParseError.INCLUDE_AS_STACK_TRACE;
    conf.dataFormatConfig.maxStackTraceLines = 100;
    conf.dataFormatConfig.enableLog4jCustomLogFormat = false;
    conf.dataFormatConfig.log4jCustomLogFormat = null;

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class, createSource(conf))
      .addOutputLane("lane")
      .build();

    sourceRunner.runInit();

    startLatch.countDown();
    List<Record> records = new ArrayList<>();
    StageRunner.Output output = getOutputAndRecords(sourceRunner, 9, "lane", records);

    shutDownExecutorService(executorService);

    String newOffset = output.getNewOffset();
    Assert.assertNull(newOffset);

    Assert.assertEquals(9, records.size());

    for(Record record : records) {
      Assert.assertEquals(sdcKafkaTestUtil.generateTestData(DataType.LOG_STACK_TRACE, null),
        record.get().getValueAsMap().get("originalLine").getValueAsString());

      Assert.assertFalse(record.has("/truncated"));

      Assert.assertTrue(record.has("/" + Constants.TIMESTAMP));
      Assert.assertEquals("2015-03-24 17:49:16,808", record.get("/" + Constants.TIMESTAMP).getValueAsString());

      Assert.assertTrue(record.has("/" + Constants.SEVERITY));
      Assert.assertEquals("ERROR", record.get("/" + Constants.SEVERITY).getValueAsString());

      Assert.assertTrue(record.has("/" + Constants.CATEGORY));
      Assert.assertEquals("ExceptionToHttpErrorProvider", record.get("/" + Constants.CATEGORY).getValueAsString());

      Assert.assertTrue(record.has("/" + Constants.MESSAGE));
      Assert.assertEquals(
          SdcKafkaTestUtil.ERROR_MSG_WITH_STACK_TRACE,
        record.get("/" + Constants.MESSAGE).getValueAsString());
    }

    sourceRunner.runDestroy();
  }

  @Test
  public void testAutoOffsetResetSmallestConfig() throws Exception {
    CountDownLatch startLatch = new CountDownLatch(1);
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    CountDownLatch countDownLatch = new CountDownLatch(1);
    executorService.submit(new ProducerRunnable(TOPIC11, SINGLE_PARTITION, producer, startLatch,
      DataType.LOG_STACK_TRACE, null, 10, countDownLatch, sdcKafkaTestUtil));
    // produce all 10 records first before starting the source(KafkaConsumer)
    startLatch.countDown();
    countDownLatch.await();

    KafkaConfigBean conf = new KafkaConfigBean();
    conf.metadataBrokerList = sdcKafkaTestUtil.getMetadataBrokerURI();
    conf.topic = TOPIC11;
    conf.consumerGroup = CONSUMER_GROUP;
    conf.zookeeperConnect = zkConnect;
    conf.maxBatchSize = 100;
    conf.maxWaitTime = 10000;
    conf.produceSingleRecordPerMessage = false;
    conf.dataFormat = DataFormat.LOG;
    conf.dataFormatConfig.charset = "UTF-8";
    conf.dataFormatConfig.removeCtrlChars = false;
    conf.dataFormatConfig.logMode = LogMode.LOG4J;
    conf.dataFormatConfig.logMaxObjectLen = 10000;
    conf.dataFormatConfig.retainOriginalLine = true;
    conf.dataFormatConfig.customLogFormat = null;
    conf.dataFormatConfig.regex = null;
    conf.dataFormatConfig.fieldPathsToGroupName = null;
    conf.dataFormatConfig.grokPatternDefinition = null;
    conf.dataFormatConfig.grokPattern = null;
    conf.dataFormatConfig.onParseError = OnParseError.INCLUDE_AS_STACK_TRACE;
    conf.dataFormatConfig.maxStackTraceLines = 100;
    conf.dataFormatConfig.enableLog4jCustomLogFormat = false;
    conf.dataFormatConfig.log4jCustomLogFormat = null;

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class, createSource(conf))
      .addOutputLane("lane")
        // Set mode to preview
      .setPreview(true)
      .build();

    sourceRunner.runInit();

    List<Record> records = new ArrayList<>();
    StageRunner.Output output = getOutputAndRecords(sourceRunner, 10, "lane", records);

    shutDownExecutorService(executorService);
    String newOffset = output.getNewOffset();
    Assert.assertNull(newOffset);
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
    props.put("metadata.broker.list", sdcKafkaTestUtil.getMetadataBrokerURI());
    props.put("serializer.class", "kafka.serializer.DefaultEncoder");
    props.put("key.serializer.class", "kafka.serializer.StringEncoder");
    props.put("request.required.acks", "-1");
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
    sdcKafkaTestUtil.setAutoOffsetReset(kafkaConsumerConfigs);

    KafkaConfigBean conf = new KafkaConfigBean();
    conf.metadataBrokerList = sdcKafkaTestUtil.getMetadataBrokerURI();
    conf.topic = TOPIC12;
    conf.consumerGroup = CONSUMER_GROUP;
    conf.zookeeperConnect = zkConnect;
    conf.maxBatchSize = 100;
    conf.maxWaitTime = 10000;
    conf.kafkaConsumerConfigs = kafkaConsumerConfigs;
    conf.produceSingleRecordPerMessage = false;
    conf.dataFormat = DataFormat.AVRO;
    conf.dataFormatConfig.charset = "UTF-8";
    conf.dataFormatConfig.removeCtrlChars = false;
    conf.dataFormatConfig.avroSchemaSource = OriginAvroSchemaSource.SOURCE;
    conf.dataFormatConfig.avroSchema = AVRO_SCHEMA;

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class, createSource(conf))
      .addOutputLane("lane")
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
    Assert.assertTrue(e3Record.get("/emails").getValueAsList() != null);
    List<Field> emails = e3Record.get("/emails").getValueAsList();
    Assert.assertEquals(2, emails.size());
    Assert.assertEquals("c@company.com", emails.get(0).getValueAsString());
    Assert.assertEquals("c2@company.com", emails.get(1).getValueAsString());
    Assert.assertTrue(e3Record.has("/boss"));
    Assert.assertTrue(e3Record.get("/boss").getValueAsMap() != null);
    Assert.assertTrue(e3Record.has("/boss/name"));
    Assert.assertEquals("boss", e3Record.get("/boss/name").getValueAsString());
    Assert.assertTrue(e3Record.has("/boss/age"));
    Assert.assertEquals(60, e3Record.get("/boss/age").getValueAsInteger());
    Assert.assertTrue(e3Record.has("/boss/emails"));
    Assert.assertTrue(e3Record.get("/boss/emails").getValueAsList() != null);
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
    Assert.assertTrue(e2Record.get("/emails").getValueAsList() != null);
    emails = e2Record.get("/emails").getValueAsList();
    Assert.assertEquals(2, emails.size());
    Assert.assertEquals("b@company.com", emails.get(0).getValueAsString());
    Assert.assertEquals("b2@company.com", emails.get(1).getValueAsString());
    Assert.assertTrue(e2Record.has("/boss"));
    Assert.assertTrue(e2Record.get("/boss").getValueAsMap() != null);
    Assert.assertTrue(e2Record.has("/boss/name"));
    Assert.assertEquals("boss", e2Record.get("/boss/name").getValueAsString());
    Assert.assertTrue(e2Record.has("/boss/age"));
    Assert.assertEquals(60, e2Record.get("/boss/age").getValueAsInteger());
    Assert.assertTrue(e2Record.has("/boss/emails"));
    Assert.assertTrue(e2Record.get("/boss/emails").getValueAsList() != null);
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
    Assert.assertTrue(e1Record.get("/emails").getValueAsList() != null);
    emails = e1Record.get("/emails").getValueAsList();
    Assert.assertEquals(2, emails.size());
    Assert.assertEquals("a@company.com", emails.get(0).getValueAsString());
    Assert.assertEquals("a2@company.com", emails.get(1).getValueAsString());
    Assert.assertTrue(e1Record.has("/boss"));
    Assert.assertTrue(e1Record.get("/boss").getValueAsMap() != null);
    Assert.assertTrue(e1Record.has("/boss/name"));
    Assert.assertEquals("boss", e1Record.get("/boss/name").getValueAsString());
    Assert.assertTrue(e1Record.has("/boss/age"));
    Assert.assertEquals(60, e1Record.get("/boss/age").getValueAsInteger());
    Assert.assertTrue(e1Record.has("/boss/emails"));
    Assert.assertTrue(e1Record.get("/boss/emails").getValueAsList() != null);
    emails = e1Record.get("/boss/emails").getValueAsList();
    Assert.assertEquals(2, emails.size());
    Assert.assertEquals("boss@company.com", emails.get(0).getValueAsString());
    Assert.assertEquals("boss2@company.com", emails.get(1).getValueAsString());

  }

  @Ignore
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


    Producer<String, byte[]> producer = createDefaultProducer();

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
    sdcKafkaTestUtil.setAutoOffsetReset(kafkaConsumerConfigs);

    KafkaConfigBean conf = new KafkaConfigBean();
    conf.metadataBrokerList = sdcKafkaTestUtil.getMetadataBrokerURI();
    conf.topic = TOPIC13;
    conf.consumerGroup = CONSUMER_GROUP;
    conf.zookeeperConnect = zkConnect;
    conf.maxBatchSize = 100;
    conf.maxWaitTime = 10000;
    conf.kafkaConsumerConfigs = kafkaConsumerConfigs;
    conf.produceSingleRecordPerMessage = false;
    conf.dataFormat = DataFormat.AVRO;
    conf.dataFormatConfig.charset = "UTF-8";
    conf.dataFormatConfig.removeCtrlChars = false;
    conf.dataFormatConfig.avroSchemaSource = OriginAvroSchemaSource.INLINE;
    conf.dataFormatConfig.avroSchema = AVRO_SCHEMA;

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class, createSource(conf))
      .addOutputLane("lane")
      .build();

    sourceRunner.runInit();
    StageRunner.Output output = sourceRunner.runProduce(null, 10);

    String newOffset = output.getNewOffset();
    Assert.assertNull(newOffset);

    List<Record> records = output.getRecords().get("lane");
    Assert.assertEquals(0, sourceRunner.getErrorRecords().size());
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
    Assert.assertTrue(e2Record.get("/emails").getValueAsList() != null);
    emails = e2Record.get("/emails").getValueAsList();
    Assert.assertEquals(2, emails.size());
    Assert.assertEquals("b@company.com", emails.get(0).getValueAsString());
    Assert.assertEquals("b2@company.com", emails.get(1).getValueAsString());
    Assert.assertTrue(e2Record.has("/boss"));
    Assert.assertTrue(e2Record.get("/boss").getValueAsMap() != null);
    Assert.assertTrue(e2Record.has("/boss/name"));
    Assert.assertEquals("boss", e2Record.get("/boss/name").getValueAsString());
    Assert.assertTrue(e2Record.has("/boss/age"));
    Assert.assertEquals(60, e2Record.get("/boss/age").getValueAsInteger());
    Assert.assertTrue(e2Record.has("/boss/emails"));
    Assert.assertTrue(e2Record.get("/boss/emails").getValueAsList() != null);
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
    Assert.assertTrue(e1Record.get("/emails").getValueAsList() != null);
    emails = e1Record.get("/emails").getValueAsList();
    Assert.assertEquals(2, emails.size());
    Assert.assertEquals("a@company.com", emails.get(0).getValueAsString());
    Assert.assertEquals("a2@company.com", emails.get(1).getValueAsString());
    Assert.assertTrue(e1Record.has("/boss"));
    Assert.assertTrue(e1Record.get("/boss").getValueAsMap() != null);
    Assert.assertTrue(e1Record.has("/boss/name"));
    Assert.assertEquals("boss", e1Record.get("/boss/name").getValueAsString());
    Assert.assertTrue(e1Record.has("/boss/age"));
    Assert.assertEquals(60, e1Record.get("/boss/age").getValueAsInteger());
    Assert.assertTrue(e1Record.has("/boss/emails"));
    Assert.assertTrue(e1Record.get("/boss/emails").getValueAsList() != null);
    emails = e1Record.get("/boss/emails").getValueAsList();
    Assert.assertEquals(2, emails.size());
    Assert.assertEquals("boss@company.com", emails.get(0).getValueAsString());
    Assert.assertEquals("boss2@company.com", emails.get(1).getValueAsString());

  }

  @Test
  public void testProduceBinaryRecords() throws StageException, InterruptedException {

    CountDownLatch startLatch = new CountDownLatch(1);

    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new ProducerRunnable( TOPIC14, SINGLE_PARTITION, producer, startLatch, DataType.TEXT, null, -1,
      null, sdcKafkaTestUtil));

    KafkaConfigBean conf = new KafkaConfigBean();
    conf.metadataBrokerList = sdcKafkaTestUtil.getMetadataBrokerURI();
    conf.topic = TOPIC14;
    conf.consumerGroup = CONSUMER_GROUP;
    conf.zookeeperConnect = zkConnect;
    conf.maxBatchSize = 9;
    conf.maxWaitTime = 5000;
    conf.produceSingleRecordPerMessage = false;
    conf.dataFormat = DataFormat.BINARY;
    conf.dataFormatConfig.charset = "UTF-8";
    conf.dataFormatConfig.removeCtrlChars = false;
    conf.dataFormatConfig.binaryMaxObjectLen = 1000;

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class, createSource(conf))
      .addOutputLane("lane")
      .build();
    sourceRunner.runInit();

    startLatch.countDown();
    List<Record> records = new ArrayList<>();
    StageRunner.Output output = getOutputAndRecords(sourceRunner, 10, "lane", records);
    shutDownExecutorService(executorService);

    String newOffset = output.getNewOffset();
    Assert.assertNull(newOffset);
    Assert.assertEquals(10, records.size());

    for(int i = 0; i < records.size(); i++) {
      Assert.assertNotNull(records.get(i).get("/"));
      Assert.assertNotNull(records.get(i).get().getValueAsByteArray());
      Assert.assertTrue(Arrays.equals(sdcKafkaTestUtil.generateTestData(DataType.TEXT, null).getBytes(),
        records.get(i).get("/").getValueAsByteArray()));
    }

    sourceRunner.runDestroy();
  }

  @Test
  public void testProduceProtobufRecords() throws StageException, InterruptedException, IOException {

    Producer<String, byte[]> producer = createDefaultProducer();
    ByteArrayOutputStream bOut = new ByteArrayOutputStream();
    //send 10 protobuf messages to kafka topic
    for(int i = 0; i < 10; i++) {
      ProtobufTestUtil.getSingleProtobufData(bOut, i);
      producer.send(new KeyedMessage<>(TOPIC15, "0", bOut.toByteArray()));
      bOut.reset();
    }
    bOut.close();

    Map<String, String> kafkaConsumerConfigs = new HashMap<>();
    sdcKafkaTestUtil.setAutoOffsetReset(kafkaConsumerConfigs);

    KafkaConfigBean conf = new KafkaConfigBean();
    conf.metadataBrokerList = sdcKafkaTestUtil.getMetadataBrokerURI();
    conf.topic = TOPIC15;
    conf.consumerGroup = CONSUMER_GROUP;
    conf.zookeeperConnect = zkConnect;
    conf.maxBatchSize = 10;
    conf.maxWaitTime = 5000;
    conf.kafkaConsumerConfigs = kafkaConsumerConfigs;
    conf.produceSingleRecordPerMessage = false;
    conf.dataFormat = DataFormat.PROTOBUF;
    conf.dataFormatConfig.charset = "UTF-8";
    conf.dataFormatConfig.removeCtrlChars = false;
    conf.dataFormatConfig.protoDescriptorFile = protoDescFile.getPath();
    conf.dataFormatConfig.messageType = "util.Employee";
    conf.dataFormatConfig.isDelimited = true;

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class, createSource(conf))
      .addOutputLane("lane")
      .build();
    sourceRunner.runInit();

    List<Record> records = new ArrayList<>();
    StageRunner.Output output = getOutputAndRecords(sourceRunner, 10, "lane", records);

    String newOffset = output.getNewOffset();
    Assert.assertNull(newOffset);
    Assert.assertEquals(10, records.size());

    ProtobufTestUtil.compareProtoRecords(records, 0);

    sourceRunner.runDestroy();
  }

  @Test
  public void testMultipleProtobufSingleMessage() throws StageException, InterruptedException, IOException {

    Producer<String, byte[]> producer = createDefaultProducer();
    //send 10 protobuf messages to kafka topic
    producer.send(new KeyedMessage<>(TOPIC16, "0", ProtobufTestUtil.getProtoBufData()));

    Map<String, String> kafkaConsumerConfigs = new HashMap<>();
    sdcKafkaTestUtil.setAutoOffsetReset(kafkaConsumerConfigs);

    KafkaConfigBean conf = new KafkaConfigBean();
    conf.metadataBrokerList = sdcKafkaTestUtil.getMetadataBrokerURI();
    conf.topic = TOPIC16;
    conf.consumerGroup = CONSUMER_GROUP;
    conf.zookeeperConnect = zkConnect;
    conf.maxBatchSize = 10;
    conf.maxWaitTime = 10000;
    conf.kafkaConsumerConfigs = kafkaConsumerConfigs;
    conf.produceSingleRecordPerMessage = false;
    conf.dataFormat = DataFormat.PROTOBUF;
    conf.dataFormatConfig.charset = "UTF-8";
    conf.dataFormatConfig.removeCtrlChars = false;
    conf.dataFormatConfig.protoDescriptorFile = protoDescFile.getPath();
    conf.dataFormatConfig.messageType = "util.Employee";
    conf.dataFormatConfig.isDelimited = true;

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class, createSource(conf))
      .addOutputLane("lane")
      .build();
    sourceRunner.runInit();

    List<Record> records = new ArrayList<>();
    StageRunner.Output output = getOutputAndRecords(sourceRunner, 10, "lane", records);


    String newOffset = output.getNewOffset();
    Assert.assertNull(newOffset);
    Assert.assertEquals(10, records.size());

    ProtobufTestUtil.compareProtoRecords(records, 0);

    sourceRunner.runDestroy();
  }

  @Test
  public void testCollectdSignedMessage() throws StageException, InterruptedException, IOException {

    Producer<String, byte[]> producer = createDefaultProducer();
    producer.send(
        new KeyedMessage<>(
            TOPIC17,
            "0",
            UDPTestUtil.getUDPData(
              UDPConstants.COLLECTD,
              Resources.toByteArray(Resources.getResource(COLLECTD_SIGNED_BIN))
            )
        )
    );

    Map<String, String> kafkaConsumerConfigs = new HashMap<>();
    sdcKafkaTestUtil.setAutoOffsetReset(kafkaConsumerConfigs);

    KafkaConfigBean conf = new KafkaConfigBean();
    conf.metadataBrokerList = sdcKafkaTestUtil.getMetadataBrokerURI();
    conf.topic = TOPIC17;
    conf.consumerGroup = CONSUMER_GROUP;
    conf.zookeeperConnect = zkConnect;
    conf.maxBatchSize = 10;
    conf.maxWaitTime = 10000;
    conf.kafkaConsumerConfigs = kafkaConsumerConfigs;
    conf.produceSingleRecordPerMessage = false;
    conf.dataFormat = DataFormat.DATAGRAM;
    conf.dataFormatConfig.charset = "UTF-8";
    conf.dataFormatConfig.removeCtrlChars = false;
    conf.dataFormatConfig.datagramMode = DatagramMode.COLLECTD;
    conf.dataFormatConfig.convertTime = false;
    conf.dataFormatConfig.typesDbPath = null;
    conf.dataFormatConfig.excludeInterval = false;
    conf.dataFormatConfig.authFilePath = Resources.getResource(COLLECTD_AUTH_TXT).getPath();

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class, createSource(conf))
      .addOutputLane("lane")
      .build();
    sourceRunner.runInit();

    List<Record> records = new ArrayList<>();
    StageRunner.Output output = getOutputAndRecords(sourceRunner, 22, "lane", records);

    String newOffset = output.getNewOffset();
    Assert.assertNull(newOffset);
    Assert.assertEquals(22, records.size());

    Record record15 = records.get(15);
    UDPTestUtil.verifyCollectdRecord(UDPTestUtil.signedRecord15, record15);

    sourceRunner.runDestroy();
  }

  @Test
  public void testSyslogMessage() throws StageException, InterruptedException, IOException {

    Producer<String, byte[]> producer = createDefaultProducer();
    producer.send(new KeyedMessage<>(TOPIC18, "0", UDPTestUtil.getUDPData(UDPConstants.SYSLOG, SYSLOG.getBytes())));

    Map<String, String> kafkaConsumerConfigs = new HashMap<>();
    sdcKafkaTestUtil.setAutoOffsetReset(kafkaConsumerConfigs);

    KafkaConfigBean conf = new KafkaConfigBean();
    conf.metadataBrokerList = sdcKafkaTestUtil.getMetadataBrokerURI();
    conf.topic = TOPIC18;
    conf.consumerGroup = CONSUMER_GROUP;
    conf.zookeeperConnect = zkConnect;
    conf.maxBatchSize = 10;
    conf.maxWaitTime = 10000;
    conf.kafkaConsumerConfigs = kafkaConsumerConfigs;
    conf.produceSingleRecordPerMessage = false;
    conf.dataFormat = DataFormat.DATAGRAM;
    conf.dataFormatConfig.charset = "UTF-8";
    conf.dataFormatConfig.removeCtrlChars = false;
    conf.dataFormatConfig.datagramMode = DatagramMode.SYSLOG;

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class, createSource(conf))
      .addOutputLane("lane")
      .build();
    sourceRunner.runInit();

    List<Record> records = new ArrayList<>();
    StageRunner.Output output = getOutputAndRecords(sourceRunner, 1, "lane", records);

    String newOffset = output.getNewOffset();
    Assert.assertNull(newOffset);

    Assert.assertEquals(1, records.size());
    Assert.assertEquals(SYSLOG, records.get(0).get("/raw").getValueAsString());
    Assert.assertEquals("127.0.0.1:2000", records.get(0).get("/receiverAddr").getValueAsString());
    Assert.assertEquals("127.0.0.1:3000", records.get(0).get("/senderAddr").getValueAsString());
    Assert.assertEquals("mymachine", records.get(0).get("/host").getValueAsString());
    Assert.assertEquals(2, records.get(0).get("/severity").getValueAsInteger());
    Assert.assertEquals("34", records.get(0).get("/priority").getValueAsString());
    Assert.assertEquals(4, records.get(0).get("/facility").getValueAsInteger());
    Assert.assertEquals(timestamp, records.get(0).get("/timestamp").getValueAsDate());
    Assert.assertEquals(
      "su: 'su root' failed for lonvick on /dev/pts/8",
      records.get(0).get("/remaining").getValueAsString()
    );
    Assert.assertEquals(2000, records.get(0).get("/receiverPort").getValueAsInteger());
    Assert.assertEquals(3000, records.get(0).get("/senderPort").getValueAsInteger());

    sourceRunner.runDestroy();
  }

  @Test
  public void testNetflowMessage() throws StageException, InterruptedException, IOException {

    Producer<String, byte[]> producer = createDefaultProducer();
    producer.send(
        new KeyedMessage<>(
            TOPIC19,
            "0",
            UDPTestUtil.getUDPData(UDPConstants.NETFLOW, Resources.toByteArray(Resources.getResource(TEN_PACKETS)))
        )
    );

    Map<String, String> kafkaConsumerConfigs = new HashMap<>();
    sdcKafkaTestUtil.setAutoOffsetReset(kafkaConsumerConfigs);

    KafkaConfigBean conf = new KafkaConfigBean();
    conf.metadataBrokerList = sdcKafkaTestUtil.getMetadataBrokerURI();
    conf.topic = TOPIC19;
    conf.consumerGroup = CONSUMER_GROUP;
    conf.zookeeperConnect = zkConnect;
    conf.maxBatchSize = 10;
    conf.maxWaitTime = 10000;
    conf.kafkaConsumerConfigs = kafkaConsumerConfigs;
    conf.produceSingleRecordPerMessage = false;
    conf.dataFormat = DataFormat.DATAGRAM;
    conf.dataFormatConfig.charset = "UTF-8";
    conf.dataFormatConfig.removeCtrlChars = false;
    conf.dataFormatConfig.datagramMode = DatagramMode.NETFLOW;

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class, createSource(conf))
      .addOutputLane("lane")
      .build();
    sourceRunner.runInit();

    List<Record> records = new ArrayList<>();
    StageRunner.Output output = getOutputAndRecords(sourceRunner, 10, "lane", records);

    String newOffset = output.getNewOffset();
    Assert.assertNull(newOffset);

    UDPTestUtil.assertRecordsForTenPackets(records);

    sourceRunner.runDestroy();
  }

  private void shutDownExecutorService(ExecutorService executorService) throws InterruptedException {
    executorService.shutdownNow();
    if(!executorService.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
      //If it cant be stopped then throw exception
      throw new RuntimeException("Could not shutdown Executor service");
    }
  }

  private Producer<String, byte[]> createDefaultProducer() {
    Properties props = new Properties();
    props.put("metadata.broker.list", sdcKafkaTestUtil.getMetadataBrokerURI());
    props.put("serializer.class", "kafka.serializer.DefaultEncoder");
    props.put("batch.size", 1); // force messages to be sent immediately.
    props.put("key.serializer.class", "kafka.serializer.StringEncoder");
    props.put("request.required.acks", "-1");
    ProducerConfig config = new ProducerConfig(props);
    return new Producer<>(config);
  }

  private StageRunner.Output getOutputAndRecords(
    SourceRunner sourceRunner,
    int recordsToProduce,
    String lane,
    List<Record> records
  ) throws StageException {
    StageRunner.Output output = null;
    while(records.size() < recordsToProduce) {
      output = sourceRunner.runProduce(null, recordsToProduce - records.size());
      records.addAll(output.getRecords().get(lane));
    }
    return output;
  }

}
