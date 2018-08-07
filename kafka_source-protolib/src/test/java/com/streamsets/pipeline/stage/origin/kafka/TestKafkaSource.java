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

import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.kafka.common.SdcKafkaTestUtil;
import com.streamsets.pipeline.kafka.common.SdcKafkaTestUtilFactory;
import com.streamsets.pipeline.lib.util.ProtobufTestUtil;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.testing.SingleForkNoReuseTest;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.utils.TestUtils;
import org.apache.commons.io.FileUtils;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Ignore
@Category(SingleForkNoReuseTest.class)
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
  private static final String TOPIC12 = "TestKafkaSource12";
  private static final String TOPIC13 = "TestKafkaSource13";
  private static final String TOPIC14 = "TestKafkaSource14";
  private static final String TOPIC15 = "TestKafkaSource15";
  private static final String TOPIC16 = "TestKafkaSource16";
  private static final String TOPIC17 = "TestKafkaSource17";
  private static final String TOPIC18 = "TestKafkaSource18";
  private static final String TOPIC19 = "TestKafkaSource19";
  private static final String CONSUMER_GROUP = "SDC";

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

    for (int i = 1; i <= 16; i++) {
      TestUtils.waitUntilMetadataIsPropagated(
          scala.collection.JavaConversions.asScalaBuffer(sdcKafkaTestUtil.getKafkaServers()),
          "TestKafkaSource" + String.valueOf(i),
          0,
          5000
      );
      // For now, the only topic that needs more than one partition is topic 2. Eventually we should put all these into
      // a class and make sure we create the topics based on a list of Topic/Partition info, rather than this.
      if (i == 2) {
        for (int j = 0; j < MULTIPLE_PARTITIONS; j++) {
          TestUtils.waitUntilMetadataIsPropagated(
              scala.collection.JavaConversions.asScalaBuffer(sdcKafkaTestUtil.getKafkaServers()),
              "TestKafkaSource" + String.valueOf(i),
              j,
              5000
          );
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
  public void testProduceProtobufRecords() throws StageException, InterruptedException, IOException {

    Producer<String, byte[]> producer = createDefaultProducer();
    ByteArrayOutputStream bOut = new ByteArrayOutputStream();
    //send 10 protobuf messages to kafka topic
    for (int i = 0; i < 10; i++) {
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

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class, createSource(conf)).addOutputLane("lane")
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

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class, createSource(conf)).addOutputLane("lane")
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
      SourceRunner sourceRunner, int recordsToProduce, String lane, List<Record> records
  ) throws StageException {
    StageRunner.Output output = null;
    while (records.size() < recordsToProduce) {
      output = sourceRunner.runProduce(null, recordsToProduce - records.size());
      records.addAll(output.getRecords().get(lane));
    }
    return output;
  }

}
