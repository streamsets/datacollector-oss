/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.json.StreamingJsonParser;
import com.streamsets.pipeline.lib.util.JsonUtil;
import com.streamsets.pipeline.sdk.RecordCreator;
import kafka.admin.AdminUtils;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaServer;
import kafka.utils.TestUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.IOUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaTestUtil {

  private static final String MIME = "text/plain";
  private static final String TEST_STRING = "Hello World";

  public static List<KafkaStream<byte[], byte[]>> createKafkaStream(String zookeeperConnectString, String topic, int partitions) {
    //create consumer
    Properties consumerProps = new Properties();
    consumerProps.put("zookeeper.connect", zookeeperConnectString);
    consumerProps.put("group.id", "testClient");
    consumerProps.put("zookeeper.session.timeout.ms", "400");
    consumerProps.put("zookeeper.sync.time.ms", "200");
    consumerProps.put("auto.commit.interval.ms", "1000");
    consumerProps.put("consumer.timeout.ms", "500");
    ConsumerConfig consumerConfig = new ConsumerConfig(consumerProps);
    ConsumerConnector consumer = Consumer.createJavaConsumerConnector(consumerConfig);
    Map<String, Integer> topicCountMap = new HashMap<>();
    topicCountMap.put(topic, partitions);
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
    return consumerMap.get(topic);

  }

  public static Producer<String, String> createProducer(String host, int port) {
    Properties props = new Properties();
    props.put("metadata.broker.list", host + ":" + port);
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("partitioner.class", "com.streamsets.pipeline.lib.kafka.ExpressionPartitioner");
    props.put("request.required.acks", "1");
    ProducerConfig config = new ProducerConfig(props);

    Producer<String, String> producer = new Producer<>(config);
    return producer;
  }

  public static List<Record> createEmptyLogRecords() {
    return Collections.emptyList();
  }

  public static List<Record> createStringRecords() {
    List<Record> records = new ArrayList<>(9);
    for (int i = 0; i < 9; i++) {
      Record r = RecordCreator.create("s", "s:1", (TEST_STRING + i).getBytes(), MIME);
      r.set(Field.create((TEST_STRING + i)));
      records.add(r);
    }
    return records;
  }

  public static List<Record> createIntegerRecords() {
    List<Record> records = new ArrayList<>(9);
    for (int i = 0; i < 9; i++) {
      Record r = RecordCreator.create("s", "s:1", (TEST_STRING + i).getBytes(), MIME);
      r.set(Field.create(i));
      records.add(r);
    }
    return records;
  }

  public static List<KeyedMessage<String, String>> produceStringMessages(String topic, String partition) {
    List<KeyedMessage<String, String>> messages = new ArrayList<>();
    for (int i = 0; i < 9; i++) {
      messages.add(new KeyedMessage<>(topic, partition, (TEST_STRING + i)));
    }
    return messages;
  }

  public static List<KeyedMessage<String, String>> produceJsonMessages(String topic, String partition) throws IOException {
    TypeReference<List<HashMap<String, Object>>> typeRef
      = new TypeReference<List<HashMap<String, Object>>>() {
    };
    List<Map<String, String>> listOfJson = new ObjectMapper().readValue(KafkaTestUtil.class.getClassLoader()
      .getResourceAsStream("testKafkaTarget.json"), typeRef);

    List<KeyedMessage<String, String>> messages = new ArrayList<>();
    for (Map<String, String> map : listOfJson) {
      messages.add(new KeyedMessage<>(topic, partition, new ObjectMapper().writeValueAsString(map)));
    }
    return messages;
  }

  public static List<KeyedMessage<String, String>> produceXmlMessages(String topic, String partition) throws IOException {

    List<String> stringList = IOUtils.readLines(KafkaTestUtil.class.getClassLoader().getResourceAsStream("testKafkaTarget.xml"));
    StringBuilder sb = new StringBuilder();
    for (String s : stringList) {
      sb.append(s);
    }
    String xmlString = sb.toString();

    List<KeyedMessage<String, String>> messages = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      messages.add(new KeyedMessage<>(topic, partition, xmlString));
    }
    return messages;
  }

  public static List<KeyedMessage<String, String>> produceCsvMessages(String topic, String partition,
                                                                      CSVFormat csvFormat) throws IOException {
    List<KeyedMessage<String, String>> messages = new ArrayList<>();
    String line;
    BufferedReader bufferedReader = new BufferedReader(new FileReader(KafkaTestUtil.class.getClassLoader()
      .getResource("testKafkaTarget.csv").getFile()));
    while ((line = bufferedReader.readLine()) != null) {
      String[] strings = line.split(",");
      StringWriter stringWriter = new StringWriter();
      CSVPrinter csvPrinter = new CSVPrinter(stringWriter, csvFormat);
      csvPrinter.printRecord(strings);
      csvPrinter.flush();
      csvPrinter.close();
      messages.add(new KeyedMessage<>(topic, partition, stringWriter.toString()));
    }
    return messages;
  }

  public static List<Record> createJsonRecords() throws IOException {

    TypeReference<List<HashMap<String, Object>>> typeRef
      = new TypeReference<List<HashMap<String, Object>>>() {
    };
    List<Map<String, String>> o = new ObjectMapper().readValue(KafkaTestUtil.class.getClassLoader()
      .getResourceAsStream("testKafkaTarget.json"), typeRef);
    List<Record> records = new ArrayList<>();
    for (Map<String, String> map : o) {
      Record r = RecordCreator.create("s", "s:1", null, null);
      r.set(JsonUtil.jsonToField(map));
      records.add(r);
    }
    return records;
  }

  public static List<Record> createCsvRecords() throws IOException {
    List<Record> records = new ArrayList<>();
    String line;
    BufferedReader bufferedReader = new BufferedReader(new FileReader(KafkaTestUtil.class.getClassLoader()
      .getResource("testKafkaTarget.csv").getFile()));
    while ((line = bufferedReader.readLine()) != null) {
      String columns[] = line.split(",");
      Map<String, Field> map = new LinkedHashMap<>();
      List<Field> values = new ArrayList<>(columns.length);
      for (String column : columns) {
        values.add(Field.create(column));
      }
      map.put("values", Field.create(values));
      Record record = RecordCreator.create("s", "s:1", null, null);
      record.set(Field.create(map));
      records.add(record);
    }
    return records;
  }

  public static void createTopic(ZkClient zkClient, List<KafkaServer> kafkaServers, String topic, int partitions,
                                 int replicationFactor, int timeout) {
    AdminUtils.createTopic(zkClient, topic, partitions, replicationFactor, new Properties());
    TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asBuffer(kafkaServers), topic, 0, timeout);
  }

  /*public static String generateTestData(DataType dataType) {
    return generateTestData(dataType, StreamingJsonParser.Mode.MULTIPLE_OBJECTS);
  }*/

  public static String generateTestData(DataType dataType, StreamingJsonParser.Mode jsonMode) {
    switch (dataType) {
      case LOG:
        return "Hello Kafka";
      case JSON:
        return createJson(jsonMode);
      case CSV:
        return "2010,NLDS1,PHI,NL,CIN,NL,3,0,0";
      case XML:
        return "<book id=\"bk104\">\n" +
          "<author>Corets, Eva</author>\n" +
          "<title>Oberon's Legacy</title>\n" +
          "<genre>Fantasy</genre>\n" +
          "<price>5.95</price>\n" +
          "<publish_date>2001-03-10</publish_date>\n" +
          "<description>Description</description>\n" +
          "</book>";

    }
    throw new IllegalArgumentException("Unsupported data type requested");
  }

  private static String createJson(StreamingJsonParser.Mode jsonMode) {
    switch (jsonMode) {
      case MULTIPLE_OBJECTS:
        return "{\"menu\": {\n" +
          "  \"id\": \"1\",\n" +
          "  \"value\": \"File\",\n" +
          "  \"popup\": {\n" +
          "    \"menuitem\": [\n" +
          "      {\"value\": \"New\", \"onclick\": \"CreateNewDoc()\"},\n" +
          "      {\"value\": \"Open\", \"onclick\": \"OpenDoc()\"},\n" +
          "      {\"value\": \"Close\", \"onclick\": \"CloseDoc()\"}\n" +
          "    ]\n" +
          "  }\n" +
          "}}\n" +
          "{\"menu\": {\n" +
          "  \"id\": \"2\",\n" +
          "  \"value\": \"File\",\n" +
          "  \"popup\": {\n" +
          "    \"menuitem\": [\n" +
          "      {\"value\": \"New\", \"onclick\": \"CreateNewDoc()\"},\n" +
          "      {\"value\": \"Open\", \"onclick\": \"OpenDoc()\"},\n" +
          "      {\"value\": \"Close\", \"onclick\": \"CloseDoc()\"}\n" +
          "    ]\n" +
          "  }\n" +
          "}}\n" +
          "{\"menu\": {\n" +
          "  \"id\": \"3\",\n" +
          "  \"value\": \"File\",\n" +
          "  \"popup\": {\n" +
          "    \"menuitem\": [\n" +
          "      {\"value\": \"New\", \"onclick\": \"CreateNewDoc()\"},\n" +
          "      {\"value\": \"Open\", \"onclick\": \"OpenDoc()\"},\n" +
          "      {\"value\": \"Close\", \"onclick\": \"CloseDoc()\"}\n" +
          "    ]\n" +
          "  }\n" +
          "}}\n" +
          "{\"menu\": {\n" +
          "  \"id\": \"4\",\n" +
          "  \"value\": \"File\",\n" +
          "  \"popup\": {\n" +
          "    \"menuitem\": [\n" +
          "      {\"value\": \"New\", \"onclick\": \"CreateNewDoc()\"},\n" +
          "      {\"value\": \"Open\", \"onclick\": \"OpenDoc()\"},\n" +
          "      {\"value\": \"Close\", \"onclick\": \"CloseDoc()\"}\n" +
          "    ]\n" +
          "  }\n" +
          "}}\n";

      case ARRAY_OBJECTS:
        return "[{“menu\": {\n" +
          "  \"id\": \"1\",\n" +
          "  \"value\": \"File\",\n" +
          "  \"popup\": {\n" +
          "    \"menuitem\": [\n" +
          "      {\"value\": \"New\", \"onclick\": \"CreateNewDoc()\"},\n" +
          "      {\"value\": \"Open\", \"onclick\": \"OpenDoc()\"},\n" +
          "      {\"value\": \"Close\", \"onclick\": \"CloseDoc()\"}\n" +
          "    ]\n" +
          "  }\n" +
          "}},\n" +
          "{\"menu\": {\n" +
          "  \"id\": \"2\",\n" +
          "  \"value\": \"File\",\n" +
          "  \"popup\": {\n" +
          "    \"menuitem\": [\n" +
          "      {\"value\": \"New\", \"onclick\": \"CreateNewDoc()\"},\n" +
          "      {\"value\": \"Open\", \"onclick\": \"OpenDoc()\"},\n" +
          "      {\"value\": \"Close\", \"onclick\": \"CloseDoc()\"}\n" +
          "    ]\n" +
          "  }\n" +
          "}},\n" +
          "{\"menu\": {\n" +
          "  \"id\": \"3\",\n" +
          "  \"value\": \"File\",\n" +
          "  \"popup\": {\n" +
          "    \"menuitem\": [\n" +
          "      {\"value\": \"New\", \"onclick\": \"CreateNewDoc()\"},\n" +
          "      {\"value\": \"Open\", \"onclick\": \"OpenDoc()\"},\n" +
          "      {\"value\": \"Close\", \"onclick\": \"CloseDoc()\"}\n" +
          "    ]\n" +
          "  }\n" +
          "}}\n" +
          "{\"menu\": {\n" +
          "  \"id\": \"4\",\n" +
          "  \"value\": \"File\",\n" +
          "  \"popup\": {\n" +
          "    \"menuitem\": [\n" +
          "      {\"value\": \"New\", \"onclick\": \"CreateNewDoc()\"},\n" +
          "      {\"value\": \"Open\", \"onclick\": \"OpenDoc()\"},\n" +
          "      {\"value\": \"Close\", \"onclick\": \"CloseDoc()\"}\n" +
          "    ]\n" +
          "  }\n" +
          "}}]";

    }
    throw new IllegalArgumentException("Unsupported data type requested");
  }

}
