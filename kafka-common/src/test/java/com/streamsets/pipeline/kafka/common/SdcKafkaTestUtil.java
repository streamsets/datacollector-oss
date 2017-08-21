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
package com.streamsets.pipeline.kafka.common;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.api.ext.RecordWriter;
import com.streamsets.pipeline.api.ext.json.Mode;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.sdk.RecordCreator;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaServer;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.IOUtils;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
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

public abstract class SdcKafkaTestUtil {

  private static final String MIME = "text/plain";
  private static final String TEST_STRING = "Hello World";

  public static final String DATE_LEVEL_CLASS =
    "2015-03-24 17:49:16,808 ERROR ExceptionToHttpErrorProvider - ";

  public static final String ERROR_MSG_WITH_STACK_TRACE = "REST API call error: LOG_PARSER_01 - Error parsing log line '2015-03-24 12:38:05,206 DEBUG LogConfigurator - Log starting, from configuration: /Users/harikiran/Documents/workspace/streamsets/dev/dist/target/streamsets-datacollector-1.0.0b2-SNAPSHOT/streamsets-datacollector-1.0.0b2-SNAPSHOT/etc/log4j.properties', reason : 'LOG_PARSER_03 - Log line 2015-03-24 12:38:05,206 DEBUG LogConfigurator - Log starting, from configuration: /Users/harikiran/Documents/workspace/streamsets/dev/dist/target/streamsets-datacollector-1.0.0b2-SNAPSHOT/streamsets-datacollector-1.0.0b2-SNAPSHOT/etc/log4j.properties does not confirm to Log4j Log Format'\n" +
    "com.streamsets.pipeline.lib.parser.DataParserException: LOG_PARSER_01 - Error parsing log line '2015-03-24 12:38:05,206 DEBUG LogConfigurator - Log starting, from configuration: /Users/harikiran/Documents/workspace/streamsets/dev/dist/target/streamsets-datacollector-1.0.0b2-SNAPSHOT/streamsets-datacollector-1.0.0b2-SNAPSHOT/etc/log4j.properties', reason : 'LOG_PARSER_03 - Log line 2015-03-24 12:38:05,206 DEBUG LogConfigurator - Log starting, from configuration: /Users/harikiran/Documents/workspace/streamsets/dev/dist/target/streamsets-datacollector-1.0.0b2-SNAPSHOT/streamsets-datacollector-1.0.0b2-SNAPSHOT/etc/log4j.properties does not confirm to Log4j Log Format'\n" +
    "\tat com.streamsets.pipeline.lib.parser.log.LogDataParser.parse(LogDataParser.java:69)\n" +
    "\tat com.streamsets.pipeline.stage.origin.spooldir.SpoolDirSource.produce(SpoolDirSource.java:566)\n" +
    "\tat com.streamsets.pipeline.stage.origin.spooldir.SpoolDirSource.produce(SpoolDirSource.java:535)\n" +
    "\tat com.streamsets.pipeline.configurablestage.DSource.produce(DSource.java:24)\n" +
    "\tat com.streamsets.pipeline.runner.StageRuntime.execute(StageRuntime.java:149)\n" +
    "\tat com.streamsets.pipeline.runner.StagePipe.process(StagePipe.java:106)\n" +
    "\tat com.streamsets.pipeline.runner.preview.PreviewPipelineRunner.run(PreviewPipelineRunner.java:85)\n" +
    "\tat com.streamsets.pipeline.runner.Pipeline.run(Pipeline.java:98)\n" +
    "\tat com.streamsets.pipeline.runner.preview.PreviewPipeline.run(PreviewPipeline.java:38)\n" +
    "\tat com.streamsets.pipeline.restapi.PreviewResource.previewWithOverride(PreviewResource.java:105)\n" +
    "\tat sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\n" +
    "\tat sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:57)\n" +
    "\tat sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\n" +
    "\tat java.lang.reflect.Method.invoke(Method.java:606)\n" +
    "\tat org.glassfish.jersey.server.model.internal.ResourceMethodInvocationHandlerFactory$1.invoke(ResourceMethodInvocationHandlerFactory.java:81)\n" +
    "\tat org.glassfish.jersey.server.model.internal.AbstractJavaResourceMethodDispatcher$1.run(AbstractJavaResourceMethodDispatcher.java:151)\n" +
    "\tat org.glassfish.jersey.server.model.internal.AbstractJavaResourceMethodDispatcher.invoke(AbstractJavaResourceMethodDispatcher.java:171)\n" +
    "\tat org.glassfish.jersey.server.model.internal.JavaResourceMethodDispatcherProvider$ResponseOutInvoker.doDispatch(JavaResourceMethodDispatcherProvider.java:152)\n" +
    "\tat org.glassfish.jersey.server.model.internal.AbstractJavaResourceMethodDispatcher.dispatch(AbstractJavaResourceMethodDispatcher.java:104)\n" +
    "\tat org.glassfish.jersey.server.model.ResourceMethodInvoker.invoke(ResourceMethodInvoker.java:384)\n" +
    "\tat org.glassfish.jersey.server.model.ResourceMethodInvoker.apply(ResourceMethodInvoker.java:342)\n" +
    "\tat org.glassfish.jersey.server.model.ResourceMethodInvoker.apply(ResourceMethodInvoker.java:101)\n" +
    "\tat org.glassfish.jersey.server.ServerRuntime$1.run(ServerRuntime.java:271)\n" +
    "\tat org.glassfish.jersey.internal.Errors$1.call(Errors.java:271)\n" +
    "\tat org.glassfish.jersey.internal.Errors$1.call(Errors.java:267)\n" +
    "\tat org.glassfish.jersey.internal.Errors.process(Errors.java:315)\n" +
    "\tat org.glassfish.jersey.internal.Errors.process(Errors.java:297)\n" +
    "\tat org.glassfish.jersey.internal.Errors.process(Errors.java:267)\n" +
    "\tat org.glassfish.jersey.process.internal.RequestScope.runInScope(RequestScope.java:297)\n" +
    "\tat org.glassfish.jersey.server.ServerRuntime.process(ServerRuntime.java:254)\n" +
    "\tat org.glassfish.jersey.server.ApplicationHandler.handle(ApplicationHandler.java:1030)\n" +
    "\tat org.glassfish.jersey.servlet.WebComponent.service(WebComponent.java:373)\n" +
    "\tat org.glassfish.jersey.servlet.ServletContainer.service(ServletContainer.java:381)\n" +
    "\tat org.glassfish.jersey.servlet.ServletContainer.service(ServletContainer.java:344)\n" +
    "\tat org.glassfish.jersey.servlet.ServletContainer.service(ServletContainer.java:221)\n" +
    "\tat org.eclipse.jetty.servlet.ServletHolder.handle(ServletHolder.java:769)\n" +
    "\tat org.eclipse.jetty.servlet.ServletHandler$CachedChain.doFilter(ServletHandler.java:1667)\n" +
    "\tat com.streamsets.pipeline.http.LocaleDetectorFilter.doFilter(LocaleDetectorFilter.java:29)\n" +
    "\tat org.eclipse.jetty.servlet.ServletHandler$CachedChain.doFilter(ServletHandler.java:1650)\n" +
    "\tat org.eclipse.jetty.servlets.UserAgentFilter.doFilter(UserAgentFilter.java:83)\n" +
    "\tat org.eclipse.jetty.servlets.GzipFilter.doFilter(GzipFilter.java:300)\n" +
    "\tat org.eclipse.jetty.servlet.ServletHandler$CachedChain.doFilter(ServletHandler.java:1650)\n" +
    "\tat org.eclipse.jetty.servlet.ServletHandler.doHandle(ServletHandler.java:583)\n" +
    "\tat org.eclipse.jetty.server.handler.ScopedHandler.handle(ScopedHandler.java:143)\n" +
    "\tat org.eclipse.jetty.security.SecurityHandler.handle(SecurityHandler.java:542)\n" +
    "\tat org.eclipse.jetty.server.session.SessionHandler.doHandle(SessionHandler.java:223)\n" +
    "\tat org.eclipse.jetty.server.handler.ContextHandler.doHandle(ContextHandler.java:1125)\n" +
    "\tat org.eclipse.jetty.servlet.ServletHandler.doScope(ServletHandler.java:515)\n" +
    "\tat org.eclipse.jetty.server.session.SessionHandler.doScope(SessionHandler.java:185)\n" +
    "\tat org.eclipse.jetty.server.handler.ContextHandler.doScope(ContextHandler.java:1059)\n" +
    "\tat org.eclipse.jetty.server.handler.ScopedHandler.handle(ScopedHandler.java:141)\n" +
    "\tat org.eclipse.jetty.server.handler.HandlerWrapper.handle(HandlerWrapper.java:97)\n" +
    "\tat org.eclipse.jetty.rewrite.handler.RewriteHandler.handle(RewriteHandler.java:309)\n" +
    "\tat org.eclipse.jetty.server.handler.HandlerCollection.handle(HandlerCollection.java:110)\n" +
    "\tat org.eclipse.jetty.server.handler.HandlerWrapper.handle(HandlerWrapper.java:97)\n" +
    "\tat org.eclipse.jetty.server.Server.handle(Server.java:497)\n" +
    "\tat org.eclipse.jetty.server.HttpChannel.handle(HttpChannel.java:311)\n" +
    "\tat org.eclipse.jetty.server.HttpConnection.onFillable(HttpConnection.java:248)\n" +
    "\tat org.eclipse.jetty.io.AbstractConnection$2.run(AbstractConnection.java:540)\n" +
    "\tat org.eclipse.jetty.util.thread.QueuedThreadPool.runJob(QueuedThreadPool.java:610)\n" +
    "\tat org.eclipse.jetty.util.thread.QueuedThreadPool$3.run(QueuedThreadPool.java:539)\n" +
    "\tat java.lang.Thread.run(Thread.java:745)\n" +
    "Caused by: com.streamsets.pipeline.lib.parser.DataParserException: LOG_PARSER_03 - Log line 2015-03-24 12:38:05,206 DEBUG LogConfigurator - Log starting, from configuration: /Users/harikiran/Documents/workspace/streamsets/dev/dist/target/streamsets-datacollector-1.0.0b2-SNAPSHOT/streamsets-datacollector-1.0.0b2-SNAPSHOT/etc/log4j.properties does not confirm to Log4j Log Format\n" +
    "\tat com.streamsets.pipeline.lib.parser.log.Log4jParser.handleNoMatch(Log4jParser.java:30)\n" +
    "\tat com.streamsets.pipeline.lib.parser.log.GrokParser.parseLogLine(GrokParser.java:51)\n" +
    "\tat com.streamsets.pipeline.lib.parser.log.LogDataParser.parse(LogDataParser.java:67)\n" +
    "\t... 61 more";

  public static final String LOG_LINE_WITH_STACK_TRACE = DATE_LEVEL_CLASS + ERROR_MSG_WITH_STACK_TRACE;
  public static final String LOG_LINE = "2015-03-20 15:53:31,161 DEBUG PipelineConfigurationValidator - " +
    "Pipeline 'test:preview' validation. valid=true, canPreview=true, issuesCount=0";

  public static final String TEST_STRING_255 = "StreamSets was founded in June 2014 by business and engineering " +
    "leaders in the data integration space with a history of bringing successful products to market. We’re a " +
    "team that is laser-focused on solving hard problems so our customers don’t have to.";

  public abstract String getMetadataBrokerURI();

  public abstract String getZkConnect();

  public abstract List<KafkaServer> getKafkaServers();

  public abstract void startZookeeper();

  public abstract void startKafkaBrokers(int numberOfBrokers) throws IOException;

  public abstract void createTopic(String topic, int partitions, int replicationFactor);

  public abstract void shutdown();

  public abstract void setAutoOffsetReset(Map<String, String> kafkaConsumerConfigs);

  public List<KafkaStream<byte[], byte[]>> createKafkaStream(
      String zookeeperConnectString,
      String topic,
      int partitions
  ) {
    //create consumer
    Properties consumerProps = new Properties();
    consumerProps.put("zookeeper.connect", zookeeperConnectString);
    consumerProps.put("group.id", "testClient");
    consumerProps.put("zookeeper.session.timeout.ms", "6000");
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

  public Producer<String, String> createProducer(String metadataBrokerURI, boolean setPartitioner) {
    Properties props = new Properties();
    props.put("metadata.broker.list", metadataBrokerURI);
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    if (setPartitioner) {
      props.put("partitioner.class", "com.streamsets.pipeline.kafka.impl.ExpressionPartitioner");
    }
    props.put("request.required.acks", "-1");
    ProducerConfig config = new ProducerConfig(props);
    Producer<String, String> producer = new Producer<>(config);
    return producer;
  }

  public List<Record> createEmptyLogRecords() {
    return Collections.emptyList();
  }

  public List<Record> createStringRecords() {
    List<Record> records = new ArrayList<>(9);
    for (int i = 0; i < 9; i++) {
      Record r = RecordCreator.create("s", "s:1", (TEST_STRING + i).getBytes(), MIME);
      r.set(Field.create((TEST_STRING + i)));
      records.add(r);
    }
    return records;
  }

  public List<Record> createIdenticalStringRecords() {
    List<Record> records = new ArrayList<>(9);
    int id = 0;
    for (int i = 0; i < 9; i++) {
      Record r = RecordCreator.create("s", "s:1", (TEST_STRING + id).getBytes(), MIME);
      r.set(Field.create((TEST_STRING + id)));
      records.add(r);
    }
    return records;
  }

  public List<Record> createBinaryRecords() {
    List<Record> records = new ArrayList<>(9);
    for (int i = 0; i < 9; i++) {
      Record record = RecordCreator.create();
      Map<String, Field> map = new HashMap<>();
      map.put("data", Field.create((TEST_STRING_255 + i).getBytes()));
      record.set(Field.create(map));
      records.add(record);
    }
    return records;
  }

  public List<Record> createIntegerRecords() {
    List<Record> records = new ArrayList<>(9);
    for (int i = 0; i < 9; i++) {
      Record r = RecordCreator.create("s", "s:1", (TEST_STRING + i).getBytes(), MIME);
      r.set(Field.create(i));
      records.add(r);
    }
    return records;
  }

  public List<KeyedMessage<String, String>> produceStringMessages(String topic, String partition, int number) {
    List<KeyedMessage<String, String>> messages = new ArrayList<>();
    for (int i = 0; i < number; i++) {
      messages.add(new KeyedMessage<>(topic, partition, (TEST_STRING + i)));
    }
    return messages;
  }

  public List<KeyedMessage<String, String>> produceStringMessages(String topic, int partitions, int number) {
    List<KeyedMessage<String, String>> messages = new ArrayList<>();
    int partition = 0;
    for (int i = 0; i < number; i++) {
      partition = (partition + 1) % partitions;
      messages.add(new KeyedMessage<>(topic, String.valueOf(partition), (TEST_STRING + i)));
    }
    return messages;
  }

  public List<Record> produce20Records() throws IOException {
    List<Record> list = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      Record record = RecordCreator.create();
      Map<String, Field> map = new HashMap<>();
      map.put("name", Field.create("NAME" + i));
      map.put("lastStatusChange", Field.create(i));
      record.set(Field.create(map));
      list.add(record);
    }
    return list;
  }

  public List<KeyedMessage<String, String>> produceJsonMessages(String topic, String partition) throws IOException {
    ContextExtensions ctx = (ContextExtensions) ContextInfoCreator.createTargetContext("", false, OnRecordError.TO_ERROR);
    List<KeyedMessage<String, String>> messages = new ArrayList<>();
    for (Record record : produce20Records()) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      RecordWriter rw = ctx.createRecordWriter(baos);
      rw.write(record);
      rw.close();
      messages.add(new KeyedMessage<>(topic, partition, baos.toString()));
    }
    return messages;
  }

  public List<KeyedMessage<String, String>> produceXmlMessages(String topic, String partition) throws IOException {

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

  public List<KeyedMessage<String, String>> produceCsvMessages(String topic, String partition,
                                                                      CSVFormat csvFormat, File csvFile) throws IOException {
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

  public List<Record> createJsonRecords() throws IOException {
    return produce20Records();
  }

  public List<Record> createJsonRecordsWithTopicPartitionField(List<String> topics, int partitions) throws IOException {
    int size = topics.size();
    List<Record> list = new ArrayList<>();
    for(String topic : topics) {
      for (int i = 0; i < partitions; i++) {
        Record record = RecordCreator.create();
        Map<String, Field> map = new HashMap<>();
        map.put("name", Field.create("NAME" + i));
        map.put("lastStatusChange", Field.create(i));
        int j = i % size;
        map.put("topic", Field.create(topic));
        map.put("partition", Field.create(i));
        record.set(Field.create(map));
        list.add(record);
      }
    }
    return list;
  }

  public List<Record> createJsonRecordsWithTopicField(List<String> topics) throws IOException {
    int size = topics.size();
    List<Record> list = new ArrayList<>();
    for (int i = 0; i < size*3; i++) {
      Record record = RecordCreator.create();
      Map<String, Field> map = new HashMap<>();
      map.put("name", Field.create("NAME" + i));
      map.put("lastStatusChange", Field.create(i));
      map.put("partition", Field.create(i%2));
      int j = i % size;
      map.put("topic", Field.create(topics.get(j)));
      record.set(Field.create(map));
      list.add(record);
    }
    return list;
  }

  public List<Record> createCsvRecords(File csvFile) throws Exception {
    List<Record> records = new ArrayList<>();
    String line;
    BufferedReader bufferedReader = new BufferedReader(new FileReader(csvFile));
    while ((line = bufferedReader.readLine()) != null) {
      String columns[] = line.split(",");
      List<Field> list = new ArrayList<>();
      for (String column : columns) {
        Map<String, Field> map = new LinkedHashMap<>();
        map.put("value", Field.create(column));
        list.add(Field.create(map));
      }
      Record record = RecordCreator.create("s", "s:1", null, null);
      record.set(Field.create(list));
      records.add(record);
    }
    return records;
  }

  public String generateTestData(DataType dataType, Mode jsonMode) {
    switch (dataType) {
      case TEXT:
        return "Hello Kafka";
      case JSON:
        return createJson(jsonMode);
      case CSV:
        return "2010,NLDS1,PHI,NL,CIN,NL,3,0,0";
      case XML:
        return "<book id=\"bk104\">\n" +
          "<author>Corets, Eva</author>\n" +
          "<author>CoretsXX, EvaXX</author>\n" +
          "<title>Oberon's Legacy</title>\n" +
          "<genre>Fantasy</genre>\n" +
          "<price>5.95</price>\n" +
          "<publish_date>2001-03-10</publish_date>\n" +
          "<description>Description</description>\n" +
          "</book>";
      case LOG:
        return LOG_LINE;
      case LOG_STACK_TRACE:
        return LOG_LINE_WITH_STACK_TRACE;
    }
    throw new IllegalArgumentException("Unsupported data type requested");
  }



  private static String createJson(Mode jsonMode) {
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
        return "[\n" +
          "{\"menu\": {\n" +
          "  \"id\": \"1\",\n" +
          "  \"value\": \"File\",\n" +
          "  \"popup\": {\n" +
          "    \"menuitem\": [\n" +
          "      {\"value\": \"New\", \"onclick\": \"CreateNewDoc()\"},\n" +
          "      {\"value\": \"Open\", \"onclick\": \"OpenDoc()\"},\n" +
          "      {\"value\": \"Close\", \"onclick\": \"CloseDoc()\"}\n" +
          "    ]\n" +
          "  }\n" +
          "}}," +
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
          "}}," +
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
          "}}," +
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
          "}}" +
          "]";

    }
    throw new IllegalArgumentException("Unsupported data type requested");
  }

  public abstract Map<String,String> setMaxAcks(Map<String, String> producerConfigs);
}
