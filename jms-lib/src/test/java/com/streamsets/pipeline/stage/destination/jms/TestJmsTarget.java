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
package com.streamsets.pipeline.stage.destination.jms;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.AvroCompression;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.DestinationAvroSchemaSource;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.lib.jms.config.DestinationType;
import com.streamsets.pipeline.lib.jms.config.InitialContextFactory;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.stage.common.CredentialsConfig;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.security.AuthenticationUser;
import org.apache.activemq.security.SimpleAuthenticationPlugin;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TestJmsTarget {
  private static final Logger LOG = LoggerFactory.getLogger(TestJmsTarget.class);

  private final static String INITIAL_CONTEXT_FACTORY = "org.apache.activemq.jndi.ActiveMQInitialContextFactory";
  private final static String CONNECTION_FACTORY = "ConnectionFactory";
  private final static String BROKER_BIND_URL = "tcp://localhost:61516";
  private final static  String DESTINATION_NAME = "test";
  private final static  String USERNAME = "user";
  private final static String PASSWORD = "pass";
  private final static String RECORD_SEPERATOR = "\n";
  // specific for dynamic queues on ActiveMq
  public static final String JNDI_PREFIX = "dynamicQueues/";
  private File baseDir;
  private File tmpDir;
  private File dataDir;
  private File passwordFile;

  private Connection connection;
  private BrokerService broker;
  private CredentialsConfig credentialsConfig;
  private DataGeneratorFormatConfig dataFormatConfig;
  private JmsTargetConfig jmsTargetConfig;
  private DataFormat dataFormat;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() throws Exception {
    baseDir = Files.createTempDir();
    tmpDir = new File(baseDir, "tmp");
    dataDir = new File(baseDir, "data");
    Assert.assertTrue(tmpDir.mkdir());
    passwordFile = new File(baseDir, "password");
    Files.write(PASSWORD.getBytes(StandardCharsets.UTF_8), passwordFile);

    broker = new BrokerService();

    broker.addConnector(BROKER_BIND_URL);
    broker.setTmpDataDirectory(tmpDir);
    broker.setDataDirectoryFile(dataDir);
    List<AuthenticationUser> users = Lists.newArrayList();
    users.add(new AuthenticationUser(USERNAME, PASSWORD, ""));
    SimpleAuthenticationPlugin authentication = new SimpleAuthenticationPlugin(users);
    broker.setPlugins(new BrokerPlugin[]{authentication});
    broker.start();

    credentialsConfig = new CredentialsConfig();
    dataFormatConfig = new DataGeneratorFormatConfig();
    jmsTargetConfig = new JmsTargetConfig();
    credentialsConfig.useCredentials = true;
    credentialsConfig.username = () -> USERNAME;
    credentialsConfig.password = () -> PASSWORD;
    jmsTargetConfig.destinationName = JNDI_PREFIX + DESTINATION_NAME;
    jmsTargetConfig.initialContextFactory = INITIAL_CONTEXT_FACTORY;
    jmsTargetConfig.connectionFactory = CONNECTION_FACTORY;
    jmsTargetConfig.providerURL = BROKER_BIND_URL;
    // Create a connection and start
    ConnectionFactory factory = new ActiveMQConnectionFactory(USERNAME,
        PASSWORD, BROKER_BIND_URL);
    connection = factory.createConnection();
    connection.start();
  }

  @After
  public void tearDown() throws Exception {
    if (connection != null){
      connection.close();
    }

    if (broker != null) {
      broker.stop();
    }
  }

  private List<String> getQueue() throws Exception {
    List<String> rows = new ArrayList<>();

    Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
    Destination destination = session.createQueue(DESTINATION_NAME);
    MessageConsumer consumer = session.createConsumer(destination);

    Message temp;
    while((temp = consumer.receive(100)) != null) {
      if(temp instanceof BytesMessage) {
        BytesMessage message = (BytesMessage) temp;
        byte[] payload = new byte[(int) message.getBodyLength()];
        message.readBytes(payload);
        rows.add(new String(payload) + RECORD_SEPERATOR);
      } else if(temp instanceof TextMessage) {
        rows.add(((TextMessage) temp).getText());
      } else {
        throw new Exception("Unexpected message type");
      }
    }

    return rows;
  }

  private TargetRunner createRunner() {
    JmsTarget destination = new JmsTarget(
        credentialsConfig,
        jmsTargetConfig,
        dataFormat,
        dataFormatConfig,
        new JmsMessageProducerFactoryImpl(),
        new InitialContextFactory()
    );
    return new TargetRunner.Builder(JmsTarget.class, destination).build();
  }

  private void runInit(String expectedError) {
    dataFormat = DataFormat.BINARY;
    TargetRunner runner = createRunner();
    try {
      runner.runInit();
      Assert.fail();
    } catch (StageException ex) {
      Assert.assertTrue(Utils.format("Expected {} got: {}", expectedError, ex), ex.getMessage().
          contains(expectedError + " "));
    }
  }

  @Test
  public void testInvalidInitialContext() throws Exception {
    jmsTargetConfig.initialContextFactory = "invalid";
    runInit("JMS_00");
  }

  @Test
  public void testInvalidConnectionFactory() throws Exception {
    jmsTargetConfig.connectionFactory = "invalid";
    runInit("JMS_01");
  }

  @Test
  public void testInvalidDestination() throws Exception {
    jmsTargetConfig.destinationName = "invalid";
    runInit("JMS_05");
  }

  @Test
  public void testInvalidCreds() throws Exception {
    credentialsConfig.username = () -> "invalid";
    runInit("JMS_04");
  }

  @Test
  public void testNoBroker() throws Exception {
    broker.stop();
    runInit("JMS_03");
  }

  @Test
  public void testTextSuccess() throws Exception {
    dataFormat = DataFormat.TEXT;
    dataFormatConfig.textFieldPath = "/text";
    dataFormatConfig.textRecordSeparator = RECORD_SEPERATOR;

    List<Record> recordsList = generateRecordPayload("text", Field.Type.STRING);
    List<String> rows, expected;
    TargetRunner runner = createRunner();
    runner.runInit();
    try {
      runner.runWrite(recordsList);

      rows = getQueue();
      expected = recordsList
          .stream()
          .map(x -> x.get("/text").getValueAsString() + RECORD_SEPERATOR)
          .collect(Collectors.toList());
    } finally {
      runner.runDestroy();
    }

    Assert.assertNotNull(rows);
    Assert.assertEquals(expected, rows);
  }

  @Test
  public void testByteSuccess() throws Exception {
    dataFormat = DataFormat.BINARY;
    dataFormatConfig.binaryFieldPath = "/bin";

    List<Record> recordsList = generateRecordPayload("bin", Field.Type.BYTE_ARRAY);
    List<String> rows, expected;
    TargetRunner runner = createRunner();
    runner.runInit();
    try {
      runner.runWrite(recordsList);
      rows = getQueue();
      expected = recordsList
          .stream()
          .map(x -> new String(x.get("/bin").getValueAsByteArray()) + RECORD_SEPERATOR)
          .collect(Collectors.toList());
    } finally {
      runner.runDestroy();
    }

    Assert.assertNotNull(rows);
    Assert.assertEquals(expected, rows);
  }

  @Test
  public void testAvroSuccess() throws Exception {
    String avroSchema = "{\"type\":\"record\",\"name\":\"abcd\",\"fields\":[{\"name\":\"text\",\"type\":\"string\"}]}";
    String testValue = "test string";
    dataFormat = DataFormat.AVRO;
    dataFormatConfig.avroSchemaSource = DestinationAvroSchemaSource.INLINE;
    dataFormatConfig.avroCompression = AvroCompression.NULL;
    dataFormatConfig.includeSchema = true;
    dataFormatConfig.registerSchema = false;
    dataFormatConfig.avroSchema = avroSchema;

    LinkedHashMap<String, Field> r1 = new LinkedHashMap<>();
    r1.put("text", Field.create(Field.Type.STRING, testValue));
    Record record1 = RecordCreator.create("s", "s:1");
    record1.set(Field.createListMap(r1));

    String result;
    TargetRunner runner = createRunner();
    runner.runInit();
    try {
      runner.runWrite(Arrays.asList(record1));
      List<String> rows = getQueue();
      result = rows.get(0);
    } finally {
      runner.runDestroy();
    }

    Assert.assertNotNull(result);
    Assert.assertTrue(result.contains(testValue));
  }

  @Test
  public void testJsonSuccess() throws Exception {
    dataFormat = DataFormat.JSON;
    dataFormatConfig.charset = "UTF-8";
    dataFormatConfig.jsonMode = JsonMode.ARRAY_OBJECTS;
    ObjectMapper objectMapper = new ObjectMapper();

    List<Record> recordsList = generateRecordPayload("text", Field.Type.STRING);
    List<String> rows;
    TargetRunner runner = createRunner();
    runner.runInit();
    try {
      runner.runWrite(recordsList);
      rows = getQueue();
    } finally {
      runner.runDestroy();
    }

    Assert.assertNotNull(rows);
    try {
      objectMapper.writeValueAsString(rows.get(0));
    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testDelimitedSuccess() throws Exception {
    dataFormat = DataFormat.DELIMITED;
    dataFormatConfig.csvFileFormat = CsvMode.CSV;
    dataFormatConfig.csvHeader = CsvHeader.IGNORE_HEADER;
    dataFormatConfig.csvReplaceNewLines = false;
    dataFormatConfig.csvReplaceNewLinesString = "";
    dataFormatConfig.charset = "UTF-8";

    List<Record> recordsList = generateRecordPayload("text", Field.Type.STRING);
    List<String> rows;
    TargetRunner runner = createRunner();
    runner.runInit();
    try {
      runner.runWrite(recordsList);
      rows = getQueue();
    } finally {
      runner.runDestroy();
    }

    Assert.assertNotNull(rows);
    Assert.assertTrue(rows.get(0).contains(","));
  }

  @Test
  public void testXmlSuccess() throws Exception {
    dataFormat = DataFormat.XML;
    dataFormatConfig.xmlPrettyPrint = false;
    dataFormatConfig.xmlValidateSchema = true;
    dataFormatConfig.xmlSchema =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<xs:schema id=\"testdataset\" xmlns=\"\"\n" +
            "  xmlns:xs=\"http://www.w3.org/2001/XMLSchema\"\n" +
            "  xmlns:msdata=\"urn:schemas-microsoft-com:xml-msdata\">\n" +
            "  <xs:element name=\"person\">\n" +
            "    <xs:complexType>\n" +
            "      <xs:all>\n" +
            "        <xs:element name=\"name\" type=\"xs:string\" minOccurs=\"0\" maxOccurs=\"1\"/>\n" +
            "        <xs:element name=\"age\" type=\"xs:string\" minOccurs=\"0\" maxOccurs=\"1\"/>\n" +
            "        <xs:element name=\"address\" minOccurs=\"0\" maxOccurs=\"1\">\n" +
            "          <xs:complexType>\n" +
            "            <xs:all>\n" +
            "              <xs:element name=\"street\" type=\"xs:string\" minOccurs=\"0\" maxOccurs=\"1\"/>\n" +
            "              <xs:element name=\"state\" type=\"xs:string\" minOccurs=\"0\" maxOccurs=\"1\"/>\n" +
            "            </xs:all>\n" +
            "          </xs:complexType>\n" +
            "        </xs:element>\n" +
            "      </xs:all>\n" +
            "    </xs:complexType>\n" +
            "  </xs:element>\n" +
            "  <xs:element name=\"testdataset\" msdata:IsDataSet=\"true\" msdata:UseCurrentLocale=\"true\">\n" +
            "    <xs:complexType>\n" +
            "      <xs:choice minOccurs=\"0\" maxOccurs=\"1\">\n" +
            "        <xs:element ref=\"person\"/>\n" +
            "      </xs:choice>\n" +
            "    </xs:complexType>\n" +
            "  </xs:element>\n" +
            "</xs:schema>";

    Map<String, Field> root = new HashMap<>();
    Map<String, Field> person = new HashMap<>();
    Map<String, Field> address = new HashMap<>();
    address.put("street", Field.create("123 blueberry ln"));
    address.put("state", Field.create("aruba"));
    person.put("address", Field.create(address));
    person.put("name", Field.create("joe schmoe"));
    person.put("age", Field.create(32));
    root.put("person", Field.create(person));

    Record r1 = RecordCreator.create("s", "s:1");
    r1.set(Field.create(root));

    List<String> rows;
    TargetRunner runner = createRunner();
    runner.runInit();
    try {
      runner.runWrite(Arrays.asList(r1));
      rows = getQueue();
    } finally {
      runner.runDestroy();
    }

    Assert.assertNotNull(rows);
    Assert.assertEquals(1, rows.size());
  }

  @Test
  public void testAllDestinationTypes() throws Exception {
    dataFormat = DataFormat.BINARY;

    jmsTargetConfig.destinationType = DestinationType.QUEUE;
    TargetRunner runner = createRunner();
    runner.runInit();
    runner.runDestroy();

    jmsTargetConfig.destinationType = DestinationType.TOPIC;
    runner = createRunner();
    runner.runInit();
    runner.runDestroy();

    jmsTargetConfig.destinationType = DestinationType.UNKNOWN;
    runner = createRunner();
    runner.runInit();
    runner.runDestroy();
  }

  private List<Record> generateRecordPayload(String fieldName, Field.Type type) {
    Object val1 = (type == Field.Type.STRING ? "hello" : "hello".getBytes());
    Object val2 = (type == Field.Type.STRING ? "world" : "world".getBytes());
    Object val3 = (type == Field.Type.STRING ? "!?.:;" : "!?.:;".getBytes());

    LinkedHashMap<String, Field> r1 = new LinkedHashMap<>();
    r1.put(fieldName, Field.create(type, val1));
    r1.put(fieldName + "2", Field.create(type, val2));
    r1.put(fieldName + "3", Field.create(type, val3));
    Record record1 = RecordCreator.create("s", "s:1");
    record1.set(Field.createListMap(r1));

    return Arrays.asList(record1);
  }
}
