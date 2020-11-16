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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.service.dataformats.DataFormatGeneratorService;
import com.streamsets.pipeline.lib.jms.config.DestinationType;
import com.streamsets.pipeline.lib.jms.config.InitialContextFactory;
import com.streamsets.pipeline.lib.jms.config.connection.JmsConnection;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.sdk.service.SdkJsonDataFormatGeneratorService;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.security.AuthenticationUser;
import org.apache.activemq.security.SimpleAuthenticationPlugin;
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
import java.util.LinkedHashMap;
import java.util.List;

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
  private JmsTargetConfig jmsTargetConfig;

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

    jmsTargetConfig = new JmsTargetConfig();
    jmsTargetConfig.connection = new JmsConnection();
    jmsTargetConfig.connection.useCredentials = true;
    jmsTargetConfig.connection.username = () -> USERNAME;
    jmsTargetConfig.connection.password = () -> PASSWORD;
    jmsTargetConfig.destinationName = JNDI_PREFIX + DESTINATION_NAME;
    jmsTargetConfig.connection.initialContextFactory = INITIAL_CONTEXT_FACTORY;
    jmsTargetConfig.connection.connectionFactory = CONNECTION_FACTORY;
    jmsTargetConfig.connection.providerURL = BROKER_BIND_URL;
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
        jmsTargetConfig,
        new JmsMessageProducerFactoryImpl(),
        new InitialContextFactory()
    );
    return new TargetRunner.Builder(JmsDTarget.class, destination)
      .setOnRecordError(OnRecordError.TO_ERROR)
      .addService(DataFormatGeneratorService.class, new SdkJsonDataFormatGeneratorService())
      .build();
  }

  private void runInit(String expectedError) {
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
    jmsTargetConfig.connection.initialContextFactory = "invalid";
    runInit("JMS_00");
  }

  @Test
  public void testInvalidConnectionFactory() throws Exception {
    jmsTargetConfig.connection.connectionFactory = "invalid";
    runInit("JMS_01");
  }

  @Test
  public void testInvalidCreds() throws Exception {
    jmsTargetConfig.connection.username = () -> "invalid";
    runInit("JMS_04");
  }

  @Test
  public void testNoBroker() throws Exception {
    broker.stop();
    runInit("JMS_03");
  }

  @Test
  public void testNonExistingDestination() throws Exception {
    jmsTargetConfig.destinationName = "guess-what-i-dont-exists";

    Record record = generateRecordPayload("text", Field.Type.STRING);
    List<String> rows;
    TargetRunner runner = createRunner();
    runner.runInit();
    try {
      runner.runWrite(ImmutableList.of(record));
      rows = getQueue();

      Assert.assertEquals(1, runner.getErrorRecords().size());
      Assert.assertEquals(0, rows.size());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testDestinationExpression() throws Exception {
    jmsTargetConfig.destinationName = "${record:attribute('destination')}";

    Record record = generateRecordPayload("text", Field.Type.STRING);
    record.getHeader().setAttribute("destination", JNDI_PREFIX + DESTINATION_NAME);
    List<String> rows;
    TargetRunner runner = createRunner();
    runner.runInit();
    try {
      runner.runWrite(ImmutableList.of(record));

      rows = getQueue();
    } finally {
      runner.runDestroy();
    }

    Assert.assertNotNull(rows);
    Assert.assertEquals(ImmutableList.of("{\"text\":\"hello\"}"), rows);
  }

  @Test
  public void testTextSuccess() throws Exception {
    List<Record> recordsList = ImmutableList.of(generateRecordPayload("text", Field.Type.STRING));
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
    Assert.assertEquals(ImmutableList.of("{\"text\":\"hello\"}"), rows);
  }

  @Test
  public void testAllDestinationTypes() throws Exception {
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

  private Record generateRecordPayload(String fieldName, Field.Type type) {
    Object val1 = (type == Field.Type.STRING ? "hello" : "hello".getBytes());

    LinkedHashMap<String, Field> r1 = new LinkedHashMap<>();
    r1.put(fieldName, Field.create(type, val1));
    Record record1 = RecordCreator.create("s", "s:1");
    record1.set(Field.createListMap(r1));

    return record1;
  }
}
