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
package com.streamsets.pipeline.stage.origin.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import com.streamsets.pipeline.api.ErrorStage;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseStage;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.DataParserFormat;
import com.streamsets.pipeline.lib.rabbitmq.config.Errors;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TransferQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

@PrepareForTest(RabbitSource.class)
public class RabbitSourceTest extends BaseRabbitStageTest{
  private static final String CONSUMER_TAG = "c1";
  private static final String QUEUE_NAME = "hello";
  private static final String EXCHANGE_NAME = "";
  private static final String TEST_MESSAGE_1 = "{\"field1\": \"abcdef\"}";
  private static final String CLUSTER_ID = "cluster-1";
  private static final long DELIVERY_TAG = 1;
  private static final boolean REDELIVERED = false;
  private static final String CUSTOM_HEADER_KEY = "k1";
  private static final String CUSTOM_HEADER_VAL = "v1";
  private static final String CONTENT_TYPE = "application/json";

  @Before
  public void setup() {
    this.conf = new RabbitSourceConfigBean();
    this.conf.queue.name = QUEUE_NAME;
  }

  @Override
  protected StageRunner newStageRunner(String outputLane) {
    return new SourceRunner.Builder(RabbitDSource.class, (Source) this.stage).addOutputLane("output").build();
  }

  @Override
  protected BaseStage newStage() {
    return new RabbitSource((RabbitSourceConfigBean) this.conf);
  }

  @Test(expected = StageException.class)
  public void testFailIfAutomaticRecoveryDisabled() throws Exception {
    super.testFailIfAutomaticRecoveryDisabled();

    doReturn(new ArrayList<Stage.ConfigIssue>()).when((RabbitSource)stage).init();

    PowerMockito.doReturn(false).when(stage, "isConnected");

    ((SourceRunner)runner).runProduce(null, 1000);
    runner.runDestroy();
  }

  @Test
  public void testContinueIfAutomaticRecoveryEnabled() throws Exception {
    ((RabbitSourceConfigBean)conf).basicConfig.maxWaitTime = 0; // Set this low so that we don't slow down the test.

    stage = PowerMockito.spy(newStage());

    doReturn(new ArrayList<Stage.ConfigIssue>()).when((RabbitSource)stage).init();

    PowerMockito.doReturn(false).when(stage, "isConnected");

    this.runner = newStageRunner("output");

    runner.runInit();

    StageRunner.Output output = ((SourceRunner)runner).runProduce(null, 1000);
    assertTrue(output.getRecords().get("output").isEmpty());

    runner.runDestroy();
  }

  @Test
  public void testHeaderProcessing() throws Exception {
    ((RabbitSourceConfigBean)conf).basicConfig.maxWaitTime = 1000; // Set this low so that we don't slow down the test.

    stage = PowerMockito.spy(newStage());

    // setup some fake data and force it onto the source's queue
    RabbitSource source = (RabbitSource)stage;
    TransferQueue<RabbitMessage> messages = source.getMessageQueue();
    Envelope envelope = new Envelope(DELIVERY_TAG, REDELIVERED, EXCHANGE_NAME, QUEUE_NAME);
    AMQP.BasicProperties.Builder propertiesBuilder = new AMQP.BasicProperties.Builder();
    propertiesBuilder.contentType(CONTENT_TYPE);
    Map<String, Object> customHeaders = new HashMap<>();
    customHeaders.put(CUSTOM_HEADER_KEY, CUSTOM_HEADER_VAL);
    propertiesBuilder.headers(customHeaders);
    propertiesBuilder.clusterId(CLUSTER_ID);
    AMQP.BasicProperties properties = propertiesBuilder.build();
    RabbitMessage msg = new RabbitMessage(CONSUMER_TAG, envelope, properties, TEST_MESSAGE_1.getBytes());
    source.getMessageQueue().put(msg);
    doReturn(new ArrayList<Stage.ConfigIssue>()).when((RabbitSource)stage).init();

    PowerMockito.doReturn(false).when(stage, "isConnected");

    this.runner = newStageRunner("output");

    // setup items which are not correctly configured in init
    Channel channel = mock(Channel.class);
    StreamSetsMessageConsumer consumer = new StreamSetsMessageConsumer(channel, messages);
    source.setStreamSetsMessageConsumer(consumer);
    DataParserFactory parserFactory = new DataParserFactoryBuilder(runner.getContext(), DataParserFormat.JSON)
        .setCharset(StandardCharsets.UTF_8)
        .setMode(JsonMode.MULTIPLE_OBJECTS)
        .setMaxDataLen(-1)
        .build();
    source.setDataParserFactory(parserFactory);

    runner.runInit();

    StageRunner.Output output = ((SourceRunner)runner).runProduce(null, 1000);
    List<Record> records = output.getRecords().get("output");
    assertEquals(1, records.size());
    Record record = records.get(0);
    assertEquals(String.valueOf(DELIVERY_TAG), record.getHeader().getAttribute("deliveryTag"));
    assertEquals(String.valueOf(REDELIVERED), record.getHeader().getAttribute("redelivered"));
    assertEquals(EXCHANGE_NAME, record.getHeader().getAttribute("exchange"));
    assertEquals(CONTENT_TYPE, record.getHeader().getAttribute("contentType"));
    assertNull(record.getHeader().getAttribute("appId"));
    assertEquals(CUSTOM_HEADER_VAL, record.getHeader().getAttribute(CUSTOM_HEADER_KEY));
    runner.runDestroy();
  }

  @Test
  public void testUnspecifiedQueue() throws Exception {
    conf.queue.name = "";

    stage = newStage();

    this.runner = newStageRunner("output");

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertTrue(issues.stream().anyMatch(issue -> checkIssue(issue, Errors.RABBITMQ_10.getCode())));
  }
}
