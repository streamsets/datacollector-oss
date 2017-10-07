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

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TransferQueue;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class TestStreamSetsMessageConsumer {
  private static final Logger LOG = LoggerFactory.getLogger(TestStreamSetsMessageConsumer.class);
  private static final String QUEUE_NAME = "hello";
  private static final String EXCHANGE_NAME = "";
  private static final String TEST_MESSAGE_1 = "{\"field1\": \"abcdef\"}";

  private Source.Context context;
  private ExecutorService executor;

  @Before
  public void setUp() throws Exception {
    context = ContextInfoCreator.createSourceContext(
        "i", false, OnRecordError.TO_ERROR, Collections.<String>emptyList()
    );

    executor = Executors.newFixedThreadPool(1);
  }

  @After
  public void tearDown() throws Exception {
    executor.shutdown();
  }

  @Test
  public void testConsumerSingleMessage() throws Exception {
    TransferQueue<RabbitMessage> messages = new LinkedTransferQueue<>();

    Channel channel = mock(Channel.class);

    final Consumer consumer = new StreamSetsMessageConsumer(channel, messages);
    final Envelope envelope = new Envelope(1L, false, EXCHANGE_NAME, QUEUE_NAME);

    executor.submit(new Runnable() {
      @Override
      public void run() {
        try {
          consumer.handleDelivery("consumerTag", envelope, null, TEST_MESSAGE_1.getBytes());
        } catch (IOException ignored) {
          // no op
        }
      }
    });

    RabbitMessage message = messages.take();
    assertEquals(TEST_MESSAGE_1, new String(message.getBody(), StandardCharsets.UTF_8));
  }
}
