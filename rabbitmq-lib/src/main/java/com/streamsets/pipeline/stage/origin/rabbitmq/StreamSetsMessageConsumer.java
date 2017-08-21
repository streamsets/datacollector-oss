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
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TransferQueue;

public class StreamSetsMessageConsumer extends DefaultConsumer {
  private static final Logger LOG = LoggerFactory.getLogger(StreamSetsMessageConsumer.class);

  private final TransferQueue<RabbitMessage> records;

  /**
   * Constructs a new instance and records its association to the passed-in channel.
   *
   * @param channel the channel to which this consumer is attached
   */
  public StreamSetsMessageConsumer(Channel channel, TransferQueue<RabbitMessage> records) {
    super(channel);
    this.records = records;
  }

  @Override
  public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
    try {
      records.transfer(new RabbitMessage(consumerTag, envelope, properties, body));
    } catch (InterruptedException ignored) {
      // no op
    }
  }

  @Override
  public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
    super.handleShutdownSignal(consumerTag, sig);
  }
}
