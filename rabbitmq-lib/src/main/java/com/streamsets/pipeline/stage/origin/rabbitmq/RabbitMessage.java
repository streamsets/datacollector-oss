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

import com.rabbitmq.client.BasicProperties;
import com.rabbitmq.client.Envelope;

/**
 * Wrapper class for the data received with each RabbitMQ Message
 */
public class RabbitMessage {
  private final String consumerTag;
  private final Envelope envelope;
  private final BasicProperties properties;
  private final byte[] body;

  public RabbitMessage(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body) {
    this.consumerTag = consumerTag;
    this.envelope = envelope;
    this.properties = properties;
    this.body = body;
  }

  public String getConsumerTag() {
    return consumerTag;
  }

  public Envelope getEnvelope() {
    return envelope;
  }

  public BasicProperties getProperties() {
    return properties;
  }

  public byte[] getBody() {
    return body;
  }
}
