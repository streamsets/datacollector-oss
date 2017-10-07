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
package com.streamsets.pipeline.lib.rabbitmq.common;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.rabbitmq.config.BaseRabbitConfigBean;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Simple connection Manager for RabbitMQ Connection.
 * This can be extended to a multi connection pool later if needed.
 */
public class RabbitCxnManager {
  private Connection connection = null;
  private Channel channel = null;

  public void init(BaseRabbitConfigBean conf) throws IOException, StageException, TimeoutException {
    ConnectionFactory connectionFactory = RabbitUtil.createConnectionFactory(conf);
    this.connection = connectionFactory.newConnection();
    this.channel = this.connection.createChannel();
  }

  public Channel getChannel() {
    return channel;
  }

  public boolean checkConnected() {
    return channel.isOpen() && connection.isOpen();
  }

  public void close() throws IOException, TimeoutException {
    if (this.channel != null) {
      channel.close();
    }
    if (this.connection != null) {
      connection.close();
    }
  }

}
