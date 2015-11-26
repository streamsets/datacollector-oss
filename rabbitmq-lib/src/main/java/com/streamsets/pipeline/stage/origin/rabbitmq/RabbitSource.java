/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.base.Optional;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.stage.origin.lib.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.origin.lib.ErrorRecordHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TransferQueue;

public class RabbitSource extends BaseSource implements OffsetCommitter {
  private static final Logger LOG = LoggerFactory.getLogger(RabbitSource.class);

  private final RabbitConfigBean conf;
  private final TransferQueue<RabbitMessage> messages = new LinkedTransferQueue<>();

  private ErrorRecordHandler errorRecordHandler;
  private Connection connection = null;
  private Channel channel = null;
  private StreamSetsMessageConsumer consumer;
  private DataParserFactory parserFactory;
  private String lastSourceOffset;

  public RabbitSource(RabbitConfigBean conf) {
    this.conf = conf;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    errorRecordHandler = new DefaultErrorRecordHandler(getContext());

    lastSourceOffset = "";
    ConnectionFactory factory = createConnectionFactory();
    try {
      factory.setUri(conf.uri);

      connection = factory.newConnection();
      channel = connection.createChannel();

      // Channel is always bound to the default exchange. When specified, we must declare the exchange.
      if (!conf.exchange.name.isEmpty()) {
        channel.exchangeDeclare(
            conf.exchange.name,
            conf.exchange.type.getValue(),
            conf.exchange.durable,
            conf.exchange.autoDelete,
            conf.exchange.properties
        );
      }

      channel.queueDeclare(
          conf.queue.name, conf.queue.durable, conf.queue.exclusive, conf.queue.autoDelete, conf.queue.properties
      );

      bindQueue();

      conf.dataFormatConfig.init(
          getContext(),
          conf.dataFormat,
          Groups.RABBITMQ.name(),
          issues
      );
      parserFactory = conf.dataFormatConfig.getParserFactory();

      startConsuming();
    } catch (IllegalArgumentException | NoSuchAlgorithmException | KeyManagementException | URISyntaxException e) {
      issues.add(getContext().createConfigIssue(
          Groups.RABBITMQ.name(),
          "conf.uri",
          Errors.RABBITMQ_03,
          e.toString()
      ));
    } catch (TimeoutException | IOException e) {
      // Connecting to RabbitMQ timed out or some other issue.
      String reason;
      if (e.getCause() != null) {
        reason = e.getCause().toString();
      } else {
        reason = e.toString();
      }
      issues.add(getContext().createConfigIssue(
          Groups.RABBITMQ.name(),
          "conf.uri",
          Errors.RABBITMQ_01,
          reason
      ));
    }

    return issues;
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    if (!isConnected() && !conf.advanced.automaticRecoveryEnabled) {
      // If we don't have automatic recovery enabled and the connection is closed, we should stop the pipeline.
      throw new StageException(Errors.RABBITMQ_05);
    }

    long maxTime = System.currentTimeMillis() + conf.basicConfig.maxWaitTime;
    int maxRecords = Math.min(maxBatchSize, conf.basicConfig.maxBatchSize);
    int numRecords = 0;
    String nextSourceOffset = lastSourceOffset;

    while (System.currentTimeMillis() < maxTime && numRecords < maxRecords) {
      try {
        RabbitMessage message = messages.poll(conf.basicConfig.maxWaitTime, TimeUnit.MILLISECONDS);
        if (message == null) {
          continue;
        }
        String recordId = message.getEnvelope().toString();
        Optional<Record> optional = parseRecord(recordId, message.getBody());
        if (optional.isPresent()) {
          Record record = optional.get();
          record.getHeader().setAttribute("deliveryTag", Long.toString(message.getEnvelope().getDeliveryTag()));
          batchMaker.addRecord(record);
          nextSourceOffset = record.getHeader().getAttribute("deliveryTag");
        }

      } catch (InterruptedException e) {
        LOG.warn("Pipeline is shutting down.");
      }
    }
    return nextSourceOffset;
  }

  private boolean isConnected() {
    return channel.isOpen() && connection.isOpen();
  }

  @Override
  public void commit(String offset) throws StageException {
    if (offset == null || offset.isEmpty() || lastSourceOffset.equals(offset)) {
      return;
    }

    try {
      consumer.getChannel().basicAck(Long.parseLong(offset), true);
      lastSourceOffset = offset;
    } catch (IOException e) {
      LOG.error("Failed to acknowledge offset: {}", offset, e);
      throw new StageException(Errors.RABBITMQ_02, offset, e.toString());
    }
  }

  @Override
  public void destroy() {
    try {
      if (channel != null) {
        channel.close();
      }

      if (connection != null) {
        connection.close();
      }
    } catch (IOException | TimeoutException e) {
      LOG.warn("Error while closing channel/connection: {}", e.toString(), e);
    }
    super.destroy();
  }

  private ConnectionFactory createConnectionFactory() {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setUsername(conf.credentialsConfig.username);
    factory.setPassword(conf.credentialsConfig.password);
    factory.setClientProperties(conf.rabbitmqProperties);
    factory.setConnectionTimeout(conf.advanced.connectionTimeout);
    factory.setAutomaticRecoveryEnabled(conf.advanced.automaticRecoveryEnabled);
    factory.setNetworkRecoveryInterval(conf.advanced.networkRecoveryInterval);
    factory.setRequestedHeartbeat(conf.advanced.heartbeatInterval);
    factory.setHandshakeTimeout(conf.advanced.handshakeTimeout);
    factory.setShutdownTimeout(conf.advanced.shutdownTimeout);
    factory.setRequestedFrameMax(conf.advanced.frameMax);
    factory.setRequestedChannelMax(conf.advanced.channelMax);
    return factory;
  }

  private void bindQueue() throws IOException {
    // If an exchange is specified, we need the routing key and then we bind it to the channel.
    if (!conf.exchange.name.isEmpty()) {
      String routingKey = conf.queue.routingKey;
      if (routingKey == null || routingKey.isEmpty()) {
        routingKey = conf.queue.name;
      }

      channel.queueBind(conf.queue.name, conf.exchange.name, routingKey);
    }
  }

  private void startConsuming() throws IOException {
    consumer = new StreamSetsMessageConsumer(channel, messages);
    if (conf.consumerTag == null || conf.consumerTag.isEmpty()) {
      channel.basicConsume(conf.queue.name, false, consumer);
    } else {
      channel.basicConsume(conf.queue.name, false, conf.consumerTag, consumer);
    }
  }

  private Optional<Record> parseRecord(String id, byte[] data) throws StageException {
    Record record = null;
    try {
      DataParser parser = parserFactory.getParser(id, data);
      record = parser.parse();
    } catch (DataParserException | IOException e) {
      LOG.error("Failed to parse record from received message: '{}'", e.toString(), e);
      errorRecordHandler.onError(Errors.RABBITMQ_04, new String(data, parserFactory.getSettings().getCharset()));
    }
    return Optional.fromNullable(record);
  }
}
