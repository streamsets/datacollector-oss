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
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.rabbitmq.config.Errors;
import com.streamsets.pipeline.lib.rabbitmq.config.Groups;
import com.streamsets.pipeline.lib.rabbitmq.common.RabbitCxnManager;
import com.streamsets.pipeline.lib.rabbitmq.common.RabbitUtil;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TransferQueue;

public class RabbitSource extends BaseSource implements OffsetCommitter {
  private static final Logger LOG = LoggerFactory.getLogger(RabbitSource.class);

  private final RabbitSourceConfigBean conf;
  private final TransferQueue<RabbitMessage> messages = new LinkedTransferQueue<>();

  private ErrorRecordHandler errorRecordHandler;
  private RabbitCxnManager rabbitCxnManager = new RabbitCxnManager();
  private StreamSetsMessageConsumer consumer;
  private DataParserFactory parserFactory;
  private String lastSourceOffset = "";

  private boolean checkBatchSize = true;

  public RabbitSource(RabbitSourceConfigBean conf) {
    this.conf = conf;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    if (conf.queue.name == null || conf.queue.name.equals("")) {
      issues.add(getContext().createConfigIssue(
          Groups.QUEUE.name(),
          "conf.queue.name",
          Errors.RABBITMQ_10
      ));
    }

    RabbitUtil.initRabbitStage(
        getContext(),
        conf,
        conf.dataFormat,
        conf.dataFormatConfig,
        rabbitCxnManager,
        issues
    );

    if (!issues.isEmpty()) {
      return issues;
    }

    try {
      startConsuming();
      errorRecordHandler = new DefaultErrorRecordHandler(getContext());
      parserFactory = conf.dataFormatConfig.getParserFactory();
    } catch (IOException e) {
      // Some other issue.
      LOG.error("Rabbit MQ issue.", e);

      String reason = (e.getCause() == null) ? e.toString() : e.getCause().toString();

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
    if (!getContext().isPreview() && checkBatchSize && conf.basicConfig.maxBatchSize > maxBatchSize) {
      getContext().reportError(Errors.RABBITMQ_11, maxBatchSize);
      checkBatchSize = false;
    }

    int numRecords = 0;
    String nextSourceOffset = lastSourceOffset;
    while (System.currentTimeMillis() < maxTime && numRecords < maxRecords) {
      try {
        RabbitMessage message = messages.poll(conf.basicConfig.maxWaitTime, TimeUnit.MILLISECONDS);
        if (message == null) {
          continue;
        }
        String recordId = message.getEnvelope().toString();
        List<Record> records = parseRabbitMessage(recordId, message.getBody());
        Envelope envelope = message.getEnvelope();
        for (Record record : records){
          BasicProperties properties = message.getProperties();
          Record.Header outHeader = record.getHeader();
          if (envelope != null) {
            setHeaderIfNotNull(outHeader, "deliveryTag", envelope.getDeliveryTag());
            setHeaderIfNotNull(outHeader, "exchange", envelope.getExchange());
            setHeaderIfNotNull(outHeader, "routingKey", envelope.getRoutingKey());
            setHeaderIfNotNull(outHeader, "redelivered", envelope.isRedeliver());
          }
          setHeaderIfNotNull(outHeader, "contentType", properties.getContentType());
          setHeaderIfNotNull(outHeader, "contentEncoding", properties.getContentEncoding());
          setHeaderIfNotNull(outHeader, "deliveryMode", properties.getDeliveryMode());
          setHeaderIfNotNull(outHeader, "priority", properties.getPriority());
          setHeaderIfNotNull(outHeader, "correlationId", properties.getCorrelationId());
          setHeaderIfNotNull(outHeader, "replyTo", properties.getReplyTo());
          setHeaderIfNotNull(outHeader, "expiration", properties.getExpiration());
          setHeaderIfNotNull(outHeader, "messageId", properties.getMessageId());
          setHeaderIfNotNull(outHeader, "timestamp", properties.getTimestamp());
          setHeaderIfNotNull(outHeader, "messageType", properties.getType());
          setHeaderIfNotNull(outHeader, "userId", properties.getUserId());
          setHeaderIfNotNull(outHeader, "appId", properties.getAppId());
          Map<String, Object> inHeaders = properties.getHeaders();
          if (inHeaders != null) {
            for (Map.Entry<String, Object> pair : inHeaders.entrySet()) {
              // I am concerned about overlapping with the above headers but it seems somewhat unlikely
              // in addition the behavior of copying these attributes in with no custom prefix is
              // how the jms origin behaves
              setHeaderIfNotNull(outHeader, pair.getKey(), pair.getValue());
            }
          }
          batchMaker.addRecord(record);
          numRecords++;
        }
        if (envelope != null) {
          nextSourceOffset = String.valueOf(envelope.getDeliveryTag());
        } else {
          nextSourceOffset = null;
          LOG.warn("Message received with no envelope" );
        }
      } catch (InterruptedException e) {
        LOG.warn("Pipeline is shutting down.");
      }
    }
    return nextSourceOffset;
  }

  private void setHeaderIfNotNull(Record.Header header, String key, Object val) {
    if (val != null) {
      header.setAttribute(key, convToString(val));
    }
  }

  private static String convToString(Object o) {
    if (o instanceof Date) {
      o = ((Date)o).getTime();
    }
    return String.valueOf(o);
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
      this.rabbitCxnManager.close();
    } catch (IOException | TimeoutException e) {
      LOG.warn("Error while closing channel/connection: {}", e.toString(), e);
    }
    super.destroy();
  }

  private void startConsuming() throws IOException {
    consumer = new StreamSetsMessageConsumer(this.rabbitCxnManager.getChannel(), messages);
    this.rabbitCxnManager.getChannel().basicQos(conf.basicConfig.maxBatchSize,true);
    if (conf.consumerTag == null || conf.consumerTag.isEmpty()) {
      this.rabbitCxnManager.getChannel().basicConsume(conf.queue.name, false, consumer);
    } else {
      this.rabbitCxnManager.getChannel().basicConsume(conf.queue.name, false, conf.consumerTag, consumer);
    }
  }

  private List<Record> parseRabbitMessage(String id, byte[] data) throws StageException {
    List<Record> records = new ArrayList<>();
    try (DataParser parser = parserFactory.getParser(id, data)) {
      Record record = parser.parse();
      while (record != null) {
        records.add(record);
        record = parser.parse();
      }
    } catch (IOException|DataParserException e) {
      LOG.error("Failed to parse record from received message: '{}'", e.toString(), e);
      errorRecordHandler.onError(Errors.RABBITMQ_04, new String(data, parserFactory.getSettings().getCharset()), e);
    }
    if (conf.produceSingleRecordPerMessage) {
      List<Field> list = new ArrayList<>();
      records.forEach(record -> list.add(record.get()));
      if (!list.isEmpty()) {
        Record record = records.get(0);
        record.set(Field.create(list));
        records.clear();
        records.add(record);
      }
    }
    return records;
  }

  private boolean isConnected() {
    return this.rabbitCxnManager.checkConnected();
  }

  TransferQueue<RabbitMessage> getMessageQueue() {
    return messages;
  }

  void setDataParserFactory(DataParserFactory dataParserFactory) {
    this.parserFactory = dataParserFactory;
  }

  void setStreamSetsMessageConsumer(StreamSetsMessageConsumer consumer) {
    this.consumer = consumer;
  }
}
