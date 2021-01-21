/*
 * Copyright 2020 StreamSets Inc.
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

package com.streamsets.pipeline.stage.origin.multikafka;

import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.DeliveryGuarantee;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.kafka.KafkaErrors;
import com.streamsets.pipeline.lib.kafka.MessageKeyUtil;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

public class KafkaMultitopicRunnable implements Callable<Long> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaMultitopicRunnable.class);

  private static final long MIN_CONSUMER_POLLING_INTERVAL_MS = 100;
  private final MultiSdcKafkaConsumer<String, byte[]> consumer;
  private final long threadID;
  private final List<String> topicList;
  private final CountDownLatch startProcessingGate;
  private final PushSource.Context context;
  private final DataParserFactory parserFactory;
  private final MultiKafkaBeanConfig conf;
  private final int batchSize;

  Map<TopicPartition, OffsetAndMetadata> offsetsMap;

  public KafkaMultitopicRunnable(
      long threadID,
      MultiSdcKafkaConsumer<String, byte[]> consumer,
      CountDownLatch startProcessingGate,
      PushSource.Context context,
      DataParserFactory parserFactory,
      MultiKafkaBeanConfig conf,
      int batchSize
  ) {
    this.consumer = consumer;
    this.threadID = threadID;
    this.topicList = conf.topicList;
    this.startProcessingGate = startProcessingGate;
    this.context = context;
    this.parserFactory = parserFactory;
    this.conf = conf;
    this.batchSize = batchSize;
    offsetsMap = new HashMap<>();
  }

  BatchContext batchContext = null;
  CountingDefaultErrorRecordHandler errorRecordHandler = null;
  long messagesProcessed = 0;
  long recordsProcessed = 0;

  @Override
  public Long call() throws Exception {
    Thread.currentThread().setName("kafkaConsumerThread-" + threadID);

    // wait until all threads are spun up before processing
    LOG.debug("Thread {} waiting on other consumer threads to start up", Thread.currentThread().getName());
    startProcessingGate.await();

    LOG.debug("Starting poll loop in thread: {}", Thread.currentThread().getName());

    LOG.info("Minimum Kafka consumer Poll interval is set to: {}", MIN_CONSUMER_POLLING_INTERVAL_MS);

    try {
      consumer.subscribe(topicList);
      // protected loop. want it to finish completely, or not start at all.
      // only 2 conditions that we want to halt execution. must handle gracefully

      while (!getContext().isStopped() && !Thread.interrupted()) {
        produceRecords();
      }

    } catch (Exception e) { //NOSONAR
      LOG.error("Encountered error in multi kafka thread {} during read {}", threadID, e.getMessage(), e);
      throw new StageException(KafkaErrors.KAFKA_29, e);
    } finally {
      consumer.unsubscribe();
      consumer.close();
    }

    LOG.info("multi kafka thread {} consumed {} messages into {} records.",
        threadID,
        messagesProcessed,
        recordsProcessed
    );
    return messagesProcessed;
  }

  private void produceRecords() {
    long startTime = System.currentTimeMillis();
    List<Record> records = new ArrayList<>();
    long pollInterval = Math.max(MIN_CONSUMER_POLLING_INTERVAL_MS,
        conf.batchWaitTime - (System.currentTimeMillis() - startTime)
    );

    ConsumerRecords<String, byte[]> messages = consumer.poll(pollInterval);

    if (!messages.isEmpty()) {
      LOG.info("Received {} messages from Kafka", Collections.singletonList(messages).size());
      // start a new batch, get a new error record handler for batch.
      batchContext = getContext().startBatch();
      errorRecordHandler = new CountingDefaultErrorRecordHandler(getContext(), batchContext);
      for (ConsumerRecord<String, byte[]> item : messages) {

        // We still support Kafka 0.9 that doesn't have support for timestamp. Thus this code simply calls those
        // methods in a safe manner and fills defaults in case that those methods do not exists. This fragment can
        // be dropped (or this patch reverted) when we drop support for Kafka 0.9.
        long timestamp;
        String timestampType;
        try {
          timestamp = item.timestamp();
          timestampType = item.timestampType().name;
        } catch (NoSuchMethodError ex) {
          LOG.debug("Kafka does not support timestamp in this version, skipping");
          timestamp = -1;
          timestampType = "";
        }

        records.addAll(createRecord(errorRecordHandler, item, timestamp, timestampType));

        //If we already reached the max number of records or the maximum wait time we send the batch and start a new one
        if (records.size() >= batchSize || System.currentTimeMillis() - startTime >= pollInterval) {
          LOG.info("Record or time limit reached, restarting batch");
          startTime = System.currentTimeMillis();
          records.forEach(batchContext.getBatchMaker()::addRecord);
          commitSyncAndProcess();
          recordsProcessed += records.size();
          records.clear();
          batchContext = getContext().startBatch();
          errorRecordHandler = new CountingDefaultErrorRecordHandler(getContext(), batchContext);
        }
      }

      messagesProcessed += messages.count();

      if (!records.isEmpty()) { // Some errors that are dangling after the batches are full
        LOG.info("Messages exhausted, sending extra records");
        records.forEach(batchContext.getBatchMaker()::addRecord);
        commitSyncAndProcess();
        recordsProcessed += records.size();
      } else if (errorRecordHandler.getErrorRecordCount() > 0) { // All the records are errors, so records is empty
        LOG.info("The batch only contains error records, sending them anyway");
        commitSyncAndProcess();
      }
    }
  }

  private void commitSyncAndProcess() {
    LOG.info("Committing and processing batch");
    if (!getContext().isPreview() && getContext().getDeliveryGuarantee() == DeliveryGuarantee.AT_MOST_ONCE) {
      consumer.commitSync(offsetsMap);
      offsetsMap.clear();
    }

    boolean batchSuccessful = getContext().processBatch(batchContext);

    if (!getContext().isPreview() &&
        batchSuccessful &&
        getContext().getDeliveryGuarantee() == DeliveryGuarantee.AT_LEAST_ONCE) {
      consumer.commitSync(offsetsMap);
      offsetsMap.clear();
    }
  }

  private List<Record> createRecord(
      ErrorRecordHandler errorRecordHandler, ConsumerRecord<String, byte[]> item, long timestamp, String timestampType
  ) {
    String topic = item.topic();
    int partition = item.partition();
    long offset = item.offset();
    byte[] payload = item.value();
    Object messageKey = item.key();

    Utils.checkNotNull(parserFactory, "Initialization failed");

    String messageId = getMessageId(topic, partition, offset);
    List<Record> records = new ArrayList<>();

    if (payload == null) {
      LOG.debug("NULL value (tombstone) read, it has been discarded");
    } else {

      try (DataParser parser = parserFactory.getParser(messageId, payload)) {
        Record record = parser.parse();

        while (record != null) {
          record.getHeader().setAttribute(HeaderAttributeConstants.TOPIC, topic);
          record.getHeader().setAttribute(HeaderAttributeConstants.PARTITION, String.valueOf(partition));
          record.getHeader().setAttribute(HeaderAttributeConstants.OFFSET, String.valueOf(offset));
          if (conf.timestampsEnabled) {
            record.getHeader().setAttribute(HeaderAttributeConstants.KAFKA_TIMESTAMP, String.valueOf(timestamp));
            record.getHeader().setAttribute(HeaderAttributeConstants.KAFKA_TIMESTAMP_TYPE, timestampType);
          }

          handleMessageKey(messageKey, record, partition, offset);
          records.add(record);
          record = parser.parse();
        }
      } catch (DataParserException | IOException ex) {
        Record record = getContext().createRecord(messageId);
        record.set(Field.create(payload));
        String exMessage = ex.toString();
        if (ex.getClass().toString().equals("class com.fasterxml.jackson.core.JsonParseException")) {
          // SDC-15723. Trying to catch this exception is hard, as its behaviour is not expected.
          // This workaround using its String seems to be the best way to do it.
          exMessage = "Cannot parse JSON from record";
        }
        errorRecordHandler.onError(
            new OnRecordErrorException(
                record,
                KafkaErrors.KAFKA_37,
                messageId,
                exMessage,
                ex
            )
        );
      }

      // Add the Kafka offset to be committed
      offsetsMap.put(new TopicPartition(topic, partition), new OffsetAndMetadata(offset + 1));

      if (conf.produceSingleRecordPerMessage) {
        List<Field> list = new ArrayList<>();
        for (Record record : records) {
          list.add(record.get());
        }
        Record record = records.get(0);
        record.set(Field.create(list));
        records.clear();
        records.add(record);
      }
    }

    return records;
  }

  private void handleMessageKey(Object messageKey, Record record, int partition, long offset) {
    try {
      MessageKeyUtil.handleMessageKey(messageKey,
          conf.keyCaptureMode,
          record,
          conf.keyCaptureField,
          conf.keyCaptureAttribute
      );
    } catch (Exception e) {
      throw new OnRecordErrorException(record, KafkaErrors.KAFKA_201, partition, offset, e.getMessage(), e);
    }
  }

  private PushSource.Context getContext() {
    return context;
  }

  private static String getMessageId(String topic, int partition, long offset) {
    return topic + "::" + partition + "::" + offset;
  }
}
