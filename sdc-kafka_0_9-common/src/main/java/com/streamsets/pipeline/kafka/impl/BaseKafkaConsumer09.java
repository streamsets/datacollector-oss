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
package com.streamsets.pipeline.kafka.impl;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.kafka.api.MessageAndOffset;
import com.streamsets.pipeline.kafka.api.SdcKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class BaseKafkaConsumer09 implements SdcKafkaConsumer {

  private static final int BLOCKING_QUEUE_SIZE = 10000;
  private static final int CONSUMER_POLLING_WINDOW_MS = 100;

  protected KafkaConsumer<String, byte[]> kafkaConsumer;

  protected final String topic;

  // blocking queue which is populated by the kafka consumer runnable that polls
  private final ArrayBlockingQueue<ConsumerRecord<String, byte[]>> recordQueue;
  private final ScheduledExecutorService executorService;
  // runnable that polls the kafka topic for records and populates the blocking queue
  private KafkaConsumerRunner kafkaConsumerRunner;
  // map holding required information to commit offset
  private final Map<TopicPartition, OffsetAndMetadata> topicPartitionToOffsetMetadataMap;
  // mutex to ensure poll and commit are never called concurrently
  private final Object pollCommitMutex;

  private boolean isInited = false;

  private static final Logger LOG = LoggerFactory.getLogger(BaseKafkaConsumer09.class);

  public BaseKafkaConsumer09(String topic) {
    this.topic = topic;
    this.topicPartitionToOffsetMetadataMap = new HashMap<>();
    this.recordQueue = new ArrayBlockingQueue<>(BLOCKING_QUEUE_SIZE);
    this.executorService = new ScheduledThreadPoolExecutor(1);
    this.pollCommitMutex = new Object();
  }

  @Override
  public void validate(List<Stage.ConfigIssue> issues, Stage.Context context) {
    createConsumer();
    try {
      kafkaConsumer.partitionsFor(topic);
    } catch (WakeupException | AuthorizationException e) { // NOSONAR
      handlePartitionsForException(issues, context, e);
    }
  }

  @Override
  public void init() throws StageException {
    // guard against developer error - init must not be called twice and it must be called after validate
    if(isInited) {
      throw new RuntimeException("SdcKafkaConsumer should not be initialized more than once");
    }
    if(null == kafkaConsumer) {
      throw new RuntimeException("validate method must be called before init which creates the Kafka Consumer");
    }
    kafkaConsumerRunner = new KafkaConsumerRunner(kafkaConsumer, recordQueue, pollCommitMutex);
    executorService.scheduleWithFixedDelay(kafkaConsumerRunner, 0, 20, TimeUnit.MILLISECONDS);
    isInited = true;
  }

  @Override
  public void destroy() {
    if(kafkaConsumerRunner != null) {
      kafkaConsumerRunner.shutdown();
    }
    executorService.shutdownNow();

    if(kafkaConsumer != null) {
      try {
        synchronized (pollCommitMutex) {
          kafkaConsumer.close();
        }
        kafkaConsumer = null;
      } catch (Exception e) {
        LOG.error("Error shutting down Kafka Consumer, reason: {}", e.toString(), e);
      }
    }
    isInited = false;
  }

  @Override
  public void commit() {
    synchronized (pollCommitMutex) {
      kafkaConsumer.commitSync(topicPartitionToOffsetMetadataMap);
    }
  }

  @Override
  public MessageAndOffset read() throws StageException {
    ConsumerRecord<String, byte[]> next;
    try {
       // If no record is available within the given time return null
       next = recordQueue.poll(500, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw createReadException(e);
    }
    MessageAndOffset messageAndOffset = null;
    if(next != null) {
      updateEntry(next);
      messageAndOffset = new MessageAndOffset(next.value(), next.offset(), next.partition());
    }
    return messageAndOffset;
  }

  protected abstract void configureKafkaProperties(Properties props);

  protected abstract void handlePartitionsForException(
    List<Stage.ConfigIssue> issues,
    Stage.Context context,
    KafkaException e
  );

  protected abstract StageException createReadException(Exception e);

  @Override
  public String getVersion() {
    return Kafka09Constants.KAFKA_VERSION;
  }

  private void updateEntry(ConsumerRecord<String, byte[]> next) {
    TopicPartition topicPartition = new TopicPartition(topic, next.partition());
    // The committed offset should always be the offset of the next message that we will read.
    // http://www.confluent.io/blog/tutorial-getting-started-with-the-new-apache-kafka-0.9-consumer-client
    OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(next.offset() + 1);
    topicPartitionToOffsetMetadataMap.put(topicPartition, offsetAndMetadata);
  }

  protected void createConsumer() {
    Properties  kafkaConsumerProperties = new Properties();
    configureKafkaProperties(kafkaConsumerProperties);
    LOG.debug("Creating Kafka Consumer with properties {}" , kafkaConsumerProperties.toString());
    kafkaConsumer = new KafkaConsumer<>(kafkaConsumerProperties);
    kafkaConsumer.subscribe(Collections.singletonList(topic));
  }

  static class KafkaConsumerRunner implements Runnable {
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final KafkaConsumer<String, byte[]> consumer;
    private final Object mutex;
    private final ArrayBlockingQueue<ConsumerRecord<String, byte[]>> blockingQueue;

    public KafkaConsumerRunner(
      KafkaConsumer<String, byte[]> consumer,
      ArrayBlockingQueue<ConsumerRecord<String, byte[]>> blockingQueue,
      Object mutex
    ) {
      this.consumer = consumer;
      this.blockingQueue = blockingQueue;
      this.mutex = mutex;
    }

    @Override
    public void run() {
      try {
        ConsumerRecords<String, byte[]> poll;
        synchronized (mutex) {
           poll = consumer.poll(CONSUMER_POLLING_WINDOW_MS);
        }
        for(ConsumerRecord<String, byte[]> r : poll) {
          if (!putConsumerRecord(r)) {
            return;
          }
        }
      } catch (WakeupException e) {
        // Ignore exception if closing
        if (!closed.get()) {
          throw e;
        }
      }
    }

    // Shutdown hook which can be called from a separate thread
    public void shutdown() {
      closed.set(true);
      consumer.wakeup();
    }

    private boolean putConsumerRecord(ConsumerRecord<String, byte[]> record) {
      boolean succeeded = false;
      try {
        blockingQueue.put(record);
        succeeded = true;
      } catch (InterruptedException e) {
        LOG.error("Failed to poll KafkaConsumer, reason : {}", e.toString(), e);
      }
      return succeeded;
    }
  }

}
