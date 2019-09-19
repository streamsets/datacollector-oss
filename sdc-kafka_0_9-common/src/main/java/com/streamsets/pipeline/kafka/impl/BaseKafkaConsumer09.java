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
package com.streamsets.pipeline.kafka.impl;

import com.codahale.metrics.Histogram;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.kafka.api.KafkaOriginGroups;
import com.streamsets.pipeline.kafka.api.MessageAndOffset;
import com.streamsets.pipeline.kafka.api.SdcKafkaConsumer;
import com.streamsets.pipeline.lib.kafka.KafkaErrors;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class BaseKafkaConsumer09 implements SdcKafkaConsumer, ConsumerRebalanceListener {

  private static final int BLOCKING_QUEUE_SIZE = 10000;
  private static final int CONSUMER_POLLING_WINDOW_MS = 100;
  private static final String REBALANCE_IN_PROGRESS = "Rebalance In Progress";
  private static final String WAITING_ON_POLL = "Waiting on poll";
  public static final String KAFKA_CONFIG_BEAN_PREFIX = "kafkaConfigBean.";
  public static final String TIMESTAMPS = "timestamps.";
  public static final String KAFKA_AUTO_OFFSET_RESET = "kafkaAutoOffsetReset";

  protected Consumer<Object, byte[]> kafkaConsumer;

  protected final String topic;
  private volatile long rebalanceTime;

  // blocking queue which is populated by the kafka consumer runnable that polls
  private ArrayBlockingQueue<ConsumerRecord<Object, byte[]>> recordQueue;
  private final ScheduledExecutorService executorService;
  // runnable that polls the kafka topic for records and populates the blocking queue
  private KafkaConsumerRunner kafkaConsumerRunner;
  // map holding required information to commit offset
  private final Map<TopicPartition, OffsetAndMetadata> topicPartitionToOffsetMetadataMap;
  // mutex to ensure poll and commit are never called concurrently
  private final Object pollCommitMutex;
  // Holds information whether Kafka is currently rebalancing the consumer group, if so we won't be reading any data
  private final AtomicBoolean rebalanceInProgress;
  // Set by commit() when Kafka throws CommitFailedException to force us to call consumer's poll() before anything else
  private final AtomicBoolean needToCallPoll;
  // Source's context for various metrics
  private final Source.Context context;
  // Histogram for rebalancing events
  private final Histogram rebalanceHistogram;
  // Gauge with various states that we're propagating up
  private final Map<String, Object> gaugeMap;

  private final Set<TopicPartition> currentAssignments = new HashSet<>();

  private boolean isInited = false;

  private static final Logger LOG = LoggerFactory.getLogger(BaseKafkaConsumer09.class);

  boolean isTimestampSupported(){
    return false;
  }

  boolean isTimestampEnabled(){
    return false;
  }

  MessageAndOffset getMessageAndOffset(ConsumerRecord message, boolean isEnabled){
    return null;
  }

  public BaseKafkaConsumer09(String topic, Source.Context context, int batchSize) {
    this.topic = topic;
    this.topicPartitionToOffsetMetadataMap = new HashMap<>();
    this.recordQueue = new ArrayBlockingQueue<>(batchSize);
    this.executorService = new ScheduledThreadPoolExecutor(1);
    this.pollCommitMutex = new Object();
    this.rebalanceInProgress = new AtomicBoolean(false);
    this.needToCallPoll = new AtomicBoolean(false);
    this.context = context;
    this.rebalanceHistogram = context.createHistogram("Rebalance Time");
    this.gaugeMap = context.createGauge("Internal state").getValue();
  }

  @Override
  public void validate(List<Stage.ConfigIssue> issues, Stage.Context context) {
    if (isTimestampEnabled() && !isTimestampSupported()) {
      issues.add(context.createConfigIssue(KafkaOriginGroups.KAFKA.name(),
          KAFKA_CONFIG_BEAN_PREFIX + TIMESTAMPS,
          KafkaErrors.KAFKA_75
      ));
    }

    try {
      validateAutoOffsetReset(issues);
    } catch (StageException e) {
      issues.add(context.createConfigIssue(KafkaOriginGroups.KAFKA.name(),
          KAFKA_CONFIG_BEAN_PREFIX + KAFKA_AUTO_OFFSET_RESET,
          e.getErrorCode()
      ));
    }

    if (issues.isEmpty()) {
      createConsumer();
      subscribeConsumer();
      try {
        kafkaConsumer.partitionsFor(topic);
      } catch (WakeupException | AuthorizationException e) { // NOSONAR
        handlePartitionsForException(issues, context, e);
      }
    }
  }

  protected void subscribeConsumer() {
    kafkaConsumer.subscribe(Collections.singletonList(topic), this);
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    rebalanceTime = System.currentTimeMillis();
    LOG.debug("Received onPartitionsRevoked call back for {}", StringUtils.join(partitions, ","));
    rebalanceInProgress.set(true);
    // Based on Kafka documentation, on rebalance all consumers will start from their committed offsets, hence
    // it's safe to simply discard the blocking queue.
    recordQueue.clear();
    if (!topicPartitionToOffsetMetadataMap.isEmpty()) {
      LOG.debug("Clearing offset map: {}", topicPartitionToOffsetMetadataMap.toString());
      topicPartitionToOffsetMetadataMap.clear();
    }
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    LOG.debug("Received onPartitionsAssigned call back for {}", StringUtils.join(partitions, ","));
    rebalanceInProgress.set(false);
    // Update histogram with time spent inside rebalancing
    rebalanceHistogram.update(System.currentTimeMillis() - rebalanceTime);

    currentAssignments.clear();
    currentAssignments.addAll(partitions);
  }

  @Override
  public void init() throws StageException {
    // guard against developer error - init must not be called twice and it must be called after validate
    Utils.checkState(!isInited, "SdcKafkaConsumer should not be initialized more than once");
    Utils.checkState(
        kafkaConsumer != null,
        "validate method must be called before init which creates the Kafka Consumer"
    );
    kafkaConsumerRunner = new KafkaConsumerRunner(this, context);
    executorService.scheduleWithFixedDelay(kafkaConsumerRunner, 0, 20, TimeUnit.MILLISECONDS);
    isInited = true;
  }

  public Set<TopicPartition> getCurrentAssignments() {
    return Collections.unmodifiableSet(currentAssignments);
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
      // While rebalancing there is no point for us to commit offset since it's not allowed operation
      if(rebalanceInProgress.get()) {
        LOG.debug("Kafka is rebalancing, not commiting offsets");
        return;
      }

      if(needToCallPoll.get()) {
        LOG.debug("Waiting on poll to be properly called before continuing.");
        return;
      }

      try {
        if(topicPartitionToOffsetMetadataMap.isEmpty()) {
          LOG.debug("Skipping committing offsets since we haven't consume anything.");
          return;
        }

        LOG.debug("Committing offsets: {}", topicPartitionToOffsetMetadataMap.toString());
        kafkaConsumer.commitSync(topicPartitionToOffsetMetadataMap);
      } catch(CommitFailedException ex) {
        LOG.warn("Can't commit offset to Kafka: {}", ex.toString(), ex);
        // After CommitFailedException we MUST call consumer's poll() method first
        needToCallPoll.set(true);
        // The consumer thread might be stuck on writing to the queue, so we need to clean it up to unblock that thread
        recordQueue.clear();
      } finally {
        // either we've committed the offsets (so now we drop them so that we don't re-commit anything)
        // or CommitFailedException was thrown, in which case poll needs to be called again and they are invalid
        topicPartitionToOffsetMetadataMap.clear();
      }
    }
  }

  @Override
  public MessageAndOffset read() throws StageException {
    // First of all update gauge representing internal state
    gaugeMap.put(REBALANCE_IN_PROGRESS, rebalanceInProgress.get());
    gaugeMap.put(WAITING_ON_POLL, needToCallPoll.get());

    // On rebalancing or if we need to call poll first, there is no point to read any messages from the buffer
    if(rebalanceInProgress.get() || needToCallPoll.get()) {
      // Small back off to give Kafka time to rebalance or us to get a chance to call poll()
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        // Not really important to us
        Thread.currentThread().interrupt();
      }

      // Return no message
      LOG.debug(
          "Generating empty batch since the consumer is not ready to consume (rebalance={}, needToPoll={})",
          rebalanceInProgress.get(),
          needToCallPoll.get()
      );
      return null;
    }

    ConsumerRecord<Object, byte[]> next;
    try {
       // If no record is available within the given time return null
       next = recordQueue.poll(500, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw createReadException(e);
    }
    MessageAndOffset messageAndOffset = null;
    if(next != null) {
      updateEntry(next);
      messageAndOffset = getMessageAndOffset(next, isTimestampEnabled());
    }
    return messageAndOffset;
  }

  protected abstract void configureKafkaProperties(Properties props);

  protected abstract void validateAutoOffsetReset(List<Stage.ConfigIssue> issues) throws StageException;

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

  private void updateEntry(ConsumerRecord<Object, byte[]> next) {
    TopicPartition topicPartition = new TopicPartition(topic, next.partition());
    // The committed offset should always be the offset of the next message that we will read.
    // http://www.confluent.io/blog/tutorial-getting-started-with-the-new-apache-kafka-0.9-consumer-client
    OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(next.offset() + 1);
    topicPartitionToOffsetMetadataMap.put(topicPartition, offsetAndMetadata);
  }

  protected void createConsumer() {
    Properties  kafkaConsumerProperties = new Properties();
    configureKafkaProperties(kafkaConsumerProperties);
    LOG.debug("Creating Kafka Consumer with properties {}", kafkaConsumerProperties.toString());
    kafkaConsumer = new KafkaConsumer<>(kafkaConsumerProperties);
  }

  static class KafkaConsumerRunner implements Runnable {
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final BaseKafkaConsumer09 consumer;
    private final Source.Context context;
    private boolean isErrorReported;
    private int countOfErrorRecords;

    public KafkaConsumerRunner(BaseKafkaConsumer09 consumer, Source.Context context) {
      this.consumer = consumer;
      this.context = context;
      isErrorReported = false;
      countOfErrorRecords = 0;
    }

    @Override
    public void run() {
      try {
        ConsumerRecords<Object, byte[]> poll;
        synchronized (consumer.pollCommitMutex) {
          LOG.debug(String.format(
              "Polling with current assignments: %s",
              StringUtils.join(consumer.currentAssignments, ",")
          ));
           poll = consumer.kafkaConsumer.poll(CONSUMER_POLLING_WINDOW_MS);
           // Since we've just called poll, we've always set it to true
           consumer.needToCallPoll.set(false);
        }
        LOG.trace(
            "Read {} messages from Kafka; current blockingQueue size: {}",
            poll.count(),
            consumer.recordQueue.size()
        );
        for(ConsumerRecord<Object, byte[]> r : poll) {
          // If we need to call poll, we'll jump out and ignore the rest
          if(consumer.needToCallPoll.get()) {
            LOG.info("Discarding cached and uncommitted messages retrieved from Kafka due to need to call poll().");
            consumer.recordQueue.clear();
            return;
          }
          if (!putConsumerRecord(r)) {
            return;
          }
        }
      } catch (WakeupException e) {
        // Ignore exception if closing
        if (!closed.get()) {
          throw e;
        }
      } catch (Exception e) {
        if(!isErrorReported || countOfErrorRecords > 1000){
          isErrorReported = true;
          countOfErrorRecords = 0;
          LOG.error(String.format("%s catch trying to serialize Kafka Record: %s", e.getClass().toString(),
              e.getLocalizedMessage()));
          context.reportError(e);
        }
        countOfErrorRecords++;
      }
    }

    // Shutdown hook which can be called from a separate thread
    public void shutdown() {
      closed.set(true);
      consumer.kafkaConsumer.wakeup();
    }

    private boolean putConsumerRecord(ConsumerRecord<Object, byte[]> record) {
      try {
        consumer.recordQueue.put(record);
        isErrorReported = false;
        countOfErrorRecords = 0;
        return true;
      } catch (InterruptedException e) {
        LOG.error("Failed to poll KafkaConsumer, reason : {}", e.toString(), e);
        Thread.currentThread().interrupt();
        return false;
      }
    }
  }

  public Map<TopicPartition, OffsetAndMetadata> getTopicPartitionToOffsetMetadataMap() {
    return Collections.unmodifiableMap(topicPartitionToOffsetMetadataMap);
  }
}
