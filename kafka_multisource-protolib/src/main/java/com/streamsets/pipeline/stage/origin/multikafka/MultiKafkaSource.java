/**
 * Copyright 2017 StreamSets Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.multikafka;

import com.google.common.base.Throwables;
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.DeliveryGuarantee;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ToErrorContext;
import com.streamsets.pipeline.api.base.BasePushSource;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.lineage.EndPointType;
import com.streamsets.pipeline.api.lineage.LineageEvent;
import com.streamsets.pipeline.api.lineage.LineageEventType;
import com.streamsets.pipeline.api.lineage.LineageSpecificAttribute;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.kafka.api.SdcKafkaValidationUtil;
import com.streamsets.pipeline.kafka.api.SdcKafkaValidationUtilFactory;
import com.streamsets.pipeline.lib.kafka.KafkaConstants;
import com.streamsets.pipeline.lib.kafka.KafkaErrors;
import com.streamsets.datacollector.security.kafka.KafkaKerberosUtil;
import com.streamsets.pipeline.lib.kafka.MessageKeyUtil;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;
import com.streamsets.pipeline.stage.origin.multikafka.loader.KafkaConsumerLoader;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class MultiKafkaSource extends BasePushSource {

  private static final Logger LOG = LoggerFactory.getLogger(MultiKafkaSource.class);

  private static final String MULTI_KAFKA_DATA_FORMAT_CONFIG_PREFIX = "dataFormatConfig.";
  private static final String TOPIC_PARTITION_SEPARATOR = "#";

  private final MultiKafkaBeanConfig conf;
  private AtomicBoolean shutdownCalled = new AtomicBoolean(false);
  private int batchSize;

  private DataParserFactory parserFactory;
  private ExecutorService executor;

  private SdcKafkaValidationUtil kafkaValidationUtil;
  private KafkaKerberosUtil kafkaKerberosUtil;
  private String keytabFileName;

  public MultiKafkaSource(MultiKafkaBeanConfig conf) {
    this.conf = conf;
    batchSize = conf.maxBatchSize;
  }

  /**
   * Wrapper around DefaultErrorRecordHandler that can count number of passed error records as that is relevant to the
   * logic we have in this origin.
   */
  public static class CountingDefaultErrorRecordHandler implements ErrorRecordHandler {

    private final ErrorRecordHandler delegate;
    private int errorRecordCount = 0;

    public CountingDefaultErrorRecordHandler(Stage.Context context, ToErrorContext toError) {
      this.delegate = new DefaultErrorRecordHandler(context, toError);
    }

    public int getErrorRecordCount() {
      return errorRecordCount;
    }

    @Override
    public void onError(ErrorCode errorCode, Object... params) throws StageException {
      delegate.onError(errorCode, params);
    }

    @Override
    public void onError(OnRecordErrorException error) throws StageException {
      errorRecordCount++;
      delegate.onError(error);
    }

    @Override
    public void onError(List<Record> batch, StageException error) throws StageException {
      errorRecordCount += batch.size();
      delegate.onError(batch, error);
    }
  }

  public class MultiTopicCallable implements Callable<Long> {

    // keep it same as BaseKafkaConsumer09.CONSUMER_POLLING_WINDOW_MS for now
    // TODO - Evaluate if this needs to be reduced to 10 ms
    private static final long MIN_CONSUMER_POLLING_INTERVAL_MS = 100;
    private MultiSdcKafkaConsumer<String, byte[]> consumer;
    private final long threadID;
    private final List<String> topicList;
    private final CountDownLatch startProcessingGate;

    public MultiTopicCallable(
        long threadID,
        List<String> topicList,
        MultiSdcKafkaConsumer<String, byte[]> consumer,
        CountDownLatch startProcessingGate
    ) {
      this.consumer = consumer;
      this.threadID = threadID;
      this.topicList = topicList;
      this.startProcessingGate = startProcessingGate;
    }

    @Override
    public Long call() throws Exception {
      Thread.currentThread().setName("kafkaConsumerThread-" + threadID);
      long messagesProcessed = 0;
      long recordsProcessed = 0;
      BatchContext batchContext = null;
      CountingDefaultErrorRecordHandler errorRecordHandler = null;

      // wait until all threads are spun up before processing
      LOG.debug("Thread {} waiting on other consumer threads to start up", Thread.currentThread().getName());
      startProcessingGate.await();

      LOG.debug("Starting poll loop in thread: {}", Thread.currentThread().getName());

      LOG.info("Minimum Kafka consumer Poll interval is set to: {}", MIN_CONSUMER_POLLING_INTERVAL_MS);
      try {
        consumer.subscribe(topicList);
        // protected loop. want it to finish completely, or not start at all.
        // only 2 conditions that we want to halt execution. must handle gracefully

        List<Record> records = new ArrayList<>();
        long startTime = System.currentTimeMillis();

        Map<String, Long> offsetIncrements = new HashMap<>();
        Map<String, Long> recordsSinceLastCommit = new HashMap<>();

        while (!getContext().isStopped() && !Thread.interrupted()) {

          // Xavi's awesome Deadman timer.
          long pollInterval = Math.max(MIN_CONSUMER_POLLING_INTERVAL_MS, conf.batchWaitTime - (System.currentTimeMillis() - startTime));

          ConsumerRecords<String, byte[]> messages = consumer.poll(pollInterval);

          for (ConsumerRecord<String, byte[]> item : messages) {
            String topicPartitionName = item.topic() + TOPIC_PARTITION_SEPARATOR + item.partition();
            recordsSinceLastCommit.putIfAbsent(topicPartitionName, 0L);
            recordsSinceLastCommit.put(topicPartitionName, recordsSinceLastCommit.get(topicPartitionName) + 1);
          }

          if (!messages.isEmpty()) {

            // start a new batch, get a new error record handler for batch.
            batchContext = getContext().startBatch();
            errorRecordHandler = new CountingDefaultErrorRecordHandler(getContext(), batchContext);
          }

          for (ConsumerRecord<String, byte[]> item : messages) {

            // We still support Kafka 0.9 that doesn't have support for timestamp. Thus this code simply calls those
            // methods in a safe manner and fills defaults in case that those methods do not exists. This fragment can
            // be dropped (or this patch reverted) when we drop support for Kafka 0.9.
            long timestamp;
            String timestampType;
            try {
              timestamp = item.timestamp();
              timestampType = item.timestampType().name;
            } catch (NoSuchMethodError _) {
              timestamp = -1;
              timestampType = "";
            }

            records.addAll(createRecord(errorRecordHandler,
                item.topic(),
                item.partition(),
                item.offset(),
                item.value(),
                item.key(),
                timestamp,
                timestampType
            ));

            String topicPartitionName = item.topic() + TOPIC_PARTITION_SEPARATOR + item.partition();
            offsetIncrements.putIfAbsent(topicPartitionName, 0L);
            offsetIncrements.put(topicPartitionName, offsetIncrements.get(topicPartitionName) + 1);

            if (records.size() >= batchSize) {
              records.forEach(batchContext.getBatchMaker()::addRecord);
              Map<TopicPartition, OffsetAndMetadata> offsets = getNewOffsets(offsetIncrements, recordsSinceLastCommit);
              offsetIncrements.clear();
              commitSyncAndProcess(batchContext, offsets);
              recordsSinceLastCommit.clear();
              startTime = System.currentTimeMillis();
              recordsProcessed += records.size();
              records.clear();
              batchContext = getContext().startBatch();
              errorRecordHandler = new CountingDefaultErrorRecordHandler(getContext(), batchContext);
            }
          }

          messagesProcessed += messages.count();

          // Transmit remaining when batchWaitTime expired
          if (pollInterval <= MIN_CONSUMER_POLLING_INTERVAL_MS) {
            startTime = System.currentTimeMillis();
            if (!records.isEmpty() || (errorRecordHandler != null && errorRecordHandler.getErrorRecordCount() > 0)) {
              records.forEach(batchContext.getBatchMaker()::addRecord);
              Map<TopicPartition, OffsetAndMetadata> offsets = getNewOffsets(offsetIncrements, recordsSinceLastCommit);
              offsetIncrements.clear();
              commitSyncAndProcess(batchContext, offsets);
              recordsSinceLastCommit.clear();
              batchContext = getContext().startBatch();
              errorRecordHandler = new CountingDefaultErrorRecordHandler(getContext(), batchContext);
              recordsProcessed += records.size();
              records.clear();
            }
          }

        }

      } catch (Exception e) {
        LOG.error("Encountered error in multi kafka thread {} during read {}", threadID, e.getMessage(), e);
        handleException(KafkaErrors.KAFKA_29, e);
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

    @NotNull
    private Map<TopicPartition, OffsetAndMetadata> getNewOffsets(
        Map<String, Long> offsetIncrements,
        Map<String, Long> recordsSinceLastCommit
    ) {
      Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
      for (Map.Entry<String, Long> offsetIncrement : offsetIncrements.entrySet()) {
        String[] topicAndPartition = offsetIncrement.getKey().split(TOPIC_PARTITION_SEPARATOR);
        TopicPartition topicPartition = new TopicPartition(
            topicAndPartition[0],
            Integer.parseInt(topicAndPartition[1])
        );
        long commitOffset;
        Long committedOffset = consumer.getCommittedOffset(topicPartition);
        if (committedOffset == null) {
          commitOffset = consumer.getOffset(topicPartition) - recordsSinceLastCommit.get(offsetIncrement.getKey()) + offsetIncrement.getValue();
        } else {
          commitOffset = committedOffset + offsetIncrement.getValue();
        }
        offsets.put(topicPartition, new OffsetAndMetadata(commitOffset));
      }
      return offsets;
    }

    private void commitSyncAndProcess(BatchContext batchContext, Map<TopicPartition, OffsetAndMetadata> offsets) {
      if (!getContext().isPreview() && getContext().getDeliveryGuarantee() == DeliveryGuarantee.AT_MOST_ONCE) {
        consumer.commitSync(offsets);
      }

      boolean batchSuccessful = getContext().processBatch(batchContext);

      if (!getContext().isPreview() && batchSuccessful && getContext().getDeliveryGuarantee() == DeliveryGuarantee.AT_LEAST_ONCE) {
        consumer.commitSync(offsets);
      }
    }

    private List<Record> createRecord(
        ErrorRecordHandler errorRecordHandler,
        String topic,
        int partition,
        long offset,
        byte[] payload,
        Object messageKey,
        long timestamp,
        String timestampType
    ) throws StageException {
      Utils.checkNotNull(parserFactory, "Initialization failed");

      String messageId = getMessageId(topic, partition, offset);
      List<Record> records = new ArrayList<>();

      try (DataParser parser = parserFactory.getParser(messageId, payload)) {
        Record record = parser.parse();

        while (record != null) {
          record.getHeader().setAttribute(HeaderAttributeConstants.TOPIC, topic);
          record.getHeader().setAttribute(HeaderAttributeConstants.PARTITION, String.valueOf(partition));
          record.getHeader().setAttribute(HeaderAttributeConstants.OFFSET, String.valueOf(offset));
          if (conf.timestampsEnabled) {
            record
                .getHeader()
                .setAttribute(HeaderAttributeConstants.KAFKA_TIMESTAMP, String.valueOf(timestamp));
            record
                .getHeader()
                .setAttribute(HeaderAttributeConstants.KAFKA_TIMESTAMP_TYPE, timestampType);
          }

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
          records.add(record);
          record = parser.parse();
        }
      } catch (DataParserException | IOException e) {
        Record record = getContext().createRecord(messageId);
        record.set(Field.create(payload));
        errorRecordHandler.onError(new OnRecordErrorException(
            record,
            KafkaErrors.KAFKA_37,
            messageId,
            e.toString(),
            e
        ));
      }

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

      return records;
    }

    private void handleException(KafkaErrors error, Object... args) throws StageException {
      // all threads should halt when an error is encountered
      shutdown();
      throw new StageException(error, args);
    }
  }

  @Override
  public List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    kafkaKerberosUtil = new KafkaKerberosUtil(getContext().getConfiguration());
    Utils.checkNotNull(kafkaKerberosUtil, "kafkaKerberosUtil");

    conf.init(getContext(), issues);

    conf.dataFormatConfig.stringBuilderPoolSize = getNumberOfThreads();

    if (issues.isEmpty()) {
      conf.dataFormatConfig.init(getContext(),
          conf.dataFormat,
          Groups.KAFKA.name(),
          MULTI_KAFKA_DATA_FORMAT_CONFIG_PREFIX,
          issues
      );
    }

    kafkaValidationUtil = SdcKafkaValidationUtilFactory.getInstance().create();

    if (conf.provideKeytab && kafkaValidationUtil.isProvideKeytabAllowed(issues, getContext())) {
      keytabFileName = kafkaKerberosUtil.saveUserKeytab(
          conf.userKeytab.get(),
          conf.userPrincipal,
          conf.kafkaOptions,
          issues,
          getContext()
      );
    }

    parserFactory = conf.dataFormatConfig.getParserFactory();

    if (conf.dataFormat == DataFormat.XML && conf.produceSingleRecordPerMessage) {
      issues.add(getContext().createConfigIssue(Groups.KAFKA.name(),
          MULTI_KAFKA_DATA_FORMAT_CONFIG_PREFIX + "produceSingleRecordPerMessage",
          KafkaErrors.KAFKA_40
      ));
    }

    executor = Executors.newFixedThreadPool(getNumberOfThreads());
    for (String topic : conf.topicList) {
      LineageEvent event = getContext().createLineageEvent(LineageEventType.ENTITY_READ);
      event.setSpecificAttribute(LineageSpecificAttribute.ENDPOINT_TYPE, EndPointType.KAFKA.name());
      event.setSpecificAttribute(LineageSpecificAttribute.ENTITY_NAME, topic);
      event.setSpecificAttribute(LineageSpecificAttribute.DESCRIPTION, conf.brokerURI);
      getContext().publishLineageEvent(event);
    }
    return issues;
  }

  @Override
  public int getNumberOfThreads() {
    return conf.numberOfThreads;
  }

  @Override
  public void produce(Map<String, String> lastOffsets, int maxBatchSize) throws StageException {
    shutdownCalled.set(false);
    batchSize = Math.min(maxBatchSize, conf.maxBatchSize);
    if (!getContext().isPreview() && conf.maxBatchSize > maxBatchSize) {
      getContext().reportError(KafkaErrors.KAFKA_78, maxBatchSize);
    }

    int numThreads = getNumberOfThreads();
    List<Future<Long>> futures = new ArrayList<>(numThreads);
    CountDownLatch startProcessingGate = new CountDownLatch(numThreads);

    // Run all the threads
    Stage.Context context = getContext();
    for (int i = 0; i < numThreads; i++) {
      try {
        MultiSdcKafkaConsumer consumer = KafkaConsumerLoader.createConsumer(getKafkaProperties(context),
            context,
            conf.kafkaAutoOffsetReset,
            conf.timestampToSearchOffsets,
            conf.topicList
        );
        futures.add(executor.submit(new MultiTopicCallable(i, conf.topicList, consumer, startProcessingGate)));
      } catch (Exception e) {
        LOG.error("Error while initializing Kafka consumer: {}", e.toString(), e);
        Throwables.propagateIfPossible(e.getCause(), StageException.class);
        Throwables.propagate(e);
      }
      startProcessingGate.countDown();
    }

    // Wait for proper execution completion
    long totalMessagesProcessed = 0;
    for (Future<Long> future : futures) {
      try {
        totalMessagesProcessed += future.get();
      } catch (InterruptedException e) {
        // all threads should stop if the main thread is interrupted
        shutdown();
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        LOG.info("Multi kafka thread halted unexpectedly: {}", e.getCause().getMessage(), e);
        shutdown();
        Throwables.propagateIfPossible(e.getCause(), StageException.class);
        Throwables.propagate(e.getCause());
      }
    }

    LOG.info("Total messages consumed by all threads: {}", totalMessagesProcessed);
    executor.shutdown();
  }

  // no trespassing...
  private Properties getKafkaProperties(Stage.Context context) {
    Properties props = new Properties();
    props.putAll(conf.kafkaOptions);

    props.setProperty("bootstrap.servers", conf.brokerURI);
    props.setProperty("group.id", conf.consumerGroup);
    props.setProperty("max.poll.records", String.valueOf(batchSize));
    props.setProperty(KafkaConstants.KEY_DESERIALIZER_CLASS_CONFIG, conf.keyDeserializer.getKeyClass());
    props.setProperty(KafkaConstants.VALUE_DESERIALIZER_CLASS_CONFIG, conf.valueDeserializer.getValueClass());
    props.setProperty(KafkaConstants.CONFLUENT_SCHEMA_REGISTRY_URL_CONFIG,
        StringUtils.join(conf.dataFormatConfig.schemaRegistryUrls, ",")
    );

    String userInfo = conf.dataFormatConfig.basicAuth.get();
    if (!userInfo.isEmpty()) {
      props.put(KafkaConstants.BASIC_AUTH_CREDENTIAL_SOURCE, KafkaConstants.USER_INFO);
      props.put(KafkaConstants.BASIC_AUTH_USER_INFO, userInfo);
    }

    props.setProperty(KafkaConstants.AUTO_COMMIT_OFFEST, "false");

    if (context.isPreview()) {
      props.setProperty(KafkaConstants.AUTO_OFFSET_RESET_CONFIG, KafkaConstants.AUTO_OFFSET_RESET_PREVIEW_VALUE);
    }

    return props;
  }

  private String getMessageId(String topic, int partition, long offset) {
    return topic + "::" + partition + "::" + offset;
  }

  public void await() throws InterruptedException {
    if (executor != null) {
      executor.awaitTermination(30, TimeUnit.SECONDS);
    }
  }

  public boolean isRunning() {
    if (executor == null) {
      return false;
    }

    return !executor.isShutdown() && !executor.isTerminated();
  }

  @Override
  public void destroy() {
    super.destroy();
    kafkaKerberosUtil.deleteUserKeytabIfExists(keytabFileName, getContext());
    executor.shutdownNow();
  }

  private void shutdown() {
    if (!shutdownCalled.getAndSet(true)) {
      executor.shutdownNow();
    }
  }
}
