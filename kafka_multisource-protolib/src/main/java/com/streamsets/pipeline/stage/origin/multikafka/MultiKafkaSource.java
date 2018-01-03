/**
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
package com.streamsets.pipeline.stage.origin.multikafka;

import com.google.common.base.Throwables;
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BasePushSource;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.kafka.KafkaConstants;
import com.streamsets.pipeline.lib.kafka.KafkaErrors;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
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

  private final MultiKafkaBeanConfig conf;
  private AtomicBoolean shutdownCalled = new AtomicBoolean(false);
  private int batchSize;

  private DataParserFactory parserFactory;
  private ExecutorService executor;

  public MultiKafkaSource(MultiKafkaBeanConfig conf) {
    this.conf = conf;
    batchSize = conf.maxBatchSize;
  }

  public class MultiTopicCallable implements Callable<Long> {
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
      Thread.currentThread().setName("kafkaConsumerThread-"+threadID);
      long messagesProcessed = 0;

      //wait until all threads are spun up before processing
      LOG.debug("Thread {} waiting on other consumer threads to start up", Thread.currentThread().getName());
      startProcessingGate.await();

      LOG.debug("Starting poll loop in thread {}", Thread.currentThread().getName());
      try {
        consumer.subscribe(topicList);

        // protected loop. want it to finish completely, or not start at all.
        // only 2 conditions that we want to halt execution. must handle gracefully
        while(!getContext().isStopped() && !Thread.interrupted()) {
          BatchContext batchContext = getContext().startBatch();
          ErrorRecordHandler errorRecordHandler = new DefaultErrorRecordHandler(getContext(), batchContext);

          ConsumerRecords<String, byte[]> messages = consumer.poll(conf.batchWaitTime);
          if(!messages.isEmpty()) {
            for(ConsumerRecord<String, byte[]> message : messages) {
              createRecord(
                  errorRecordHandler,
                  message.topic(),
                  message.partition(),
                  message.offset(),
                  message.value()
              ).forEach(batchContext.getBatchMaker()::addRecord);
            }

            getContext().processBatch(batchContext);
            messagesProcessed += messages.count();
            LOG.trace("Kafka thread {} finished processing {} messages", this.threadID, messages.count());
          }
        }
      } catch (Exception e) {
        LOG.error("Encountered error in multi kafka thread {} during read {}", threadID, e);
        handleException(KafkaErrors.KAFKA_29, e);
      } finally {
        consumer.unsubscribe();
        consumer.close();
      }

      LOG.info("multi kafka thread {} consumed {} messages", threadID, messagesProcessed);
      return messagesProcessed;
    }

    private List<Record> createRecord(
      ErrorRecordHandler errorRecordHandler,
      String topic,
      int partition,
      long offset,
      byte[] payload
    ) throws StageException {
      String messageId = getMessageId(topic, partition, offset);
      List<Record> records = new ArrayList<>();
      try(DataParser parser = Utils.checkNotNull(parserFactory, "Initialization failed").getParser(messageId, payload)) {
        Record record = parser.parse();
        while (record != null) {
          record.getHeader().setAttribute(HeaderAttributeConstants.TOPIC, topic);
          record.getHeader().setAttribute(HeaderAttributeConstants.PARTITION, String.valueOf(partition));
          record.getHeader().setAttribute(HeaderAttributeConstants.OFFSET, String.valueOf(offset));

          records.add(record);
          record = parser.parse();
        }
      } catch (DataParserException | IOException e) {
        Record record = getContext().createRecord(messageId);
        record.set(Field.create(payload));
        errorRecordHandler.onError(
            new OnRecordErrorException(
                record,
                KafkaErrors.KAFKA_37,
                messageId,
                e.toString(),
                e
            )
        );
      }
      if(conf.produceSingleRecordPerMessage) {
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

    conf.init(getContext(), issues);

    conf.dataFormatConfig.stringBuilderPoolSize = getNumberOfThreads();

    if(issues.isEmpty()) {
      conf.dataFormatConfig.init(getContext(),
          conf.dataFormat,
          Groups.KAFKA.name(),
          MULTI_KAFKA_DATA_FORMAT_CONFIG_PREFIX,
          issues
      );

    }

    parserFactory = conf.dataFormatConfig.getParserFactory();

    if (conf.dataFormat == DataFormat.XML && conf.produceSingleRecordPerMessage) {
      issues.add(
          getContext().createConfigIssue(
              Groups.KAFKA.name(),
              MULTI_KAFKA_DATA_FORMAT_CONFIG_PREFIX + "produceSingleRecordPerMessage",
              KafkaErrors.KAFKA_40
          )
      );
    }

    executor = Executors.newFixedThreadPool(getNumberOfThreads());

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
    int numThreads = getNumberOfThreads();
    List<Future<Long>> futures = new ArrayList<>(numThreads);
    CountDownLatch startProcessingGate = new CountDownLatch(numThreads);

    // Run all the threads
    for(int i = 0; i < numThreads; i++) {
      try {
        futures.add(executor.submit(new MultiTopicCallable(i,
            conf.topicList,
            KafkaConsumerLoader.createConsumer(getKafkaProperties(getContext())),
            startProcessingGate
        )));
      } catch (Exception e) {
        LOG.error("Error while initializing Kafka consumer: {}", e.toString(), e);
        Throwables.propagateIfPossible(e.getCause(), StageException.class);
        Throwables.propagate(e);
      }
      startProcessingGate.countDown();
    }

    // Wait for proper execution completion
    long totalMessagesProcessed = 0;
    for(Future<Long> future : futures) {
      try {
        totalMessagesProcessed += future.get();
      } catch (InterruptedException e) {
        // all threads should stop if the main thread is interrupted
        shutdown();
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        LOG.info("Multi kafka thread halted unexpectedly: {}", future, e.getCause().getMessage(), e);
        shutdown();
        Throwables.propagateIfPossible(e.getCause(), StageException.class);
        Throwables.propagate(e.getCause());
      }
    }

    LOG.info("Total messages consumed by all threads: {}", totalMessagesProcessed);
    executor.shutdown();
  }

  //no trespassing...
  private Properties getKafkaProperties(Stage.Context context) {
    Properties props = new Properties();
    props.putAll(conf.kafkaOptions);

    props.setProperty("bootstrap.servers", conf.brokerURI);
    props.setProperty("group.id", conf.consumerGroup);
    props.setProperty("max.poll.records", String.valueOf(batchSize));
    props.setProperty("enable.auto.commit", "true");
    props.setProperty("auto.commit.interval.ms", "1000");
    props.setProperty(KafkaConstants.KEY_DESERIALIZER_CLASS_CONFIG, conf.keyDeserializer.getKeyClass());
    props.setProperty(KafkaConstants.VALUE_DESERIALIZER_CLASS_CONFIG, conf.valueDeserializer.getValueClass());
    props.setProperty(KafkaConstants.CONFLUENT_SCHEMA_REGISTRY_URL_CONFIG, StringUtils.join(conf.dataFormatConfig.schemaRegistryUrls, ","));

    if(context.isPreview()) {
      props.setProperty(KafkaConstants.AUTO_OFFSET_RESET_CONFIG, KafkaConstants.AUTO_OFFSET_RESET_PREVIEW_VALUE);
    }

    return props;
  }

  private String getMessageId(String topic, int partition, long offset) {
    return topic + "::" + partition + "::" + offset;
  }

  public void await() throws InterruptedException {
    if(executor != null) {
      executor.awaitTermination(30, TimeUnit.SECONDS);
    }
  }

  public boolean isRunning() {
    if(executor == null) {
      return false;
    }

    return !executor.isShutdown() && !executor.isTerminated();
  }

  @Override
  public void destroy() {
    executor.shutdownNow();
    super.destroy();
  }

  private void shutdown() {
    if (!shutdownCalled.getAndSet(true)) {
      executor.shutdownNow();
    }
  }
}
