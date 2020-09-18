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
import com.streamsets.datacollector.security.kafka.KafkaKerberosUtil;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BasePushSource;
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
import com.streamsets.pipeline.lib.kafka.KafkaSecurityUtil;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.stage.origin.multikafka.loader.KafkaConsumerLoader;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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
  private final AtomicBoolean shutdownCalled = new AtomicBoolean(false);
  private int batchSize;

  private DataParserFactory parserFactory;
  private ExecutorService executor;

  private KafkaKerberosUtil kafkaKerberosUtil;
  private String keytabFileName;

  public MultiKafkaSource(MultiKafkaBeanConfig conf) {
    this.conf = conf;
    batchSize = conf.maxBatchSize;
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

    SdcKafkaValidationUtil kafkaValidationUtil = SdcKafkaValidationUtilFactory.getInstance().create();

    KafkaSecurityUtil.addSecurityConfigs(conf.connectionConfig.connection.securityConfig, conf.kafkaOptions);

    if (conf.connectionConfig.connection.securityConfig.provideKeytab && kafkaValidationUtil.isProvideKeytabAllowed(issues, getContext())) {
      keytabFileName = kafkaKerberosUtil.saveUserKeytab(
          conf.connectionConfig.connection.securityConfig.userKeytab.get(),
          conf.connectionConfig.connection.securityConfig.userPrincipal,
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
      event.setSpecificAttribute(LineageSpecificAttribute.DESCRIPTION, conf.connectionConfig.connection.metadataBrokerList);
      getContext().publishLineageEvent(event);
    }
    return issues;
  }

  @Override
  public int getNumberOfThreads() {
    return conf.numberOfThreads;
  }

  @Override
  public void produce(Map<String, String> lastOffsets, int maxBatchSize) {
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
        MultiSdcKafkaConsumer<String, byte[]> consumer = KafkaConsumerLoader.createConsumer(getKafkaProperties(context),
            context,
            conf.kafkaAutoOffsetReset,
            conf.timestampToSearchOffsets,
            conf.topicList
        );
        futures.add(executor.submit(new KafkaMultitopicRunnable(i,
            consumer,
            startProcessingGate,
            getContext(),
            parserFactory,
            conf,
            batchSize
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

    props.setProperty("bootstrap.servers", conf.connectionConfig.connection.metadataBrokerList);
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
