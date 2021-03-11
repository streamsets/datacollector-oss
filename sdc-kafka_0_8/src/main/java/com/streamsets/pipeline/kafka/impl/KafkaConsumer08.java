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

import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.kafka.api.KafkaOriginGroups;
import com.streamsets.pipeline.kafka.api.MessageAndOffset;
import com.streamsets.pipeline.kafka.api.SdcKafkaConsumer;
import com.streamsets.pipeline.lib.kafka.KafkaAutoOffsetReset;
import com.streamsets.pipeline.lib.kafka.KafkaErrors;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaConsumer08 implements SdcKafkaConsumer {

  public static final String CONSUMER_TIMEOUT_KEY = "consumer.timeout.ms";
  private static final String SOCKET_TIMEOUT_KEY = "socket.timeout.ms";
  private static final String SOCKET_TIMEOUT_DEFAULT = "30000";
  public static final String AUTO_COMMIT_ENABLED_KEY = "auto.commit.enable";
  public static final String AUTO_COMMIT_ENABLED_DEFAULT = "false";
  public static final String ZOOKEEPER_CONNECT_KEY = "zookeeper.connect";
  public static final String GROUP_ID_KEY = "group.id";
  private static final String FETCH_MAX_WAIT_KEY = "fetch.wait.max.ms";
  private static final String FETCH_MAX_WAIT_DEFAULT = "1000";
  private static final String FETCH_MIN_BYTES_KEY = "fetch.min.bytes";
  private static final String FETCH_MIN_BYTES_DEFAULT = "1";
  private static final String ZK_CONNECTION_TIMEOUT_MS_KEY = "zookeeper.connection.timeout.ms";
  private static final String ZK_SESSION_TIMEOUT_MS_KEY = "zookeeper.session.timeout.ms";
  private static final String ZK_CONNECTION_TIMEOUT_MS_DEFAULT = "6000";
  private static final String ZK_SESSION_TIMEOUT_MS_DEFAULT = "6000";
  private static final String AUTO_OFFSET_RESET_KEY = "auto.offset.reset";
  private static final String AUTO_OFFSET_RESET_PREVIEW = "smallest";

  public static final String KAFKA_CONFIG_BEAN_PREFIX = "kafkaConfigBean.";
  public static final String KAFKA_AUTO_OFFSET_RESET = "kafkaAutoOffsetReset";

  private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumer08.class);

  private ConsumerConnector consumer;
  private ConsumerIterator<byte[],byte[]> consumerIterator;
  private KafkaStream<byte[], byte[]> stream;

  private final String zookeeperConnect;
  private final String topic;
  private final int maxWaitTime;
  private final Source.Context context;
  private final Map<String, Object> kafkaConsumerConfigs;
  private final String consumerGroup;
  private ConsumerConfig consumerConfig;
  private String kafkaAutoOffsetReset;

  public KafkaConsumer08(String zookeeperConnect, String topic, String consumerGroup,
                         int consumerTimeout, Map<String, Object> kafkaConsumerConfigs,
                         Source.Context context, String kafkaAutoOffsetReset) {
    this.topic = topic;
    this.maxWaitTime = consumerTimeout;
    this.kafkaConsumerConfigs = kafkaConsumerConfigs;
    this.zookeeperConnect = zookeeperConnect;
    this.consumerGroup = consumerGroup;
    this.context = context;
    this.kafkaAutoOffsetReset = kafkaAutoOffsetReset;
  }

  @Override
  public void validate(List<Stage.ConfigIssue> issues, Stage.Context context) {
    validateKafkaTimestamp(issues);
    if (issues.isEmpty()) {
      Properties props = new Properties();
      configureKafkaProperties(props);
      LOG.debug("Creating Kafka Consumer with properties {}" , props.toString());
      consumerConfig = new ConsumerConfig(props);
      try {
        createConsumer(issues, context);
      } catch (StageException ex) {
        issues.add(context.createConfigIssue(null, null, KafkaErrors.KAFKA_10, ex.toString()));
      }
    }
  }

  @Override
  public void init() throws StageException {
    if(consumer == null) {
      Properties props = new Properties();
      configureKafkaProperties(props);
      LOG.debug("Creating Kafka Consumer with properties {}", props.toString());
      consumerConfig = new ConsumerConfig(props);
      createConsumer();
    }
  }

  @Override
  public void destroy() {
    if(consumer != null) {
      try {
        consumer.shutdown();
      } catch (Exception e) {
        LOG.error("Error shutting down Kafka Consumer, reason: {}", e.toString(), e);
      }
    }
  }

  @Override
  public void commit() {
    consumer.commitOffsets();
  }

  @Override
  public MessageAndOffset read() throws StageException {
    try {
      //has next blocks indefinitely if consumer.timeout.ms is set to -1
      //But if consumer.timeout.ms is set to a value, like 6000, a ConsumerTimeoutException is thrown
      //if no message is written to kafka topic in that time.
      if(consumerIterator.hasNext()) {
        MessageAndMetadata<byte[], byte[]> messageAndMetadata = consumerIterator.next();
        byte[] message = messageAndMetadata.message();
        long offset = messageAndMetadata.offset();
        int partition = messageAndMetadata.partition();
        return new MessageAndOffset(messageAndMetadata.key(), message, offset, partition);
      }
      return null;
    } catch (ConsumerTimeoutException e) {
      /*For high level consumer the fetching logic is handled by a background
        fetcher thread and is hidden from user, for either case of
        1) broker down or
        2) no message is available
        the fetcher thread will keep retrying while the user thread will wait on the fetcher thread to put some
        data into the buffer until timeout. So in a sentence the high-level consumer design is to
        not let users worry about connect / reconnect issues.*/
      return null;
    }
  }

  @Override
  public String getVersion() {
    return Kafka08Constants.KAFKA_VERSION;
  }

  private void createConsumer() throws StageException {
    createConsumer(null, null);
  }

  private void createConsumer(List<Stage.ConfigIssue> issues, Stage.Context context) throws StageException {
    LOG.debug("Creating consumer with configuration {}", consumerConfig.props().props().toString());
    try {
      consumer = Consumer.createJavaConsumerConnector(consumerConfig);
    } catch (Exception e) {
      if(issues != null) {
        issues.add(context.createConfigIssue(KafkaOriginGroups.KAFKA.name(), "zookeeperConnect",
          KafkaErrors.KAFKA_31, zookeeperConnect, e.toString()));
        return;
      } else {
        LOG.error(KafkaErrors.KAFKA_31.getMessage(), zookeeperConnect, e.toString(), e);
        throw new StageException(KafkaErrors.KAFKA_31, zookeeperConnect, e.toString(), e);
      }
    }

    Map<String, Integer> topicCountMap = new HashMap<>();

    //TODO: Create threads equal to the number of partitions for this topic
    // A known way to find number of partitions is by connecting to a known kafka broker which means the user has to
    // supply additional options - known broker host and port.
    //Another option is have the user specify the number of threads. If there are more threads than there are
    // partitions, some threads will never see a message
    topicCountMap.put(topic, 1);

    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
      consumer.createMessageStreams(topicCountMap);
    List<KafkaStream<byte[], byte[]>> topicList = consumerMap.get(topic);
    stream = topicList.get(0);
    try {
      consumerIterator = stream.iterator();
    } catch (Exception e) {
      LOG.error(KafkaErrors.KAFKA_32.getMessage(), e.toString(), e);
      throw new StageException(KafkaErrors.KAFKA_32, e.toString(), e);
    }
  }

  private void configureKafkaProperties(Properties props) {

    props.put(ZOOKEEPER_CONNECT_KEY, zookeeperConnect);
    props.put(GROUP_ID_KEY, consumerGroup);
    /*The maximum amount of time the server will block before answering the fetch request if there isn't sufficient
    data to immediately satisfy fetch.min.bytes*/
    props.put(FETCH_MAX_WAIT_KEY, FETCH_MAX_WAIT_DEFAULT);

    /*Throw a timeout exception to the consumer if no message is available for consumption after the specified
    interval*/
    props.put(CONSUMER_TIMEOUT_KEY, String.valueOf(maxWaitTime));
    /*The socket timeout for network requests.*/
    props.put(SOCKET_TIMEOUT_KEY, SOCKET_TIMEOUT_DEFAULT);
    /*The minimum amount of data the server should return for a fetch request. If insufficient data is available the
    request will wait for that much data to accumulate before answering the request.*/
    props.put(FETCH_MIN_BYTES_KEY, FETCH_MIN_BYTES_DEFAULT);
    /*sdc will commit offset after writing the batch to target*/
    props.put(AUTO_COMMIT_ENABLED_KEY, AUTO_COMMIT_ENABLED_DEFAULT);

    props.put(ZK_CONNECTION_TIMEOUT_MS_KEY, ZK_CONNECTION_TIMEOUT_MS_DEFAULT);
    props.put(ZK_SESSION_TIMEOUT_MS_KEY, ZK_SESSION_TIMEOUT_MS_DEFAULT);

    props.put(AUTO_OFFSET_RESET_KEY, kafkaAutoOffsetReset.toLowerCase());

    if (this.context.isPreview()) {
      // Set to smallest value only for preview mode
      // When running actual pipeline, the kafkaAutoOffsetReset config from kafka consumer stage should be picked up.
      props.put(AUTO_OFFSET_RESET_KEY, AUTO_OFFSET_RESET_PREVIEW);
    }

    addUserConfiguredProperties(props);
  }

  private void addUserConfiguredProperties(Properties props) {
    //The following options, if specified, are ignored :
    if(kafkaConsumerConfigs != null && !kafkaConsumerConfigs.isEmpty()) {
      kafkaConsumerConfigs.remove(ZOOKEEPER_CONNECT_KEY);
      kafkaConsumerConfigs.remove(GROUP_ID_KEY);
      kafkaConsumerConfigs.remove(AUTO_COMMIT_ENABLED_KEY);
      kafkaConsumerConfigs.remove(CONSUMER_TIMEOUT_KEY);
      kafkaConsumerConfigs.remove(AUTO_OFFSET_RESET_KEY);

      for (Map.Entry<String, Object> producerConfig : kafkaConsumerConfigs.entrySet()) {
        props.put(producerConfig.getKey(), producerConfig.getValue());
      }
    }
  }

  private void validateKafkaTimestamp(List<Stage.ConfigIssue> issues) {
    if(KafkaAutoOffsetReset.TIMESTAMP.name().equals(kafkaAutoOffsetReset)) {
      issues.add(context.createConfigIssue(KafkaOriginGroups.KAFKA.name(),
          KAFKA_CONFIG_BEAN_PREFIX + KAFKA_AUTO_OFFSET_RESET,
          KafkaErrors.KAFKA_76
      ));
    }
  }

}
