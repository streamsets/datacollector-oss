/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.kafka;

import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.Errors;
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

public class KafkaConsumer {

  public static final String CONSUMER_TIMEOUT_KEY = "consumer.timeout.ms";
  private static final String SOCKET_TIMEOUT_KEY = "socket.timeout.ms";
  private static final String SOCKET_TIMEOUT_DEFAULT = "30000";
  public static final String AUTO_COMMIT_ENABLED_KEY = "auto.commit.enable";
  public static final String AUTO_COMMIT_ENABLED_DEFAULT = "false";
  public static final String ZOOKEEPER_CONNECT_KEY = "zookeeper.connect";
  public static final String GROUP_ID_KEY = "group.id";
  private static final String FETCH_MAX_WAIT_KEY = "fetch.wait.max.ms";
  private static final String FETCH_MAX_WAIT_DEFAULT = "100";
  private static final String FETCH_MIN_BYTES_KEY = "fetch.min.bytes";
  private static final String FETCH_MIN_BYTES_DEFAULT = "1";
  private static final String ZK_CONNECTION_TIMEOUT_MS_KEY = "zookeeper.connection.timeout.ms";
  private static final String ZK_SESSION_TIMEOUT_MS_KEY = "zookeeper.session.timeout.ms";
  private static final String ZK_CONNECTION_TIMEOUT_MS_DEFAULT = "6000";
  private static final String ZK_SESSION_TIMEOUT_MS_DEFAULT = "6000";

  private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumer.class);

  private ConsumerConnector consumer;
  private ConsumerIterator<byte[],byte[]> consumerIterator;
  private KafkaStream<byte[], byte[]> stream;

  private final String zookeeperConnect;
  private final String topic;
  private final int maxBatchSize;
  private final int maxWaitTime;
  private final Source.Context context;
  private final Map<String, String> kafkaConsumerConfigs;
  private final String consumerGroup;
  private ConsumerConfig consumerConfig;

  public KafkaConsumer(String zookeeperConnect, String topic, String consumerGroup, int batchUpperLimit,
                       int consumerTimeout, Map<String, String> kafkaConsumerConfigs,
                       Source.Context context) {
    this.topic = topic;
    this.maxBatchSize = batchUpperLimit;
    this.maxWaitTime = consumerTimeout;
    this.kafkaConsumerConfigs = kafkaConsumerConfigs;
    this.zookeeperConnect = zookeeperConnect;
    this.consumerGroup = consumerGroup;
    this.context = context;
  }

  public void validate(List<Stage.ConfigIssue> issues, Stage.Context context) throws StageException {
    Properties props = new Properties();
    configureKafkaProperties(props);
    LOG.debug("Creating Kafka Consumer with properties {}" , props.toString());
    consumerConfig = new ConsumerConfig(props);
    createConsumer(issues, context);
  }

  public void init() throws StageException {
    if(consumer == null) {
      Properties props = new Properties();
      configureKafkaProperties(props);
      LOG.debug("Creating Kafka Consumer with properties {}", props.toString());
      consumerConfig = new ConsumerConfig(props);
      createConsumer();
    }
  }

  public void destroy() {
    if(consumer != null) {
      try {
        consumer.shutdown();
      } catch (Exception e) {
        LOG.error("Error shutting down Kafka Consumer, reason: {}", e.getMessage(), e);
      }
    }
  }

  public void commit() {
    consumer.commitOffsets();
  }

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
        MessageAndOffset partitionToPayloadMap = new MessageAndOffset(message, offset, partition);
        return partitionToPayloadMap;
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

  private void createConsumer() throws StageException {
    createConsumer(null, null);
  }

  private void createConsumer(List<Stage.ConfigIssue> issues, Stage.Context context) throws StageException {
    LOG.debug("Creating consumer with configuration {}", consumerConfig.props().props().toString());
    try {
      consumer = Consumer.createJavaConsumerConnector(consumerConfig);
    } catch (Exception e) {
      if(issues != null) {
        issues.add(context.createConfigIssue(Groups.KAFKA.name(), "zookeeperConnect",
          Errors.KAFKA_31, zookeeperConnect, e.getMessage()));
        return;
      } else {
        LOG.error(Errors.KAFKA_31.getMessage(), zookeeperConnect, e.getMessage(), e);
        throw new StageException(Errors.KAFKA_31, zookeeperConnect, e.getMessage(), e);
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
      LOG.error(Errors.KAFKA_32.getMessage(), e.getMessage(), e);
      throw new StageException(Errors.KAFKA_32, e.getMessage(), e);
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

    addUserConfiguredProperties(props);
  }

  private void addUserConfiguredProperties(Properties props) {
    //The following options, if specified, are ignored :
    if(kafkaConsumerConfigs != null && !kafkaConsumerConfigs.isEmpty()) {
      kafkaConsumerConfigs.remove(ZOOKEEPER_CONNECT_KEY);
      kafkaConsumerConfigs.remove(GROUP_ID_KEY);
      kafkaConsumerConfigs.remove(AUTO_COMMIT_ENABLED_KEY);
      kafkaConsumerConfigs.remove(CONSUMER_TIMEOUT_KEY);

      for (Map.Entry<String, String> producerConfig : kafkaConsumerConfigs.entrySet()) {
        props.put(producerConfig.getKey(), producerConfig.getValue());
      }
    }
  }

}
