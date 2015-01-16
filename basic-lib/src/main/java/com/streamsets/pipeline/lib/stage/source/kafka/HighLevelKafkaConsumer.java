/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.kafka;

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.util.StageLibError;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class HighLevelKafkaConsumer {

  public static final String CONSUMER_TIMEOUT_KEY = "consumer.timeout.ms";
  public static final String CONSUMER_TIMEOUT_DEFAULT = "-1";
  public static final String AUTO_COMMIT_ENABLED_KEY = "auto.commit.enable";
  public static final String AUTO_COMMIT_ENABLED_DEFAULT = "false";
  public static final String ZOOKEEPER_CONNECT_KEY = "zookeeper.connect";
  public static final String GROUP_ID_KEY = "group.id";
  private static final String FETCH_MAX_WAIT_KEY = "fetch.wait.max.ms";
  private static final String FETCH_MIN_BYTES_KEY = "fetch.min.bytes";
  private static final String FETCH_MIN_BYTES_DEFAULT = "1";
  private static final String ZK_CONNECTION_TIMEOUT_MS_KEY = "zookeeper.connection.timeout.ms";
  private static final String ZK_SESSION_TIMEOUT_MS_KEY = "zookeeper.session.timeout.ms";
  private static final String ZK_CONNECTION_TIMEOUT_MS_DEFAULT = "6000";
  private static final String ZK_SESSION_TIMEOUT_MS_DEFAULT = "6000";


  private static final Logger LOG = LoggerFactory.getLogger(HighLevelKafkaConsumer.class);


  private ConsumerConnector consumer;
  private ConsumerIterator<byte[],byte[]> consumerIterator;

  private final String zookeeperConnect;
  private final String topic;
  private final int maxBatchSize;
  private final int maxWaitTime;
  private final Map<String, String> kafkaConsumerConfigs;
  private final String consumerGroup;

  public HighLevelKafkaConsumer(String zookeeperConnect, String topic, String consumerGroup, int batchUpperLimit,
                                int consumerTimeout, Map<String, String> kafkaConsumerConfigs) {
    this.topic = topic;
    this.maxBatchSize = batchUpperLimit;
    this.maxWaitTime = consumerTimeout;
    this.kafkaConsumerConfigs = kafkaConsumerConfigs;
    this.zookeeperConnect = zookeeperConnect;
    this.consumerGroup = consumerGroup;
  }

  public void init() throws StageException {
    Properties props = new Properties();
    configureKafkaProperties(props);
    LOG.debug("Creating Kafka Consumer with properties {}" , props.toString());
    ConsumerConfig consumerConfig = new ConsumerConfig(props);
    try {
      consumer = Consumer.createJavaConsumerConnector(consumerConfig);
      Map<String, Integer> topicCountMap = new HashMap<>();

      //FIXME<Hari>: Create threads equal to the number of partitions for this topic
      // A known way to find number of partitions is by connecting to a known kafka broker which means the user has to
      // supply additional options - known broker host and port.
      //Another option is have the user specify the number of threads. If there are more threads than there are
      // partitions, some threads will never see a message
      topicCountMap.put(topic, 1);

      Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
        consumer.createMessageStreams(topicCountMap);
      List<KafkaStream<byte[], byte[]>> topicList = consumerMap.get(topic);
      KafkaStream<byte[], byte[]> stream = topicList.get(0);
      consumerIterator = stream.iterator();
    } catch (Exception e) {
      LOG.error(StageLibError.LIB_0311.getMessage(), e.getMessage());
      throw new StageException(StageLibError.LIB_0311, e.getMessage(), e);
    }
  }

  public void destroy() {
    if(consumer != null) {
      consumer.shutdown();
    }
  }

  public void commit() {
    consumer.commitOffsets();
  }

  public List<MessageAndOffset> read(int maxBatchSize) throws StageException {
    int batchSize = this.maxBatchSize > maxBatchSize ? maxBatchSize : this.maxBatchSize;
    int messageCount = 0;
    long startTime = System.currentTimeMillis();
    List<MessageAndOffset> messageAndOffsetList = new ArrayList<>();
    while (consumerIterator.hasNext() &&
      messageCount < batchSize &&
      (startTime + maxWaitTime) > System.currentTimeMillis()) {
      MessageAndMetadata<byte[], byte[]> messageAndMetadata = consumerIterator.next();
      byte[] message = messageAndMetadata.message();
      long offset = messageAndMetadata.offset();
      int partition = messageAndMetadata.partition();
      MessageAndOffset partitionToPayloadMap = new MessageAndOffset(message, offset, partition);
      messageAndOffsetList.add(partitionToPayloadMap);
      messageCount++;
    }
    return messageAndOffsetList;
  }

  private void configureKafkaProperties(Properties props) {

    props.put(ZOOKEEPER_CONNECT_KEY, zookeeperConnect);
    props.put(GROUP_ID_KEY, consumerGroup);
    /*The maximum amount of time the server will block before answering the fetch request if there isn't sufficient
    data to immediately satisfy fetch.min.bytes*/
    props.put(FETCH_MAX_WAIT_KEY, String.valueOf(maxWaitTime));

    /*Throw a timeout exception to the consumer if no message is available for consumption after the specified
    interval*/
    props.put(CONSUMER_TIMEOUT_KEY, CONSUMER_TIMEOUT_DEFAULT);
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

      for (Map.Entry<String, String> producerConfig : kafkaConsumerConfigs.entrySet()) {
        props.put(producerConfig.getKey(), producerConfig.getValue());
      }
    }
  }

}
