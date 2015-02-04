/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.kafka;

import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.util.KafkaStageLibError;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
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
  private static final String CONSUMER_TIMEOUT_DEFAULT = "60000";
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
  //Set the below option to "smallest" to get data from the beginning
  private static final String AUTO_OFFSET_RESET = "auto.offset.reset";

  private static final Logger LOG = LoggerFactory.getLogger(HighLevelKafkaConsumer.class);

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

  public HighLevelKafkaConsumer(String zookeeperConnect, String topic, String consumerGroup, int batchUpperLimit,
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

  public void init() throws StageException {
    Properties props = new Properties();
    configureKafkaProperties(props);
    LOG.debug("Creating Kafka Consumer with properties {}" , props.toString());
    consumerConfig = new ConsumerConfig(props);
    createConsumer();
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
    List<MessageAndOffset> messageAndOffsetList = new ArrayList<>();
    int batchSize = this.maxBatchSize > maxBatchSize ? maxBatchSize : this.maxBatchSize;
    int messageCount = 0;
    long startTime = System.currentTimeMillis();
    //int attemptNumber = 0;
    //Keep trying to collect messages until the batch size is reached or the max wait time is reached
    while (messageCount < batchSize && (startTime + maxWaitTime) > System.currentTimeMillis()) {
      try {
        //has next blocks indefinitely if consumer.timeout.ms is set to -1
        //But if consumer.timeout.ms is set to a value, like 6000 in this case, a ConsumerTimeoutException is thrown
        //if no message is written to kafka topic in that time.
        if(consumerIterator.hasNext()) {
          MessageAndMetadata<byte[], byte[]> messageAndMetadata = consumerIterator.next();
          byte[] message = messageAndMetadata.message();
          long offset = messageAndMetadata.offset();
          int partition = messageAndMetadata.partition();
          MessageAndOffset partitionToPayloadMap = new MessageAndOffset(message, offset, partition);
          messageAndOffsetList.add(partitionToPayloadMap);
          messageCount++;
        }
      } catch (ConsumerTimeoutException e) {
        //Another option here is to have a low consumer.timeout.ms value [less than batch duration time]
        //This allows us to better tune latency and throughput.
        //But in that case we may not be able to detect lost connections

        //For now we keep a high value for consumer.timeout.ms and in case of ConsumerTimeoutException
        //we recreate consumer connections and stream.
        //attemptNumber++;
        //FIXME<Hari>:Figure out a way to detect lost connection
        //git statushandleConnectionError(attemptNumber);
      }
    }
    return messageAndOffsetList;
  }

  private void handleConnectionError(int attemptNumber) throws StageException {
    try {
      destroy();
      createConsumer();
    } catch (StageException e) {
      //raise alert and retry after a certain time.
      LOG.warn("Error connecting to kafka broker, '{}", e.getMessage());
      context.reportError(e);
    }
    try {
      switch (attemptNumber) {
        case 1:
          Thread.sleep(30000);
          break;
        case 2:
          Thread.sleep(60000);
          break;
        case 3:
          Thread.sleep(120000);
          break;
        default:
          Thread.sleep(300000);
      }
    } catch (InterruptedException e1) {
      LOG.error(KafkaStageLibError.KFK_0311.getMessage(), e1.getMessage());
      throw new StageException(KafkaStageLibError.KFK_0311, e1.getMessage(), e1);
    }
  }


  private void createConsumer() throws StageException {
    LOG.debug("Creating consumer with configuration {}", consumerConfig.props().props().toString());
    try {
      consumer = Consumer.createJavaConsumerConnector(consumerConfig);
    } catch (Exception e) {
      LOG.error(KafkaStageLibError.KFK_0311.getMessage(), e.getMessage());
      throw new StageException(KafkaStageLibError.KFK_0311, e.getMessage(), e);
    }

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
    stream = topicList.get(0);
    try {
      consumerIterator = stream.iterator();
    } catch (Exception e) {
      LOG.error(KafkaStageLibError.KFK_0312.getMessage(), e.getMessage());
      throw new StageException(KafkaStageLibError.KFK_0312, e.getMessage(), e);
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
    props.put(CONSUMER_TIMEOUT_KEY, /*String.valueOf(maxWaitTime)*/ CONSUMER_TIMEOUT_DEFAULT);
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

      for (Map.Entry<String, String> producerConfig : kafkaConsumerConfigs.entrySet()) {
        props.put(producerConfig.getKey(), producerConfig.getValue());
      }
    }
  }

}
