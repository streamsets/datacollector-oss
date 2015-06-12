/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class KafkaUtil {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaUtil.class);
  private static final String METADATA_READER_CLIENT = "metadataReaderClient";
  private static final int METADATA_READER_TIME_OUT = 10000;
  private static final int BUFFER_SIZE = 64 * 1024;

  public static TopicMetadata getTopicMetadata(List<KafkaBroker> kafkaBrokers, String topic, int maxRetries,
                                               long backOffms) throws KafkaConnectionException {
    TopicMetadata topicMetadata = null;
    boolean connectionError = true;
    boolean retry = true;
    int retryCount = 0;
    while (retry && retryCount <= maxRetries) {
      for (KafkaBroker broker : kafkaBrokers) {
        SimpleConsumer simpleConsumer = null;
        try {
          simpleConsumer = new SimpleConsumer(broker.getHost(), broker.getPort(), METADATA_READER_TIME_OUT, BUFFER_SIZE,
            METADATA_READER_CLIENT);

          List<String> topics = Collections.singletonList(topic);
          TopicMetadataRequest req = new TopicMetadataRequest(topics);
          kafka.javaapi.TopicMetadataResponse resp = simpleConsumer.send(req);

          // No exception => no connection error
          connectionError = false;

          List<TopicMetadata> topicMetadataList = resp.topicsMetadata();
          if (topicMetadataList == null || topicMetadataList.isEmpty()) {
            //This broker did not have any metadata. May not be in sync?
            continue;
          }
          topicMetadata = topicMetadataList.iterator().next();
          if (topicMetadata != null && topicMetadata.errorCode() == 0) {
            retry = false;
          }
        } catch (Exception e) {
          //could not connect to this broker, try others
        } finally {
          if (simpleConsumer != null) {
            simpleConsumer.close();
          }
        }
      }
      if(retry) {
        LOG.warn("Unable to connect or cannot fetch topic metadata. Waiting for '{}' seconds before retrying",
          backOffms/1000);
        retryCount++;
        if(!ThreadUtil.sleep(backOffms)) {
          break;
        }
      }
    }
    if(connectionError) {
      //could not connect any broker even after retries. Fail with exception
      throw new KafkaConnectionException(Errors.KAFKA_67, getKafkaBrokers(kafkaBrokers));
    }
    return topicMetadata;
  }

  public static int getPartitionCount(String metadataBrokerList, String topic, int maxRetries,
                                               long backOffms) throws StageException {
    List<KafkaBroker> kafkaBrokers = getKafkaBrokers(metadataBrokerList);
    if(kafkaBrokers.isEmpty()) {
      throw new KafkaConnectionException(Errors.KAFKA_07, metadataBrokerList);
    }
    TopicMetadata topicMetadata = getTopicMetadata(kafkaBrokers, topic, maxRetries, backOffms);
    if(topicMetadata == null || topicMetadata.errorCode() != 0) {
      throw new StageException(Errors.KAFKA_03, topic, metadataBrokerList);
    }
    return topicMetadata.partitionsMetadata().size();
  }

  public static List<KafkaBroker> validateConnectionString(List<Stage.ConfigIssue> issues, String connectionString,
                                                     String configGroupName, String configName, Stage.Context context) {
    if(connectionString == null || connectionString.isEmpty()) {
      issues.add(context.createConfigIssue(configGroupName, configName,
        Errors.KAFKA_06, configName));
      return null;
    }
    List<KafkaBroker> kafkaBrokers = new ArrayList<>();

    // Technically, this is only necessary for testing Zookeeper connection strings, since Kafka's root znode may
    // be something other than / (as in CDH5.2, where it was /kafka). However, we're terrible people
    // who are overloading a function intended for Kafka broker lists and using it for ZK connection strings, as well.
    String[] chroot = connectionString.split("/");
    String brokerList = chroot[0];

    String[] brokers = brokerList.split(",");
    for(String broker : brokers) {
      String[] brokerHostAndPort = broker.split(":");
      if(brokerHostAndPort.length != 2) {
        issues.add(context.createConfigIssue(configGroupName, configName, Errors.KAFKA_07, connectionString));
      } else {
        try {
          int port = Integer.parseInt(brokerHostAndPort[1].trim());
          kafkaBrokers.add(new KafkaBroker(brokerHostAndPort[0].trim(), port));
        } catch (NumberFormatException e) {
          issues.add(context.createConfigIssue(configGroupName, configName, Errors.KAFKA_07, connectionString));
        }
      }
    }
    return kafkaBrokers;
  }

  public static String getKafkaBrokers(List<KafkaBroker> kafkaBrokers) {
    StringBuilder sb = new StringBuilder();
    for(KafkaBroker k : kafkaBrokers) {
      sb.append(k.getHost() + ":" + k.getPort()).append(", ");
    }
    sb.setLength(sb.length()-2);
    return sb.toString();
  }

  public static List<KafkaBroker> getKafkaBrokers(String brokerList) {
    List<KafkaBroker> kafkaBrokers = new ArrayList<>();
    if(brokerList != null && !brokerList.isEmpty()) {
      String[] brokers = brokerList.split(",");
      for (String broker : brokers) {
        String[] brokerHostAndPort = broker.split(":");
        if (brokerHostAndPort.length == 2) {
          try {
            int port = Integer.parseInt(brokerHostAndPort[1].trim());
            kafkaBrokers.add(new KafkaBroker(brokerHostAndPort[0].trim(), port));
          } catch (NumberFormatException e) {
            //ignore broker
          }
        }
      }
    }
    return kafkaBrokers;
  }
}
