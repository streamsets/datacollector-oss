/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import kafka.common.ErrorMapping;
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

  public static int findNUmberOfPartitions(String metadataBrokerList, String topic) throws StageException {
    List<KafkaBroker> kafkaBrokers = getKafkaBrokers(metadataBrokerList);
    TopicMetadata topicMetadata = getTopicMetadata(kafkaBrokers, topic);

    //The following should not happen since its already validated.
    //Unless something changes while the pipeline is running
    if(topicMetadata == null) {
      //Could not get topic metadata from any of the supplied brokers
      LOG.error(Errors.KAFKA_03.getMessage(), topic, metadataBrokerList);
      throw new StageException(Errors.KAFKA_03, topic, metadataBrokerList);
    }
    if(topicMetadata.errorCode()== ErrorMapping.UnknownTopicOrPartitionCode()) {
      //Topic does not exist
      LOG.error(Errors.KAFKA_04.getMessage(), topic);
      throw new StageException(Errors.KAFKA_04, topic);
    }
    return topicMetadata.partitionsMetadata().size();
  }

  public static TopicMetadata getTopicMetadata(List<KafkaBroker> kafkaBrokers, String topic) {
    SimpleConsumer simpleConsumer = null;
    TopicMetadata topicMetadata = null;
    for(KafkaBroker broker : kafkaBrokers) {
      try {
        simpleConsumer = new SimpleConsumer(broker.getHost(), broker.getPort(), METADATA_READER_TIME_OUT, BUFFER_SIZE,
          METADATA_READER_CLIENT);

        List<String> topics = Collections.singletonList(topic);
        TopicMetadataRequest req = new TopicMetadataRequest(topics);
        kafka.javaapi.TopicMetadataResponse resp = simpleConsumer.send(req);

        List<TopicMetadata> topicMetadataList = resp.topicsMetadata();
        if (topicMetadataList == null || topicMetadataList.isEmpty()) {
          //This broker did not have any metadata. May not be in sync?
          continue;
        }
        topicMetadata = topicMetadataList.iterator().next();
        if(topicMetadata != null) {
          break;
        }
      } catch (Exception e) {
        //try next broker from the list
      } finally {
        if (simpleConsumer != null) {
          simpleConsumer.close();
        }
      }
    }
    return topicMetadata;
  }

  public static void validateTopic(List<Stage.ConfigIssue> issues, List<KafkaBroker> kafkaBrokers,
                                   Stage.Context context, String confiGroupName, String configName,
                                   String topic, String brokerList) {
    if(topic == null || topic.isEmpty()) {
      issues.add(context.createConfigIssue(confiGroupName, configName,
        Errors.KAFKA_05));
    }

    TopicMetadata topicMetadata = KafkaUtil.getTopicMetadata(kafkaBrokers, topic);

    if(topicMetadata == null) {
      //Could not get topic metadata from any of the supplied brokers
      issues.add(context.createConfigIssue(confiGroupName, configName,
        Errors.KAFKA_03, topic, brokerList));
      return;
    }
    if(topicMetadata.errorCode()== ErrorMapping.UnknownTopicOrPartitionCode()) {
      //Topic does not exist
      issues.add(context.createConfigIssue(confiGroupName, configName,
        Errors.KAFKA_04, topic));
      return;
    }
  }

  public static List<KafkaBroker> validateBrokerList(List<Stage.ConfigIssue> issues, String brokerList,
                                                     String confiGroupName, String configName, Stage.Context context) {
    if(brokerList == null || brokerList.isEmpty()) {
      issues.add(context.createConfigIssue(confiGroupName, configName,
        Errors.KAFKA_06, configName));
      return null;
    }
    List<KafkaBroker> kafkaBrokers = new ArrayList<>();
    String[] brokers = brokerList.split(",");
    for(String broker : brokers) {
      String[] brokerHostAndPort = broker.split(":");
      if(brokerHostAndPort.length != 2) {
        issues.add(context.createConfigIssue(confiGroupName, configName, Errors.KAFKA_07, brokerList));
      } else {
        try {
          int port = Integer.parseInt(brokerHostAndPort[1]);
          kafkaBrokers.add(new KafkaBroker(brokerHostAndPort[0], port));
        } catch (NumberFormatException e) {
          issues.add(context.createConfigIssue(confiGroupName, configName, Errors.KAFKA_07, brokerList));
        }
      }
    }
    return kafkaBrokers;
  }

  private static List<KafkaBroker> getKafkaBrokers(String metadataBrokerList) {
    //configurations are already validated so do not expect any exception
    List<KafkaBroker> kafkaBrokers = new ArrayList<>();
    String[] brokers = metadataBrokerList.split(",");
    for(String broker : brokers) {
      String[] brokerHostAndPort = broker.split(":");
      kafkaBrokers.add(new KafkaBroker(brokerHostAndPort[0], Integer.parseInt(brokerHostAndPort[1])));
    }
    return kafkaBrokers;
  }

}
