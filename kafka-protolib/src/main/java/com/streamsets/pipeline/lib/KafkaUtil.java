/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib;

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

  public static int findNUmberOfPartitions(String metadataBrokerList, String topic, int timeout, int bufferSize,
                                           String clientName) throws StageException {
    List<KafkaBroker> kafkaBrokers = getKafkaBrokers(metadataBrokerList);
    SimpleConsumer simpleConsumer = null;
    TopicMetadata topicMetadata = null;
    for(KafkaBroker broker : kafkaBrokers) {
      try {
        LOG.info("Creating SimpleConsumer using the following configuration: host {}, port {}, max wait time {}, max " +
            "fetch size {}, client columnName {}", broker.getHost(), broker.getPort(), timeout, bufferSize,
          clientName);
        simpleConsumer = new SimpleConsumer(broker.getHost(), broker.getPort(), timeout, bufferSize,
          clientName);

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
    if(topicMetadata == null) {
      //Could not get topic metadata from any of the supplied brokers
      LOG.error(Errors.KAFKA_19.getMessage(), topic, metadataBrokerList);
      throw new StageException(Errors.KAFKA_19, topic, metadataBrokerList);
    }
    if(topicMetadata.errorCode()== ErrorMapping.UnknownTopicOrPartitionCode()) {
      //Topic does not exist
      LOG.error(Errors.KAFKA_24.getMessage(), topic);
      throw new StageException(Errors.KAFKA_24, topic);
    }
    return topicMetadata.partitionsMetadata().size();
  }

  private static List<KafkaBroker> getKafkaBrokers(String metadataBrokerList) {
    List<KafkaBroker> kafkaBrokers = new ArrayList<>();
    String[] brokers = metadataBrokerList.split(",");
    for(String broker : brokers) {
      String[] brokerHostAndPort = broker.split(":");
      kafkaBrokers.add(new KafkaBroker(brokerHostAndPort[0], Integer.parseInt(brokerHostAndPort[1])));
    }
    return kafkaBrokers;
  }
}
