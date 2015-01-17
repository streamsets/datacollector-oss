/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.kafka;

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.util.KafkaStageLibError;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

public class KafkaUtil {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaUtil.class);

  public static int findNUmberOfPartitions(KafkaBroker broker, String topic, int timeout, int bufferSize,
                                           String clientName) throws StageException {

    SimpleConsumer simpleConsumer = null;
    try {
      LOG.info("Creating SimpleConsumer using the following configuration: host {}, port {}, max wait time {}, max " +
          "fetch size {}, client name {}", broker.getHost(), broker.getPort(), timeout, bufferSize,
        clientName);
      simpleConsumer = new SimpleConsumer(broker.getHost(), broker.getPort(), timeout, bufferSize,
        clientName);

      List<String> topics = Collections.singletonList(topic);
      TopicMetadataRequest req = new TopicMetadataRequest(topics);
      kafka.javaapi.TopicMetadataResponse resp = simpleConsumer.send(req);

      List<TopicMetadata> topicMetadataList = resp.topicsMetadata();
      if(topicMetadataList == null || topicMetadataList.isEmpty()) {
        LOG.error(KafkaStageLibError.LIB_0353.getMessage(), topic , broker.getHost() + ":" + broker.getPort());
        throw new StageException(KafkaStageLibError.LIB_0353, topic , broker.getHost() + ":" + broker.getPort());
      }
      TopicMetadata topicMetadata = topicMetadataList.iterator().next();
      //set number of partitions
      return topicMetadata.partitionsMetadata().size();
    } catch (Exception e) {
      LOG.error(KafkaStageLibError.LIB_0352.getMessage(), topic , broker.getHost() + ":" + broker.getPort(), e.getMessage());
      throw new StageException(KafkaStageLibError.LIB_0352, topic , broker.getHost() + ":" + broker.getPort(),
        e.getMessage(), e);
    } finally {
      if (simpleConsumer != null) {
        simpleConsumer.close();
      }
    }
  }
}
