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

import com.google.common.net.HostAndPort;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.kafka.api.SdcKafkaValidationUtil;
import com.streamsets.pipeline.lib.kafka.BaseKafkaValidationUtil;
import com.streamsets.pipeline.lib.kafka.KafkaErrors;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import kafka.common.ErrorMapping;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class KafkaValidationUtil08 extends BaseKafkaValidationUtil implements SdcKafkaValidationUtil {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaValidationUtil08.class);
  public static final String KAFKA_CONFIG_BEAN_PREFIX = "kafkaConfigBean.kafkaConfig.";
  public static final String KAFKA_CONNECTION_CONFIG_BEAN_PREFIX = "kafkaConfigBean.connectionConfig.connection.";
  private static final String METADATA_READER_CLIENT = "metadataReaderClient";
  private static final int METADATA_READER_TIME_OUT = 10000;
  private static final int BUFFER_SIZE = 64 * 1024;

  @Override
  public String getVersion() {
    return Kafka08Constants.KAFKA_VERSION;
  }

  @Override
  public int getPartitionCount(
      String metadataBrokerList,
      String topic,
      Map<String, Object> kafkaClientConfigs,
      int messageSendMaxRetries,
      long retryBackoffMs
  ) throws StageException {
    List<HostAndPort> kafkaBrokers = getKafkaBrokers(metadataBrokerList);
    TopicMetadata topicMetadata;
    try {
      topicMetadata = KafkaValidationUtil08.getTopicMetadata(
          kafkaBrokers,
          topic,
          messageSendMaxRetries,
          retryBackoffMs
      );
      if (topicMetadata == null) {
        // Could not get topic metadata from any of the supplied brokers
        throw new StageException(KafkaErrors.KAFKA_03, topic, metadataBrokerList);
      }
      if (topicMetadata.errorCode() == ErrorMapping.UnknownTopicOrPartitionCode()) {
        // Topic does not exist
        throw new StageException(KafkaErrors.KAFKA_04, topic);
      }
      if (topicMetadata.errorCode() != 0) {
        // Topic metadata returned error code other than ErrorMapping.UnknownTopicOrPartitionCode()
        throw new StageException(KafkaErrors.KAFKA_03, topic, metadataBrokerList);
      }
    } catch (IOException e) {
      LOG.error(KafkaErrors.KAFKA_11.getMessage(), topic, kafkaBrokers, e.toString(), e);
      throw new StageException(KafkaErrors.KAFKA_11, topic, kafkaBrokers, e.toString());
    }
    return topicMetadata.partitionsMetadata().size();
  }

  @Override
  public boolean validateTopicExistence(
    Stage.Context context,
    String groupName,
    String configName,
    List<HostAndPort> kafkaBrokers,
    String metadataBrokerList,
    String topic,
    Map<String, Object> kafkaClientConfigs,
    List<Stage.ConfigIssue> issues,
    boolean producer
  ) {
    boolean valid = true;
    if(topic == null || topic.isEmpty()) {
      issues.add(context.createConfigIssue(groupName, configName, KafkaErrors.KAFKA_05));
      valid = false;
    } else {
      TopicMetadata topicMetadata;
      try {
        topicMetadata = KafkaValidationUtil08.getTopicMetadata(kafkaBrokers, topic, 1, 0);
        if(topicMetadata == null) {
          //Could not get topic metadata from any of the supplied brokers
          issues.add(
              context.createConfigIssue(
                  groupName,
                  KAFKA_CONFIG_BEAN_PREFIX + "topic",
                  KafkaErrors.KAFKA_03,
                  topic,
                  metadataBrokerList
              )
          );
          valid = false;
        } else if (topicMetadata.errorCode() == ErrorMapping.UnknownTopicOrPartitionCode()) {
          //Topic does not exist
          issues.add(
              context.createConfigIssue(
                  groupName,
                  KAFKA_CONFIG_BEAN_PREFIX + "topic",
                  KafkaErrors.KAFKA_04,
                  topic
              )
          );
          valid = false;
        } else if (topicMetadata.errorCode() != 0) {
          // Topic metadata returned error code other than ErrorMapping.UnknownTopicOrPartitionCode()
          issues.add(
              context.createConfigIssue(
                  groupName,
                  KAFKA_CONFIG_BEAN_PREFIX + "topic",
                  KafkaErrors.KAFKA_03,
                  topic,
                  metadataBrokerList
              )
          );
          valid = false;
        }
      } catch (IOException e) {
        //Could not connect to kafka with the given metadata broker list
        issues.add(
            context.createConfigIssue(
                groupName,
                 KAFKA_CONNECTION_CONFIG_BEAN_PREFIX + "metadataBrokerList",
                KafkaErrors.KAFKA_67,
                metadataBrokerList
            )
        );
        valid = false;
      }
    }
    return valid;
  }

  @Override
  public void createTopicIfNotExists(String topic, Map<String, Object> kafkaClientConfigs, String metadataBrokerList) throws StageException {
    // no-op
  }

  private static TopicMetadata getTopicMetadata(
      List<HostAndPort> kafkaBrokers,
      String topic,
      int maxRetries,
      long backOffms
  ) throws IOException {
    TopicMetadata topicMetadata = null;
    boolean connectionError = true;
    boolean retry = true;
    int retryCount = 0;
    while (retry && retryCount <= maxRetries) {
      for (HostAndPort broker : kafkaBrokers) {
        SimpleConsumer simpleConsumer = null;
        try {
          simpleConsumer = new SimpleConsumer(
              broker.getHostText(),
              broker.getPort(),
              METADATA_READER_TIME_OUT,
              BUFFER_SIZE,
              METADATA_READER_CLIENT
          );

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
      throw new IOException(Utils.format(KafkaErrors.KAFKA_67.getMessage(), getKafkaBrokers(kafkaBrokers)));
    }
    return topicMetadata;
  }

  private static String getKafkaBrokers(List<HostAndPort> kafkaBrokers) {
    return StringUtils.join(kafkaBrokers, ",");
  }

  private static List<HostAndPort> getKafkaBrokers(String brokerList) {
    List<HostAndPort> kafkaBrokers = new ArrayList<>();
    if(brokerList != null && !brokerList.isEmpty()) {
      String[] brokers = brokerList.split(",");
      for (String broker : brokers) {
        try {
          kafkaBrokers.add(HostAndPort.fromString(broker));
        } catch (IllegalArgumentException e) {
          // Ignore broker
        }
      }
    }
    return kafkaBrokers;
  }

}
