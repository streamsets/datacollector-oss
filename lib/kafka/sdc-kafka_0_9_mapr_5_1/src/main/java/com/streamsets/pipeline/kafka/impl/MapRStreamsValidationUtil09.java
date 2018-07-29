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
import com.streamsets.pipeline.kafka.api.SdcKafkaValidationUtil;
import com.streamsets.pipeline.lib.kafka.BaseKafkaValidationUtil;
import com.streamsets.pipeline.lib.kafka.KafkaErrors;
import com.streamsets.pipeline.lib.maprstreams.MapRStreamsErrors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class MapRStreamsValidationUtil09 extends BaseKafkaValidationUtil implements SdcKafkaValidationUtil {

  private static final Logger LOG = LoggerFactory.getLogger(MapRStreamsValidationUtil09.class);
  private static final String KAFKA_VERSION = "0.9";
  public static final String KAFKA_CONFIG_BEAN_PREFIX = "maprStreamsTargetConfigBean.mapRStreamsTargetConfig.";
  public static final String STREAMS_RPC_TIMEOUT_MS = "streams.rpc.timeout.ms";
  public static final String ORG_APACHE_KAFKA_COMMON_SERIALIZATION_STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
  public static final String ORG_APACHE_KAFKA_COMMON_SERIALIZATION_STRING_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";

  @Override
  public String getVersion() {
    return KAFKA_VERSION;
  }

  @Override
  public int getPartitionCount(
      String metadataBrokerList,
      String topic,
      Map<String, Object> kafkaClientConfigs,
      int messageSendMaxRetries,
      long retryBackoffMs
  ) throws StageException {
    int partitionCount = -1;
    try {
      KafkaConsumer<String, String> kafkaConsumer = createTopicMetadataClient();
      List<PartitionInfo> partitionInfoList = kafkaConsumer.partitionsFor(topic);
      if(partitionInfoList != null) {
        partitionCount = partitionInfoList.size();
      }
    } catch (KafkaException e) {
      LOG.error(KafkaErrors.KAFKA_41.getMessage(), topic, e.toString(), e);
      throw new StageException(KafkaErrors.KAFKA_41, topic, e.toString(), e);
    }
    return partitionCount;
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
      List<PartitionInfo> partitionInfos;
      try {
        if (producer) {
          KafkaProducer<String, String> kafkaProducer = createProducerTopicMetadataClient(kafkaClientConfigs);
          partitionInfos = kafkaProducer.partitionsFor(topic);
        } else {
          KafkaConsumer<String, String> kafkaConsumer = createTopicMetadataClient();
          partitionInfos = kafkaConsumer.partitionsFor(topic);
        }
        if (null == partitionInfos || partitionInfos.isEmpty()) {
          issues.add(
              context.createConfigIssue(
                  groupName,
                  KAFKA_CONFIG_BEAN_PREFIX + "topic",
                  MapRStreamsErrors.MAPRSTREAMS_02,
                  topic
              )
          );
          valid = false;
        }
      } catch (KafkaException e) {
        LOG.error(MapRStreamsErrors.MAPRSTREAMS_01.getMessage(), topic, e.toString(), e);
        issues.add(
            context.createConfigIssue(
                groupName,
                configName,
                MapRStreamsErrors.MAPRSTREAMS_01,
                topic,
                e.getMessage()
            )
        );
        valid = false;
      }
    }
    return valid;
  }

  /**
   * Should be called only by MapR Streams Producer. It creates a topic using KafkaProducer.
   * @param topic
   * @param kafkaClientConfigs
   * @param metadataBrokerList
   * @throws StageException
   */
  @Override
  public void createTopicIfNotExists(String topic, Map<String, Object> kafkaClientConfigs, String metadataBrokerList)
      throws StageException {
    // Stream topic can be created through KafkaProducer if Stream Path exists already
    KafkaProducer<String, String> kafkaProducer = createProducerTopicMetadataClient(kafkaClientConfigs);
    kafkaProducer.partitionsFor(topic);
  }

  @Override
  public List<HostAndPort> validateKafkaBrokerConnectionString(
    List<Stage.ConfigIssue> issues,
    String connectionString,
    String configGroupName,
    String configName,
    Stage.Context context
  ) {
    List<HostAndPort> kafkaBrokers = new ArrayList<>();
    return kafkaBrokers;
  }

  @Override
  public List<HostAndPort> validateZkConnectionString(
    List<Stage.ConfigIssue> issues,
    String connectString,
    String configGroupName,
    String configName,
    Stage.Context context
  ) {
    List<HostAndPort> kafkaBrokers = new ArrayList<>();
    return kafkaBrokers;
  }

  protected KafkaConsumer<String, String> createTopicMetadataClient() {
    Properties props = new Properties();
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "sdcTopicMetadataClient");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ORG_APACHE_KAFKA_COMMON_SERIALIZATION_STRING_DESERIALIZER);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ORG_APACHE_KAFKA_COMMON_SERIALIZATION_STRING_DESERIALIZER);
    return new KafkaConsumer<>(props);
  }

  protected KafkaProducer<String, String> createProducerTopicMetadataClient(Map<String, Object> kafkaClientConfigs) {
    Properties props = new Properties();
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "topicMetadataClient");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ORG_APACHE_KAFKA_COMMON_SERIALIZATION_STRING_SERIALIZER);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ORG_APACHE_KAFKA_COMMON_SERIALIZATION_STRING_SERIALIZER);

    // Check if user has configured 'max.block.ms' option, otherwise wait for 60 seconds to fetch metadata
    if (kafkaClientConfigs != null && kafkaClientConfigs.containsKey(STREAMS_RPC_TIMEOUT_MS)) {
      props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, kafkaClientConfigs.get(STREAMS_RPC_TIMEOUT_MS));
    } else {
      props.put(STREAMS_RPC_TIMEOUT_MS, 60000);
    }
    return new KafkaProducer<>(props);
  }
}
