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
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaValidationUtil09 extends BaseKafkaValidationUtil implements SdcKafkaValidationUtil {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaValidationUtil09.class);
  private static final String KAFKA_VERSION = "0.9";
  public static final String KAFKA_CONFIG_BEAN_PREFIX = "kafkaConfigBean.kafkaConfig.";

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
    Consumer<String, String> kafkaConsumer = null;
    try {
      kafkaConsumer = createTopicMetadataClient(metadataBrokerList, kafkaClientConfigs);
      List<PartitionInfo> partitionInfoList = kafkaConsumer.partitionsFor(topic);
      if(partitionInfoList != null) {
        partitionCount = partitionInfoList.size();
      }
    } catch (KafkaException e) {
      LOG.error(KafkaErrors.KAFKA_41.getMessage(), topic, e.toString(), e);
      throw new StageException(KafkaErrors.KAFKA_41, topic, e.toString());
    } finally {
      if (kafkaConsumer != null) {
        kafkaConsumer.close();
      }
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
      // Use Consumer to check if topic exists.
      // Using producer causes unintentionally creating a topic if not exist.
      Consumer<String, String> kafkaConsumer = null;
      try {
          kafkaConsumer = createTopicMetadataClient(
              metadataBrokerList,
              kafkaClientConfigs
          );
          partitionInfos = kafkaConsumer.partitionsFor(topic);
        if (null == partitionInfos || partitionInfos.isEmpty()) {
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
      } catch (KafkaException e) {
        LOG.error(KafkaErrors.KAFKA_68.getMessage(), topic, metadataBrokerList, e.toString(), e);
        issues.add(context.createConfigIssue(groupName, configName, KafkaErrors.KAFKA_68, topic, metadataBrokerList, e.toString()));
        valid = false;
      } finally {
        if (kafkaConsumer != null) {
          kafkaConsumer.close();
        }
      }
    }
    return valid;
  }

  @Override
  public void createTopicIfNotExists(String topic, Map<String, Object> kafkaClientConfigs, String metadataBrokerList) throws StageException {
    Producer<String, String> kafkaProducer = createProducerTopicMetadataClient(
        metadataBrokerList,
        kafkaClientConfigs
    );
    kafkaProducer.partitionsFor(topic);
  }

  private Consumer<String, String> createTopicMetadataClient(
      String metadataBrokerList,
      Map<String, Object> kafkaClientConfigs
  ) {
    Properties props = new Properties();
    props.put("bootstrap.servers", metadataBrokerList);
    props.put("group.id", "sdcTopicMetadataClient");
    props.put("enable.auto.commit", "false");
    props.put("auto.commit.interval.ms", "1000");
    props.put("session.timeout.ms", "30000");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    addSecurityProperties(kafkaClientConfigs, props);

    return new KafkaConsumer<>(props);
  }

  private Producer<String, String> createProducerTopicMetadataClient(
    String metadataBrokerList,
    Map<String, Object> kafkaClientConfigs
  ) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, metadataBrokerList);
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "topicMetadataClient");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

    // Check if user has configured 'max.block.ms' option, otherwise wait for 60 seconds to fetch metadata
    if (kafkaClientConfigs != null &&
        kafkaClientConfigs.containsKey(ProducerConfig.MAX_BLOCK_MS_CONFIG)
    ) {
      props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, kafkaClientConfigs.get(ProducerConfig.MAX_BLOCK_MS_CONFIG));
    } else {
      props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 60000);
    }
    addSecurityProperties(kafkaClientConfigs, props);

    return new KafkaProducer<>(props);
  }

  private static void addSecurityProperties(Map<String, Object> kafkaClientConfigs, Properties props) {
    //The following options, if specified, are ignored : "bootstrap.servers", "key.serializer" and "value.serializer"
    if (kafkaClientConfigs != null && !kafkaClientConfigs.isEmpty()) {
      kafkaClientConfigs.remove(Kafka09Constants.BOOTSTRAP_SERVERS_KEY);
      kafkaClientConfigs.remove(Kafka09Constants.KEY_SERIALIZER_KEY);
      kafkaClientConfigs.remove(Kafka09Constants.VALUE_SERIALIZER_KEY);

      for (Map.Entry<String, Object> clientConfig : kafkaClientConfigs.entrySet()) {
        if(clientConfig.getKey().startsWith("ssl.") ||
          clientConfig.getKey().startsWith("sasl.") ||
          clientConfig.getKey().equals("security.protocol")) {
          props.put(clientConfig.getKey(), clientConfig.getValue());
        }
      }
    }
  }
}
