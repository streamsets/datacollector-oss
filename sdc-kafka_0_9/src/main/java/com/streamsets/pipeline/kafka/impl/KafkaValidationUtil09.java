/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.kafka.api.KafkaBroker;
import com.streamsets.pipeline.kafka.api.SdcKafkaValidationUtil;
import com.streamsets.pipeline.lib.kafka.KafkaErrors;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.zookeeper.common.PathUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaValidationUtil09 implements SdcKafkaValidationUtil {

  private static final String KAFKA_VERSION = "0.9";

  @Override
  public String getVersion() {
    return KAFKA_VERSION;
  }

  public int getPartitionCount(
      String metadataBrokerList,
      String topic,
      Map<String, Object> kafkaClientConfigs,
      int messageSendMaxRetries,
      long retryBackoffMs
  ) throws StageException {
    KafkaConsumer<String, String> kafkaConsumer = createTopicMetadataClient(metadataBrokerList, kafkaClientConfigs);
    List<PartitionInfo> partitionInfoList = kafkaConsumer.partitionsFor(topic);
    return partitionInfoList.size();
  }

  public List<KafkaBroker> validateKafkaBrokerConnectionString(
      List<Stage.ConfigIssue> issues,
      String connectionString,
      String configGroupName,
      String configName,
      Stage.Context context
  ) {
    List<KafkaBroker> kafkaBrokers = new ArrayList<>();
    if (connectionString == null || connectionString.isEmpty()) {
      issues.add(context.createConfigIssue(configGroupName, configName,
          KafkaErrors.KAFKA_06, configName));
    } else {
      String[] brokers = connectionString.split(",");
      for (String broker : brokers) {
        String[] brokerHostAndPort = broker.split(":");
        if (brokerHostAndPort.length != 2) {
          issues.add(context.createConfigIssue(configGroupName, configName, KafkaErrors.KAFKA_07, connectionString));
        } else {
          try {
            int port = Integer.parseInt(brokerHostAndPort[1].trim());
            kafkaBrokers.add(new KafkaBroker(brokerHostAndPort[0].trim(), port));
          } catch (NumberFormatException e) {
            issues.add(context.createConfigIssue(configGroupName, configName, KafkaErrors.KAFKA_07, connectionString));
          }
        }
      }
    }
    return kafkaBrokers;
  }

  public List<KafkaBroker> validateZkConnectionString(
      List<Stage.ConfigIssue> issues,
      String connectString,
      String configGroupName,
      String configName,
      Stage.Context context
  ) {
    List<KafkaBroker> kafkaBrokers = new ArrayList<>();
    if (connectString == null || connectString.isEmpty()) {
      issues.add(context.createConfigIssue(configGroupName, configName,
          KafkaErrors.KAFKA_06, configName));
      return kafkaBrokers;
    }

    String chrootPath;
    int off = connectString.indexOf('/');
    if (off >= 0) {
      chrootPath = connectString.substring(off);
      // ignore a single "/". Same as null. Anything longer must be validated
      if (chrootPath.length() > 1) {
        try {
          PathUtils.validatePath(chrootPath);
        } catch (IllegalArgumentException e) {
          issues.add(context.createConfigIssue(configGroupName, configName, KafkaErrors.KAFKA_09, connectString,
            e.toString()));
        }
      }
      connectString = connectString.substring(0, off);
    }

    String brokers[] = connectString.split(",");
    for(String broker : brokers) {
      String[] brokerHostAndPort = broker.split(":");
      if(brokerHostAndPort.length != 2) {
        issues.add(context.createConfigIssue(configGroupName, configName, KafkaErrors.KAFKA_09, connectString,
          "The connection String is not of the form <HOST>:<PORT>"));
      } else {
        try {
          int port = Integer.parseInt(brokerHostAndPort[1].trim());
          kafkaBrokers.add(new KafkaBroker(brokerHostAndPort[0].trim(), port));
        } catch (NumberFormatException e) {
          issues.add(context.createConfigIssue(configGroupName, configName, KafkaErrors.KAFKA_07, connectString,
            e.toString()));
        }
      }
    }
    return kafkaBrokers;
  }

  public boolean validateTopicExistence(
    Stage.Context context,
    String groupName,
    String configName,
    List<KafkaBroker> kafkaBrokers,
    String metadataBrokerList,
    String topic,
    Map<String, Object> kafkaClientConfigs,
    List<Stage.ConfigIssue> issues
  ) {
    boolean valid = true;
    if(topic == null || topic.isEmpty()) {
      issues.add(context.createConfigIssue(groupName, configName, KafkaErrors.KAFKA_05));
      valid = false;
    } else {
      KafkaConsumer<String, String> kafkaConsumer = createTopicMetadataClient(metadataBrokerList, kafkaClientConfigs);
      try {
        List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(topic);
        if(null == partitionInfos || partitionInfos.size() == 0) {
          issues.add(context.createConfigIssue(groupName, "topic", KafkaErrors.KAFKA_03, topic, metadataBrokerList));
          valid = false;
        }
      } catch (WakeupException | AuthorizationException e) {
        issues.add(context.createConfigIssue(groupName, configName, KafkaErrors.KAFKA_68, topic, metadataBrokerList));
        valid = false;
      }
    }
    return valid;
  }

  private KafkaConsumer<String, String> createTopicMetadataClient(
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

  private void addSecurityProperties(Map<String, Object> kafkaClientConfigs, Properties props) {
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
