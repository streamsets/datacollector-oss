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
package com.streamsets.pipeline.stage.destination.kafka;

import com.google.common.net.HostAndPort;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.kafka.api.KafkaDestinationGroups;
import com.streamsets.pipeline.kafka.api.PartitionStrategy;
import com.streamsets.pipeline.kafka.api.ProducerFactorySettings;
import com.streamsets.pipeline.kafka.api.SdcKafkaProducer;
import com.streamsets.pipeline.kafka.api.SdcKafkaProducerFactory;
import com.streamsets.pipeline.kafka.api.SdcKafkaValidationUtil;
import com.streamsets.pipeline.kafka.api.SdcKafkaValidationUtilFactory;
import com.streamsets.pipeline.lib.el.ELUtils;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.kafka.KafkaErrors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class KafkaTargetConfig {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaTargetConfig.class);

  private static final String MESSAGE_SEND_MAX_RETRIES_KEY = "message.send.max.retries";
  private static final int MESSAGE_SEND_MAX_RETRIES_DEFAULT = 10;
  private static final String RETRY_BACKOFF_MS_KEY = "retry.backoff.ms";
  private static final long RETRY_BACKOFF_MS_DEFAULT = 1000;
  private static final int TOPIC_WARN_SIZE = 500;
  public static final String KAFKA_CONFIG_BEAN_PREFIX = "kafkaConfigBean.kafkaConfig.";

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "localhost:9092",
    label = "Broker URI",
    description = "Comma-separated list of URIs for brokers that write to the topic.  Use the format " +
      "<HOST>:<PORT>. To ensure a connection, enter as many as possible.",
    displayPosition = 10,
    group = "#0"
  )
  public String metadataBrokerList;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.BOOLEAN,
    defaultValue = "false",
    label = "Runtime Topic Resolution",
    description = "Select topic at runtime based on the field values in the record",
    displayPosition = 15,
    group = "#0"
  )
  public boolean runtimeTopicResolution;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "${record:value('/topic')}",
    label = "Topic Expression",
    description = "An expression that resolves to the name of the topic to use",
    displayPosition = 20,
    elDefs = {RecordEL.class},
    group = "#0",
    evaluation = ConfigDef.Evaluation.EXPLICIT,
    dependsOn = "runtimeTopicResolution",
    triggeredByValue = "true"
  )
  public String topicExpression;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.TEXT,
    lines = 5,
    defaultValue = "*",
    label = "Topic White List",
    description = "A comma-separated list of valid topic names. " +
      "Records with invalid topic names are treated as error records. " +
      "'*' indicates that all topic names are allowed.",
    displayPosition = 23,
    group = "#0",
    dependsOn = "runtimeTopicResolution",
    triggeredByValue = "true"
  )
  public String topicWhiteList;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "topicName",
    label = "Topic",
    description = "",
    displayPosition = 25,
    group = "#0",
    dependsOn = "runtimeTopicResolution",
    triggeredByValue = "false"
  )
  public String topic;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "ROUND_ROBIN",
    label = "Partition Strategy",
    description = "Strategy to select a partition to write to",
    displayPosition = 30,
    group = "#0"
  )
  @ValueChooserModel(PartitionStrategyChooserValues.class)
  public PartitionStrategy partitionStrategy;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.STRING,
    defaultValue = "${0}",
    label = "Partition Expression",
    description = "Determines the partition key to use with default kafka partitioner class in case of 'Default " +
      "Partition Strategy'. In case of 'Expression Partition Strategy' it determines the partition number",
    displayPosition = 40,
    group = "#0",
    dependsOn = "partitionStrategy",
    triggeredByValue = {"EXPRESSION", "DEFAULT"},
    elDefs = {RecordEL.class},
    evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String partition;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.BOOLEAN,
    defaultValue = "false",
    label = "One Message per Batch",
    description = "Generates a single Kafka message with all records in the batch",
    displayPosition = 50,
    group = "#0"
  )
  public boolean singleMessagePerBatch;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.MAP,
    defaultValue = "",
    label = "Kafka Configuration",
    description = "Additional Kafka properties to pass to the underlying Kafka producer",
    displayPosition = 60,
    group = "#0"
  )
  public Map<String, String> kafkaProducerConfigs;


  // Private members

  private SdcKafkaProducer kafkaProducer;
  private SdcKafkaValidationUtil kafkaValidationUtil;
  private ELEval partitionEval;
  private ELVars partitionVars;
  private ELEval topicEval;
  private ELVars topicVars;
  private Set<String> allowedTopics;
  private boolean allowAllTopics;
  private List<HostAndPort> kafkaBrokers;
  // cache topic name vs the number of partitions
  private Map<String, Integer> topicPartitionMap;
  // cache invalid topic names encountered while resolving the topic names dynamically at runtime
  private Map<String, StageException> invalidTopicMap;
  // holds the value of 'message.send.max.retries' supplied by the user or default value*
  private int messageSendMaxRetries;
  // holds the value of 'retry.backoff.ms' supplied by the user or the default value
  private long retryBackoffMs;

  public void init(
      Stage.Context context,
      DataFormat dataFormat,
      List<Stage.ConfigIssue> issues
  ) {
    this.topicPartitionMap = new HashMap<>();
    this.allowedTopics = new HashSet<>();
    this.invalidTopicMap = new HashMap<>();
    allowAllTopics = false;
    kafkaValidationUtil = SdcKafkaValidationUtilFactory.getInstance().create();
    //metadata broker list should be one or more <host>:<port> separated by a comma
    kafkaBrokers = kafkaValidationUtil.validateKafkaBrokerConnectionString(
        issues,
        metadataBrokerList,
        KafkaDestinationGroups.KAFKA.name(),
        KAFKA_CONFIG_BEAN_PREFIX + "metadataBrokerList",
        context
    );

    //check if the topic contains EL expression with record: functions
    //If yes, then validate the EL expression. Do not validate for existence of topic
    boolean validateTopicExists = !runtimeTopicResolution;

    if(runtimeTopicResolution) {
      //EL containing record: functions - make sure the expression is valid and parses correctly
      if(topicExpression == null || topicExpression.trim().isEmpty()) {
        issues.add(
            context.createConfigIssue(
                KafkaDestinationGroups.KAFKA.name(),
                KAFKA_CONFIG_BEAN_PREFIX + "topicExpression",
                KafkaErrors.KAFKA_05
            )
        );
      }
      validateTopicExpression(context, issues);
      //Also a topic white list is expected in this case, validate the list
      validateTopicWhiteList(context, issues, kafkaBrokers);
    } else {
      //check if the topic contains EL expression other than record: functions. It could be str: or constants
      //If yes, then evaluate expression as it is static.
      //Then validate for topic existence
      if (topic.startsWith("${")) {
        //EL with constants or String functions
        //evaluate expression and validate topic
        topicEval = context.createELEval("topic");
        topicVars = context.createELVars();
        try {
          topic = topicEval.eval(topicVars, topic, String.class);
        } catch (Exception ex) {
          validateTopicExists = false;
          issues.add(
              context.createConfigIssue(
                  KafkaDestinationGroups.KAFKA.name(),
                  KAFKA_CONFIG_BEAN_PREFIX + "topic",
                  KafkaErrors.KAFKA_61,
                  topic,
                  ex.toString(),
                  ex
              )
          );
        }
      }

      if(issues.isEmpty() && validateTopicExists) {
        validateTopicExistence(context, issues, topic);
      }
    }

    //validate partition expression
    validatePartitionExpression(context, issues);

    //kafka producer configs
    validateKafkaProducerConfigs(context, issues);

    if (issues.isEmpty()) {
      ProducerFactorySettings settings = new ProducerFactorySettings(
          kafkaProducerConfigs == null ?
              Collections.<String, Object>emptyMap() :
              new HashMap<String, Object>(kafkaProducerConfigs),
          partitionStrategy,
          metadataBrokerList,
          dataFormat
      );
      kafkaProducer = SdcKafkaProducerFactory.create(settings).create();
      try {
        kafkaProducer.init();
      } catch (StageException ex) {
        issues.add(context.createConfigIssue(null, null, ex.getErrorCode(), ex.getParams()));
      }
    }
  }

  public void destroy() {
    if(kafkaProducer != null) {
      kafkaProducer.destroy();
    }
  }

  private void validatePartitionExpression(Stage.Context context, List<Stage.ConfigIssue> issues) {
    if (partitionStrategy == PartitionStrategy.EXPRESSION || partitionStrategy == PartitionStrategy.DEFAULT) {
      partitionEval = context.createELEval("partition");
      partitionVars = context.createELVars();
      //There is no scope to provide partitionVars for kafka target as of today, create empty partitionVars
      ELUtils.validateExpression(
          partitionEval,
          context.createELVars(),
          partition,
          context,
          KafkaDestinationGroups.KAFKA.name(),
          KAFKA_CONFIG_BEAN_PREFIX + "partition",
          KafkaErrors.KAFKA_57,
          Object.class,
          issues
      );
    }
  }

  private void validateTopicExpression(Stage.Context context, List<Stage.ConfigIssue> issues) {
    topicEval = context.createELEval("topicExpression");
    topicVars = context.createELVars();
    ELUtils.validateExpression(
        topicEval,
        context.createELVars(),
        topicExpression,
        context,
        KafkaDestinationGroups.KAFKA.name(),
        KAFKA_CONFIG_BEAN_PREFIX + "topicExpression",
        KafkaErrors.KAFKA_61,
        Object.class,
        issues
    );
  }

  private void validateTopicExistence(
      Stage.Context context,
      List<Stage.ConfigIssue> issues,
      String topic
  ) {

    boolean valid = kafkaValidationUtil.validateTopicExistence(
      context,
      KafkaDestinationGroups.KAFKA.name(),
      KAFKA_CONFIG_BEAN_PREFIX + "topic",
      kafkaBrokers,
      metadataBrokerList,
      topic,
      kafkaProducerConfigs == null ?
          Collections.<String, Object>emptyMap() :
          new HashMap<String, Object>(kafkaProducerConfigs),
      issues,
      true
    );
    if(valid) {
      try {
        int partitionCount = kafkaValidationUtil.getPartitionCount(
            metadataBrokerList,
            topic,
            kafkaProducerConfigs == null ?
                Collections.<String, Object>emptyMap() :
                new HashMap<String, Object>(kafkaProducerConfigs),
            messageSendMaxRetries,
            retryBackoffMs
        );
        if(partitionCount != -1) {
          allowedTopics.add(topic);
          topicPartitionMap.put(topic, partitionCount);
        }
      } catch (Exception e) {
        issues.add(
            context.createConfigIssue(
                KafkaDestinationGroups.KAFKA.name(),
                "topic",
                KafkaErrors.KAFKA_11,
                topic,
                metadataBrokerList,
                e.toString(),
                e
            )
        );
      }
    }
  }

  private void validateTopicWhiteList(
      Stage.Context context,
      List<Stage.ConfigIssue> issues,
      List<HostAndPort> kafkaBrokers
  ) {
    //if runtimeTopicResolution then topic white list cannot be empty
    if(topicWhiteList == null || topicWhiteList.isEmpty()) {
      issues.add(
          context.createConfigIssue(
              KafkaDestinationGroups.KAFKA.name(),
              KAFKA_CONFIG_BEAN_PREFIX + "topicWhiteList",
              KafkaErrors.KAFKA_64
          )
      );
    } else if (topicWhiteList.equals("*")) {
      allowAllTopics = true;
    } else {
      //Must be comma separated list of topic names
      if(kafkaBrokers != null && !kafkaBrokers.isEmpty()) {
        String[] topics = topicWhiteList.split(",");
        for (String t : topics) {
          t = t.trim();
          //validate supplied topic names in the white list
          validateTopicExistence(context, issues, t);
        }
      }
    }
  }

  private void validateKafkaProducerConfigs(Stage.Context context, List<Stage.ConfigIssue> issues) {
    if(kafkaProducerConfigs != null) {
      if(kafkaProducerConfigs.containsKey(MESSAGE_SEND_MAX_RETRIES_KEY)) {
        try {
          messageSendMaxRetries = Integer.parseInt(
              kafkaProducerConfigs.get(MESSAGE_SEND_MAX_RETRIES_KEY).toString().trim()
          );
        } catch (NullPointerException | NumberFormatException e) {
          issues.add(
              context.createConfigIssue(
                  KafkaDestinationGroups.KAFKA.name(),
                  KAFKA_CONFIG_BEAN_PREFIX + "kafkaProducerConfigs",
                  KafkaErrors.KAFKA_66,
                  MESSAGE_SEND_MAX_RETRIES_KEY,
                  "integer",
                  e.toString(),
                  e
              )
          );
        }
        if(messageSendMaxRetries < 0) {
          issues.add(
              context.createConfigIssue(
                  KafkaDestinationGroups.KAFKA.name(),
                  KAFKA_CONFIG_BEAN_PREFIX + "kafkaProducerConfigs",
                  KafkaErrors.KAFKA_66,
                  MESSAGE_SEND_MAX_RETRIES_KEY,
                  "integer"
              )
          );
        }
      } else {
        messageSendMaxRetries = MESSAGE_SEND_MAX_RETRIES_DEFAULT;
      }

      if(kafkaProducerConfigs.containsKey(RETRY_BACKOFF_MS_KEY)) {
        try {
          retryBackoffMs = Long.parseLong(kafkaProducerConfigs.get(RETRY_BACKOFF_MS_KEY).toString().trim());
        } catch (NullPointerException | NumberFormatException e) {
          issues.add(
              context.createConfigIssue(
                  KafkaDestinationGroups.KAFKA.name(),
                  KAFKA_CONFIG_BEAN_PREFIX + "kafkaProducerConfigs",
                  KafkaErrors.KAFKA_66,
                  RETRY_BACKOFF_MS_KEY,
                  "long",
                  e.toString(),
                  e
              )
          );
        }
        if(retryBackoffMs < 0) {
          issues.add(
              context.createConfigIssue(
                  KafkaDestinationGroups.KAFKA.name(),
                  KAFKA_CONFIG_BEAN_PREFIX + "kafkaProducerConfigs",
                  KafkaErrors.KAFKA_66,
                  RETRY_BACKOFF_MS_KEY,
                  "long"
              )
          );
        }
      } else {
        retryBackoffMs = RETRY_BACKOFF_MS_DEFAULT;
      }
    }
  }


  String getPartitionKey(Record record, String topic) throws StageException {
    String partitionKey = "";
    if(partitionStrategy == PartitionStrategy.EXPRESSION) {
      RecordEL.setRecordInContext(partitionVars, record);
      try {
        int p = partitionEval.eval(partitionVars, partition, Integer.class);
        if (p < 0 || p >= topicPartitionMap.get(topic)) {
          throw new StageException(KafkaErrors.KAFKA_56, partition, topic, topicPartitionMap.get(topic),
            record.getHeader().getSourceId());
        }
        partitionKey = Integer.toString(p);
      } catch (ELEvalException e) {
        throw new StageException(KafkaErrors.KAFKA_54, partition, record.getHeader().getSourceId(), e.toString());
      }
    } else if(partitionStrategy == PartitionStrategy.DEFAULT) {
      RecordEL.setRecordInContext(partitionVars, record);
      try {
        partitionKey = partitionEval.eval(partitionVars, partition, String.class);
      } catch (ELEvalException e) {
        throw new StageException(KafkaErrors.KAFKA_54, partition, record.getHeader().getSourceId(), e.getMessage());
      }
    }
    return partitionKey;
  }


  /**
   * Returns the topic given the record.
   *
   * Returns the configured topic or statically evaluated topic in case runtime resolution is not required.
   *
   * If runtime resolution is required then the following is done:
   * 1.Resolve the topic name by evaluating the topic expression
   * 2.If the white list does not contain topic name and white list is not configured with "*" throw StageException
   *    and the record will be handled based on the OnError configuration for the stage
   * 3.If the topic is encountered for the first time make sure the topic exists and get the number of partitions and
   *   store it in the topicPartitionMap.
   *   Note that if the white list was provided then this would already be computed before we start processing records.
   *   This code is required to handle the scenario where the user sets a value of "*" in the white list and then an
   *   invalid topic is encountered. We could of course skip validation, send records to the broker and rely on the
   *   exception from the broker. But if the user has configured retry and backOff we will miss this.
   *
   * @param record
   * @return
   * @throws StageException
   */
  String getTopic(Record record) throws StageException {
    String result = topic;
    if(runtimeTopicResolution) {
      RecordEL.setRecordInContext(topicVars, record);
      try {
        result = topicEval.eval(topicVars, topicExpression, String.class);
        if (result == null || result.isEmpty()) {
          throw new StageException(KafkaErrors.KAFKA_62, topicExpression, record.getHeader().getSourceId());
        }
        if (!allowedTopics.contains(result) && !allowAllTopics) {
          throw new StageException(KafkaErrors.KAFKA_65, result, record.getHeader().getSourceId());
        }
        if (!topicPartitionMap.containsKey(result)) {
          //allowAllTopics must be true to get here
          //Encountered topic name for the very first time.
          //get topic metadata and cache it
          if (invalidTopicMap.containsKey(result)) {
            //Invalid topic previously seen
            throw invalidTopicMap.get(result);
          }
          //Never seen this topic name before
          try {
            int partitionCount = kafkaValidationUtil.getPartitionCount(
                metadataBrokerList,
                result,
                kafkaProducerConfigs == null ?
                    Collections.<String, Object>emptyMap() :
                    new HashMap<String, Object>(kafkaProducerConfigs),
                messageSendMaxRetries,
                retryBackoffMs
            );
            topicPartitionMap.put(result, partitionCount);
          } catch (StageException s) {
            invalidTopicMap.put(result, s);
            throw s;
          }
        }
        if (topicPartitionMap.keySet().size() % TOPIC_WARN_SIZE == 0) {
          LOG.warn("Encountered {} different topics while running the pipeline", topicPartitionMap.keySet().size());
        }
      } catch (ELEvalException e) {
        throw new StageException(KafkaErrors.KAFKA_63, topicExpression, record.getHeader().getSourceId(), e.toString());
      }
    }
    return result;
  }

  SdcKafkaProducer getKafkaProducer() {
    return kafkaProducer;
  }

}
