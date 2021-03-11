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
package com.streamsets.pipeline.stage.destination.kafka;

import com.google.common.base.Joiner;
import com.google.common.net.HostAndPort;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.kafka.api.KafkaDestinationGroups;
import com.streamsets.pipeline.kafka.api.KafkaOriginGroups;
import com.streamsets.pipeline.kafka.api.PartitionStrategy;
import com.streamsets.pipeline.kafka.api.ProducerFactorySettings;
import com.streamsets.pipeline.kafka.api.ProducerKeyFormat;
import com.streamsets.pipeline.kafka.api.SdcKafkaProducer;
import com.streamsets.pipeline.kafka.api.SdcKafkaProducerFactory;
import com.streamsets.pipeline.kafka.api.SdcKafkaValidationUtil;
import com.streamsets.pipeline.kafka.api.SdcKafkaValidationUtilFactory;
import com.streamsets.pipeline.lib.el.AvroEL;
import com.streamsets.pipeline.lib.el.Base64EL;
import com.streamsets.pipeline.lib.el.ELUtils;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.kafka.connection.KafkaConnectionConfigBean;
import com.streamsets.pipeline.lib.kafka.KafkaConstants;
import com.streamsets.pipeline.lib.kafka.KafkaErrors;
import com.streamsets.datacollector.security.kafka.KafkaKerberosUtil;
import com.streamsets.pipeline.lib.kafka.KafkaSecurityUtil;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.streamsets.pipeline.config.AvroSchemaLookupMode.ID;
import static com.streamsets.pipeline.config.AvroSchemaLookupMode.SUBJECT;
import static com.streamsets.pipeline.config.DestinationAvroSchemaSource.REGISTRY;
import static com.streamsets.pipeline.stage.destination.kafka.Serializer.CONFLUENT;
import static org.apache.commons.lang3.StringUtils.isEmpty;

public class KafkaTargetConfig {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaTargetConfig.class);

  private static final String MESSAGE_SEND_MAX_RETRIES_KEY = "message.send.max.retries";
  private static final int MESSAGE_SEND_MAX_RETRIES_DEFAULT = 10;
  private static final String RETRY_BACKOFF_MS_KEY = "retry.backoff.ms";
  private static final long RETRY_BACKOFF_MS_DEFAULT = 1000;
  private static final int TOPIC_WARN_SIZE = 500;
  private static final String KAFKA_CONFIG_BEAN_PREFIX = "conf.";
  public static final String KAFKA_CONNECTION_CONFIG_BEAN_PREFIX = "conf.connectionConfig.connection.";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "JSON",
      label = "Data Format",
      displayPosition = 1,
      group = "DATA_FORMAT"
  )
  @ValueChooserModel(ProducerDataFormatChooserValues.class)
  public DataFormat dataFormat;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "STRING",
      label = "Message Key Format",
      displayPosition = 2,
      group = "DATA_FORMAT"
  )
  @ValueChooserModel(ProducerKeyFormatChooserValues.class)
  public ProducerKeyFormat messageKeyFormat;

  @ConfigDefBean(groups = {"DATA_FORMAT"})
  public DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();

  @ConfigDefBean()
  public KafkaConnectionConfigBean connectionConfig = new KafkaConnectionConfigBean();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Runtime Topic Resolution",
      description = "Select topic at runtime based on the field values in the record",
      displayPosition = 15,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
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
      displayMode = ConfigDef.DisplayMode.ADVANCED,
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
      displayMode = ConfigDef.DisplayMode.ADVANCED,
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
      displayMode = ConfigDef.DisplayMode.BASIC,
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
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0"
  )
  @ValueChooserModel(PartitionStrategyChooserValues.class)
  public PartitionStrategy partitionStrategy;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "${0}",
      label = "Partition Expression",
      description = "When using the default partition strategy, enter an expression to evaluate the partition key " +
          "from record, which will be used with hash function to determine the topic's partition. " +
          "When using Expression, enter an expression that determines the partition number. ",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
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
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0"
  )
  public boolean singleMessagePerBatch;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Key Serializer",
      description = "Method used to serialize the Kafka message key. Set to Confluent to embed the Avro schema ID in each message the destination writes.",
      defaultValue = "STRING",
      displayPosition = 440,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      dependsOn = "messageKeyFormat",
      triggeredByValue = "AVRO",
      group = "KAFKA"
  )
  @ValueChooserModel(KeySerializerChooserValues.class)
  public Serializer keySerializer = Serializer.STRING;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Value Serializer",
      description = "Method used to serialize the Kafka message value. Set to Confluent to embed the Avro schema ID in each message the destination writes.",
      defaultValue = "DEFAULT",
      displayPosition = 450,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      dependsOn = "dataFormat",
      triggeredByValue = "AVRO",
      group = "KAFKA"
  )
  @ValueChooserModel(ValueSerializerChooserValues.class)
  public Serializer valueSerializer = Serializer.DEFAULT;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      defaultValue = "",
      label = "Kafka Configuration",
      description = "Additional Kafka properties to pass to the underlying Kafka producer",
      displayPosition = 60,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0"
  )
  public Map<String, String> kafkaProducerConfigs = new HashMap<>();

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "${avro:decode(record:attribute('avroKeySchema'),base64:decodeBytes(record:attribute('kafkaMessageKey')))}",
      label = "Kafka Message Key",
      description = "The Kafka message key",
      displayPosition = 90,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      dependsOn = "messageKeyFormat",
      triggeredByValue = "AVRO",
      group = "#0",
      elDefs = {AvroEL.class, RecordEL.class, Base64EL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String avroMessageKey;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "${record:attribute('kafkaMessageKey')}",
      label = "Kafka Message Key",
      description = "The Kafka message key",
      displayPosition = 100,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0",
      dependsOn = "messageKeyFormat",
      triggeredByValue = "STRING",
      elDefs = {RecordEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String stringMessageKey;


  // Private members

  private SdcKafkaProducer kafkaProducer;
  private SdcKafkaValidationUtil kafkaValidationUtil;
  private ELEval partitionEval;
  private ELVars partitionVars;
  private ELEval topicEval;
  private ELVars topicVars;
  private ELVars messageKeyVars;
  private ELEval messageKeyEval;
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
  private String keytabFileName;
  private KafkaKerberosUtil kafkaKerberosUtil;

  public void init(Stage.Context context, List<Stage.ConfigIssue> issues) {
    init(context, this.dataFormat, false, issues);
  }

  public void init(Stage.Context context, boolean sendResponse, List<Stage.ConfigIssue> issues) {
    init(context, this.dataFormat, sendResponse, issues);
  }

  public void init(Stage.Context context, DataFormat dataFormat, boolean sendResponse, List<Stage.ConfigIssue> issues) {
    kafkaKerberosUtil = new KafkaKerberosUtil(context.getConfiguration());
    Utils.checkNotNull(kafkaKerberosUtil, "kafkaKerberosUtil");
    dataGeneratorFormatConfig.init(
        context,
        dataFormat,
        KafkaDestinationGroups.KAFKA.name(),
        KAFKA_CONFIG_BEAN_PREFIX + "dataGeneratorFormatConfig.",
        issues
    );

    validateMessageKeySerializerConfig(context, issues);
    validateMessageKeyExpression(context, issues);

    if (valueSerializer == CONFLUENT || keySerializer == CONFLUENT) {
      validateConfluentSerializerConfigs(context, issues);
    }

    // Configure serializers.
    kafkaProducerConfigs.put(KafkaConstants.KEY_SERIALIZER_CLASS_CONFIG, keySerializer.getKeyClass());
    kafkaProducerConfigs.put(KafkaConstants.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer.getValueClass());

    List<String> schemaRegistryUrls = new ArrayList<>();
    String userInfo = "";
    if (dataGeneratorFormatConfig.avroSchemaSource == REGISTRY &&
        !dataGeneratorFormatConfig.schemaRegistryUrls.isEmpty()) {
      schemaRegistryUrls = dataGeneratorFormatConfig.schemaRegistryUrls;
      userInfo = dataGeneratorFormatConfig.basicAuthUserInfo.get();
    } else if (dataGeneratorFormatConfig.registerSchema &&
        !dataGeneratorFormatConfig.schemaRegistryUrlsForRegistration.isEmpty()) {
      schemaRegistryUrls = dataGeneratorFormatConfig.schemaRegistryUrlsForRegistration;
      userInfo = dataGeneratorFormatConfig.basicAuthUserInfoForRegistration.get();
    }

    kafkaProducerConfigs.put(
        KafkaConstants.CONFLUENT_SCHEMA_REGISTRY_URL_CONFIG,
        Joiner.on(",").join(schemaRegistryUrls)
    );

    if (userInfo != null && !userInfo.isEmpty()) {
      kafkaProducerConfigs.put(KafkaConstants.BASIC_AUTH_CREDENTIAL_SOURCE, KafkaConstants.USER_INFO);
      kafkaProducerConfigs.put(KafkaConstants.BASIC_AUTH_USER_INFO, userInfo);
    }

    this.topicPartitionMap = new HashMap<>();
    this.allowedTopics = new HashSet<>();
    this.invalidTopicMap = new HashMap<>();
    allowAllTopics = false;
    kafkaValidationUtil = SdcKafkaValidationUtilFactory.getInstance().create();
    //metadata broker list should be one or more <host>:<port> separated by a comma
    kafkaBrokers = kafkaValidationUtil.validateKafkaBrokerConnectionString(
        issues,
        connectionConfig.connection.metadataBrokerList,
        KafkaDestinationGroups.KAFKA.name(),
         KAFKA_CONNECTION_CONFIG_BEAN_PREFIX + "metadataBrokerList",
        context
    );

    KafkaSecurityUtil.validateAdditionalProperties(
        connectionConfig.connection.securityConfig,
        kafkaProducerConfigs,
        KafkaOriginGroups.KAFKA.name(),
        KAFKA_CONFIG_BEAN_PREFIX + "kafkaProducerConfigs",
        issues,
        context
    );

    KafkaSecurityUtil.addSecurityConfigs(connectionConfig.connection.securityConfig, kafkaProducerConfigs);

    if (connectionConfig.connection.securityConfig.provideKeytab && kafkaValidationUtil.isProvideKeytabAllowed(issues, context)) {
      keytabFileName = kafkaKerberosUtil.saveUserKeytab(
          connectionConfig.connection.securityConfig.userKeytab.get(),
          connectionConfig.connection.securityConfig.userPrincipal,
          kafkaProducerConfigs,
          issues,
          context
      );
    }

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
          new HashMap<String, Object>(kafkaProducerConfigs),
          partitionStrategy,
          connectionConfig.connection.metadataBrokerList,
          dataFormat,
          sendResponse
      );
      kafkaProducer = SdcKafkaProducerFactory.create(settings).create();
      try {
        kafkaProducer.init();
      } catch (StageException ex) {
        issues.add(context.createConfigIssue(null, null, ex.getErrorCode(), ex.getParams()));
      }
    }
  }

  private void validateMessageKeySerializerConfig(Stage.Context context, List<Stage.ConfigIssue> issues){

    if(messageKeyFormat == ProducerKeyFormat.AVRO && keySerializer != CONFLUENT){
      issues.add(
          context.createConfigIssue(
              KafkaOriginGroups.KAFKA.name(),
              KAFKA_CONFIG_BEAN_PREFIX + "keySerializer",
              KafkaErrors.KAFKA_77,
              keySerializer
          )
      );
    }
  }

  private void validateConfluentSerializerConfigs(Stage.Context context, List<Stage.ConfigIssue> issues) {
    try {
      getClass().getClassLoader().loadClass(Serializer.CONFLUENT.getKeyClass());
    } catch (ClassNotFoundException ignored) { // NOSONAR
      issues.add(
          context.createConfigIssue(
              KafkaOriginGroups.KAFKA.name(),
              KAFKA_CONFIG_BEAN_PREFIX + "keyDeserializer",
              KafkaErrors.KAFKA_73
          )
      );
    }

    // If using Confluent Kafka Avro Serializer, user shouldn't check includeSchema.
    if (dataGeneratorFormatConfig.includeSchema) {
      issues.add(context.createConfigIssue(KafkaDestinationGroups.DATA_FORMAT.name(),
          "conf.dataGeneratorFormatConfig.includeSchema",
          KafkaErrors.KAFKA_70
      ));
    }

    if (dataGeneratorFormatConfig.schemaRegistryUrls.isEmpty() &&
        dataGeneratorFormatConfig.schemaRegistryUrlsForRegistration.isEmpty()) {
      issues.add(context.createConfigIssue(KafkaDestinationGroups.DATA_FORMAT.name(),
          "conf.dataGeneratorFormatConfig.schemaRegistryUrls",
          KafkaErrors.KAFKA_71
      ));
    }

    if (dataGeneratorFormatConfig.avroSchemaSource == REGISTRY) {
      if (dataGeneratorFormatConfig.schemaLookupMode == ID && dataGeneratorFormatConfig.schemaId < 1) {
        issues.add(context.createConfigIssue(KafkaDestinationGroups.DATA_FORMAT.name(),
            "conf.dataGeneratorFormatConfig.schemaId",
            KafkaErrors.KAFKA_72
        ));
      }
      if (dataGeneratorFormatConfig.schemaLookupMode == SUBJECT && isEmpty(dataGeneratorFormatConfig.subject)) {
        issues.add(context.createConfigIssue(KafkaDestinationGroups.DATA_FORMAT.name(),
            "conf.dataGeneratorFormatConfig.subject",
            KafkaErrors.KAFKA_72
        ));
      }
    } else if (isEmpty(dataGeneratorFormatConfig.subjectToRegister)) {
      issues.add(context.createConfigIssue(KafkaDestinationGroups.DATA_FORMAT.name(),
          "conf.dataGeneratorFormatConfig.subjectToRegister",
          KafkaErrors.KAFKA_72
      ));
    }
  }

  public void destroy() {
    if(kafkaProducer != null) {
      kafkaProducer.destroy();
    }
  }

  public void destroy(Stage.Context context) {
    kafkaKerberosUtil.deleteUserKeytabIfExists(keytabFileName, context);
    destroy();
  }

  private void validatePartitionExpression(Stage.Context context, List<Stage.ConfigIssue> issues) {
    if (partitionStrategy == PartitionStrategy.EXPRESSION || partitionStrategy == PartitionStrategy.DEFAULT) {
      partitionEval = context.createELEval("partition");
      partitionVars = context.createELVars();
      //There is no scope to provide partitionVars for kafka target as of today, create empty partitionVars
      ELUtils.validateExpression(partition,
          context,
          KafkaDestinationGroups.KAFKA.name(),
          KAFKA_CONFIG_BEAN_PREFIX + "partition",
          KafkaErrors.KAFKA_57, issues
      );
    }
  }

  private void validateTopicExpression(Stage.Context context, List<Stage.ConfigIssue> issues) {
    topicEval = context.createELEval("topicExpression");
    topicVars = context.createELVars();
    ELUtils.validateExpression(topicExpression,
        context,
        KafkaDestinationGroups.KAFKA.name(),
        KAFKA_CONFIG_BEAN_PREFIX + "topicExpression",
        KafkaErrors.KAFKA_61, issues
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
        connectionConfig.connection.metadataBrokerList,
        topic,
        new HashMap<String, Object>(kafkaProducerConfigs),
        issues,
        true
    );
    if(valid) {
      try {
        int partitionCount = kafkaValidationUtil.getPartitionCount(
            connectionConfig.connection.metadataBrokerList,
            topic,
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
                connectionConfig.connection.metadataBrokerList,
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
    if(isEmpty(topicWhiteList)) {
      issues.add(
          context.createConfigIssue(
              KafkaDestinationGroups.KAFKA.name(),
              KAFKA_CONFIG_BEAN_PREFIX + "topicWhiteList",
              KafkaErrors.KAFKA_64
          )
      );
    } else if ("*".equals(topicWhiteList)) {
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
    if(kafkaProducerConfigs.containsKey(MESSAGE_SEND_MAX_RETRIES_KEY)) {
      try {
        messageSendMaxRetries = Integer.parseInt(
            kafkaProducerConfigs.get(MESSAGE_SEND_MAX_RETRIES_KEY).trim()
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
        retryBackoffMs = Long.parseLong(kafkaProducerConfigs.get(RETRY_BACKOFF_MS_KEY).trim());
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


  Object getPartitionKey(Record record, String topic) throws StageException {
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

  private void validateMessageKeyExpression(Stage.Context context, List<Stage.ConfigIssue> issues) {
    messageKeyVars = context.createELVars();

    if (messageKeyFormat == ProducerKeyFormat.STRING){
      messageKeyEval = context.createELEval(stringMessageKey, RecordEL.class);
      ELUtils.validateExpression(
          stringMessageKey,
          context,
          KafkaDestinationGroups.KAFKA.name(),
          KAFKA_CONFIG_BEAN_PREFIX + "stringMessageKey",
          KafkaErrors.KAFKA_53,
          issues
      );
    }
    else if(messageKeyFormat == ProducerKeyFormat.AVRO){
      messageKeyEval = context.createELEval(avroMessageKey, RecordEL.class, Base64EL.class, AvroEL.class);
      ELUtils.validateExpression(
          avroMessageKey,
          context,
          KafkaDestinationGroups.KAFKA.name(),
          KAFKA_CONFIG_BEAN_PREFIX + "avroMessageKey",
          KafkaErrors.KAFKA_53,
          issues
      );
    }
  }

  Object getMessageKey(Record record) throws StageException {
    Object messageKey = null;
    RecordEL.setRecordInContext(messageKeyVars, record);

    if (messageKeyFormat == ProducerKeyFormat.STRING) {
      try {
         messageKey = messageKeyEval.eval(messageKeyVars, stringMessageKey, String.class);
      } catch (ELEvalException e) {
        throw new StageException(KafkaErrors.KAFKA_52, stringMessageKey, record.getHeader().getSourceId(), e.getMessage(), e);
      }
    } else if (messageKeyFormat == ProducerKeyFormat.AVRO) {
      try {
        messageKey = messageKeyEval.eval(messageKeyVars, avroMessageKey, GenericRecord.class);
      } catch (ELEvalException e) {
        throw new StageException(KafkaErrors.KAFKA_52, avroMessageKey, record.getHeader().getSourceId(), e.getMessage(), e);
      }
    }
    return messageKey;
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
        if (isEmpty(result)) {
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
            Map<String, Object> kafkaConfigs = new HashMap<>(kafkaProducerConfigs);
            kafkaValidationUtil.createTopicIfNotExists(result, kafkaConfigs, connectionConfig.connection.metadataBrokerList);
            int partitionCount = kafkaValidationUtil.getPartitionCount(
                connectionConfig.connection.metadataBrokerList,
                result,
                kafkaConfigs,
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
