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
package com.streamsets.pipeline.stage.origin.kafka;

import com.google.common.net.HostAndPort;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ToErrorContext;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.kafka.api.ConsumerFactorySettings;
import com.streamsets.pipeline.kafka.api.KafkaOriginGroups;
import com.streamsets.pipeline.kafka.api.SdcKafkaConsumer;
import com.streamsets.pipeline.kafka.api.SdcKafkaConsumerFactory;
import com.streamsets.pipeline.kafka.api.SdcKafkaValidationUtil;
import com.streamsets.pipeline.kafka.api.SdcKafkaValidationUtilFactory;
import com.streamsets.pipeline.lib.kafka.KafkaConstants;
import com.streamsets.pipeline.lib.kafka.KafkaErrors;
import com.streamsets.datacollector.security.kafka.KafkaKerberosUtil;
import com.streamsets.pipeline.lib.kafka.KafkaSecurityUtil;
import com.streamsets.pipeline.lib.kafka.MessageKeyUtil;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.RecoverableDataParserException;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.streamsets.pipeline.Utils.KAFKA_CONFIG_BEAN_PREFIX;
import static com.streamsets.pipeline.Utils.KAFKA_CONNECTION_CONFIG_BEAN_PREFIX;
import static com.streamsets.pipeline.Utils.KAFKA_DATA_FORMAT_CONFIG_BEAN_PREFIX;

public abstract class BaseKafkaSource extends BaseSource implements OffsetCommitter {
  private static final Logger LOG = LoggerFactory.getLogger(BaseKafkaSource.class);

  protected final KafkaConfigBean conf;
  protected SdcKafkaConsumer kafkaConsumer;
  private KafkaKerberosUtil kafkaKerberosUtil;
  private String keytabFileName;

  private ErrorRecordHandler errorRecordHandler;
  private DataParserFactory parserFactory;
  private int originParallelism = 0;
  private SdcKafkaValidationUtil kafkaValidationUtil;
  protected static final String ZOOKEEPER_CONNECT = "zookeeperConnect";
  protected static final String CONSUMER_GROUP = "consumerGroup";
  protected static final String TOPIC = "topic";
  protected static final String BROKER_LIST = "metadataBrokerList";
  protected static final String KAFKA_CONFIGS = "kafkaConsumerConfigs";


  public BaseKafkaSource(KafkaConfigBean conf) {
    this.conf = conf;
    kafkaValidationUtil = SdcKafkaValidationUtilFactory.getInstance().create();
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = new ArrayList<>();
    kafkaKerberosUtil = new KafkaKerberosUtil(getContext().getConfiguration());
    Utils.checkNotNull(kafkaKerberosUtil, "kafkaKerberosUtil");
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());

    if (conf.topic == null || conf.topic.isEmpty()) {
      issues.add(
          getContext().createConfigIssue(
              KafkaOriginGroups.KAFKA.name(),
              KAFKA_CONFIG_BEAN_PREFIX + TOPIC,
              KafkaErrors.KAFKA_05
          )
      );
    }
    //maxWaitTime
    if (conf.maxWaitTime < 1) {
      issues.add(
          getContext().createConfigIssue(
              KafkaOriginGroups.KAFKA.name(),
              KAFKA_CONFIG_BEAN_PREFIX + "maxWaitTime",
              KafkaErrors.KAFKA_35
          )
      );
    }

    conf.init(getContext(), issues);

    conf.dataFormatConfig.init(
        getContext(),
        conf.dataFormat,
        KafkaOriginGroups.KAFKA.name(),
        KAFKA_DATA_FORMAT_CONFIG_BEAN_PREFIX,
        issues
    );
    if (conf.dataFormat == DataFormat.XML && conf.produceSingleRecordPerMessage) {
      issues.add(
          getContext().createConfigIssue(
              KafkaOriginGroups.KAFKA.name(),
              KAFKA_CONFIG_BEAN_PREFIX + "produceSingleRecordPerMessage",
              KafkaErrors.KAFKA_40
          )
      );
    }

    parserFactory = conf.dataFormatConfig.getParserFactory();

    // Validate broker config
    List<HostAndPort> kafkaBrokers = kafkaValidationUtil.validateKafkaBrokerConnectionString(
        issues,
        conf.connectionConfig.connection.metadataBrokerList,
        KafkaOriginGroups.KAFKA.name(),
        KAFKA_CONNECTION_CONFIG_BEAN_PREFIX + BROKER_LIST,
        getContext()
    );

    KafkaSecurityUtil.validateAdditionalProperties(
        conf.connectionConfig.connection.securityConfig,
        conf.kafkaConsumerConfigs,
        KafkaOriginGroups.KAFKA.name(),
        KAFKA_CONFIG_BEAN_PREFIX + KAFKA_CONFIGS,
        issues,
        getContext()
    );

    KafkaSecurityUtil.addSecurityConfigs(conf.connectionConfig.connection.securityConfig, conf.kafkaConsumerConfigs);

    if (conf.connectionConfig.connection.securityConfig.provideKeytab && kafkaValidationUtil.isProvideKeytabAllowed(issues, getContext())) {
      keytabFileName = kafkaKerberosUtil.saveUserKeytab(
          conf.connectionConfig.connection.securityConfig.userKeytab.get(),
          conf.connectionConfig.connection.securityConfig.userPrincipal,
          conf.kafkaConsumerConfigs,
          issues,
          getContext()
      );
    }

    try {
      int partitionCount = kafkaValidationUtil.getPartitionCount(
          conf.connectionConfig.connection.metadataBrokerList,
          conf.topic,
          new HashMap<String, Object>(conf.kafkaConsumerConfigs),
          3,
          1000
      );
      if (partitionCount < 1) {
        issues.add(getContext().createConfigIssue(KafkaOriginGroups.KAFKA.name(),
            KAFKA_CONFIG_BEAN_PREFIX + TOPIC,
            KafkaErrors.KAFKA_42,
            conf.topic
        ));
      } else {
        //cache the partition count as parallelism for future use
        originParallelism = partitionCount;
      }
    } catch (StageException e) {
      issues.add(getContext().createConfigIssue(KafkaOriginGroups.KAFKA.name(),
          KAFKA_CONFIG_BEAN_PREFIX + TOPIC,
          KafkaErrors.KAFKA_41,
          conf.topic,
          e.toString(),
          e
      ));
    }

    // Validate zookeeper config
    kafkaValidationUtil.validateZkConnectionString(
        issues,
        conf.zookeeperConnect,
        KafkaOriginGroups.KAFKA.name(),
        KAFKA_CONFIG_BEAN_PREFIX + ZOOKEEPER_CONNECT,
        getContext()
    );

    //consumerGroup
    if (conf.consumerGroup == null || conf.consumerGroup.isEmpty()) {
      issues.add(
          getContext().createConfigIssue(
              KafkaOriginGroups.KAFKA.name(),
              KAFKA_CONFIG_BEAN_PREFIX + CONSUMER_GROUP,
              KafkaErrors.KAFKA_33
          )
      );
    }

    //validate connecting to kafka
    if (issues.isEmpty()) {
      Map<String, Object> kafkaConsumerConfigs = new HashMap<>();
      kafkaConsumerConfigs.putAll(conf.kafkaConsumerConfigs);
      kafkaConsumerConfigs.put(KafkaConstants.KEY_DESERIALIZER_CLASS_CONFIG, conf.keyDeserializer.getKeyClass());
      kafkaConsumerConfigs.put(KafkaConstants.VALUE_DESERIALIZER_CLASS_CONFIG, conf.valueDeserializer.getValueClass());
      kafkaConsumerConfigs.put(KafkaConstants.CONFLUENT_SCHEMA_REGISTRY_URL_CONFIG,
          conf.dataFormatConfig.schemaRegistryUrls
      );

      String userInfo = conf.dataFormatConfig.basicAuth.get();
      if (!userInfo.isEmpty()) {
        kafkaConsumerConfigs.put(KafkaConstants.BASIC_AUTH_CREDENTIAL_SOURCE, KafkaConstants.USER_INFO);
        kafkaConsumerConfigs.put(KafkaConstants.BASIC_AUTH_USER_INFO, userInfo);
      }

      ConsumerFactorySettings settings = new ConsumerFactorySettings(
          conf.zookeeperConnect,
          conf.connectionConfig.connection.metadataBrokerList,
          conf.topic,
          conf.maxWaitTime,
          getContext(),
          kafkaConsumerConfigs,
          conf.consumerGroup,
          conf.maxBatchSize,
          conf.timestampsEnabled,
          conf.kafkaAutoOffsetReset.name(),
          conf.timestampToSearchOffsets
      );
      kafkaConsumer = SdcKafkaConsumerFactory.create(settings).create();
      kafkaConsumer.validate(issues, getContext());
    }

    return issues;
  }

  // This API is being used by ClusterKafkaSource
  public int getParallelism() throws StageException {
    if (originParallelism == 0) {
      //origin parallelism is not yet calculated
      originParallelism = kafkaValidationUtil.getPartitionCount(
          conf.connectionConfig.connection.metadataBrokerList,
          conf.topic,
          new HashMap<String, Object>(conf.kafkaConsumerConfigs),
          3,
          1000
      );
      if(originParallelism < 1) {
        throw new StageException(KafkaErrors.KAFKA_42, conf.topic);
      }
    }
    return originParallelism;
  }

  protected List<Record> processKafkaMessageDefault(
      Object messageKey,
      String partition,
      long offset,
      String messageId,
      byte[] payload
  ) throws StageException {
    return processKafkaMessageDefault(messageKey, partition, offset, messageId, payload, 0, "");
  }

  protected List<Record> processKafkaMessageDefault(
      Object messageKey,
      String partition,
      long offset,
      String messageId,
      byte[] payload,
      long timestamp,
      String timestampType
  ) throws StageException {
    List<Record> records = new ArrayList<>();
    if (payload == null) {
      LOG.debug("NULL value (tombstone) read, it has been discarded");
    } else {

      try (DataParser parser = Utils.checkNotNull(parserFactory, "Initialization failed").getParser(messageId, payload)) {

        Record record = null;
        boolean recoverableExceptionHit;
        do {
          try {
            recoverableExceptionHit = false;
            record = parser.parse();
          } catch (RecoverableDataParserException e) {
            recoverableExceptionHit = true;
            handleException(getContext(), getContext(), messageId, e, e.getUnparsedRecord());

            //Go to next record
            continue;
          }

          if (record != null) {
            if (timestamp != 0) {
              record.getHeader().setAttribute(HeaderAttributeConstants.KAFKA_TIMESTAMP, String.valueOf(timestamp));
              record.getHeader().setAttribute(HeaderAttributeConstants.KAFKA_TIMESTAMP_TYPE, timestampType);
            }
            record.getHeader().setAttribute(HeaderAttributeConstants.TOPIC, conf.topic);
            record.getHeader().setAttribute(HeaderAttributeConstants.PARTITION, partition);
            record.getHeader().setAttribute(HeaderAttributeConstants.OFFSET, String.valueOf(offset));

            try {
              MessageKeyUtil.handleMessageKey(messageKey,
                  conf.keyCaptureMode,
                  record,
                  conf.keyCaptureField,
                  conf.keyCaptureAttribute
              );
            } catch (Exception e) {
              throw new OnRecordErrorException(record, KafkaErrors.KAFKA_201, partition, offset, e.getMessage(), e);
            }

            records.add(record);
          }
        } while (record != null || recoverableExceptionHit);
      } catch (IOException | DataParserException ex) {
        Record record = getContext().createRecord(messageId);
        record.set(Field.create(payload));
        String exMessage = ex.toString();
        if (ex.getClass().toString().equals("class com.fasterxml.jackson.core.JsonParseException")) {
          // SDC-15723. Trying to catch this exception is hard, as its behaviour is not expected.
          // This workaround using its String seems to be the best way to do it.
          exMessage = "Cannot parse JSON from record";
        }
        errorRecordHandler.onError(new OnRecordErrorException(record, KafkaErrors.KAFKA_37, messageId, exMessage, ex));
      }
      if (conf.produceSingleRecordPerMessage) {
        List<Field> list = new ArrayList<>();
        records.forEach(record -> list.add(record.get()));
        if (!list.isEmpty()) {
          Record record = records.get(0);
          record.set(Field.create(Field.Type.LIST, list));
          records.clear();
          records.add(record);
        }
      }
    }
    return records;
  }

  @Override
  public void destroy() {
    kafkaKerberosUtil.deleteUserKeytabIfExists(keytabFileName, getContext());
  }

  private static void handleException(
      Stage.Context stageContext,
      ToErrorContext errorContext,
      String messageId,
      Exception ex,
      Record record
  ) throws StageException {
    switch (stageContext.getOnErrorRecord()) {
      case DISCARD:
        break;
      case TO_ERROR:
        errorContext.toError(record, KafkaErrors.KAFKA_37, messageId, ex.toString(), ex);
        break;
      case STOP_PIPELINE:
        if (ex instanceof StageException) {
          throw (StageException) ex;
        } else {
          throw new StageException(KafkaErrors.KAFKA_37, messageId, ex.toString(), ex);
        }
      default:
        throw new IllegalStateException(Utils.format("Unknown on error value '{}'", stageContext, ex));
    }
  }
}
