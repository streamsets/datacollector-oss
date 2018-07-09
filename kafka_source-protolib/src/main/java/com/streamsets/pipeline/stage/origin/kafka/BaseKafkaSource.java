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
import com.streamsets.pipeline.api.StageException;
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
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.streamsets.pipeline.Utils.KAFKA_CONFIG_BEAN_PREFIX;
import static com.streamsets.pipeline.Utils.KAFKA_DATA_FORMAT_CONFIG_BEAN_PREFIX;

public abstract class BaseKafkaSource extends BaseSource implements OffsetCommitter {

  protected final KafkaConfigBean conf;
  protected SdcKafkaConsumer kafkaConsumer;

  private ErrorRecordHandler errorRecordHandler;
  private DataParserFactory parserFactory;
  private int originParallelism = 0;
  private SdcKafkaValidationUtil kafkaValidationUtil;
  protected static final String ZOOKEEPER_CONNECT = "zookeeperConnect";
  protected static final String CONSUMER_GROUP = "consumerGroup";
  protected static final String TOPIC = "topic";
  protected static final String BROKER_LIST = "metadataBrokerList";


  public BaseKafkaSource(KafkaConfigBean conf) {
    this.conf = conf;
    kafkaValidationUtil = SdcKafkaValidationUtilFactory.getInstance().create();
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = new ArrayList<>();
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
        conf.metadataBrokerList,
        KafkaOriginGroups.KAFKA.name(),
        KAFKA_CONFIG_BEAN_PREFIX + BROKER_LIST,
        getContext()
    );

    try {
      int partitionCount = kafkaValidationUtil.getPartitionCount(
          conf.metadataBrokerList,
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
      ConsumerFactorySettings settings = new ConsumerFactorySettings(
          conf.zookeeperConnect,
          conf.metadataBrokerList,
          conf.topic,
          conf.maxWaitTime,
          getContext(),
          kafkaConsumerConfigs,
          conf.consumerGroup,
          conf.maxBatchSize
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
          conf.metadataBrokerList,
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

  protected List<Record> processKafkaMessageDefault(String partition, long offset, String messageId, byte[] payload)
    throws StageException {
    List<Record> records = new ArrayList<>();
    if (payload == null) {
      Record record = getContext().createRecord(messageId);
      record.set(Field.create(payload));
      errorRecordHandler.onError(
          new OnRecordErrorException(
              record,
              KafkaErrors.KAFKA_74,
              messageId
          )
      );
      return records;
    }
    try (DataParser parser = Utils.checkNotNull(parserFactory, "Initialization failed").getParser(messageId, payload)) {
      Record record = parser.parse();
      while (record != null) {
        record.getHeader().setAttribute(HeaderAttributeConstants.TOPIC, conf.topic);
        record.getHeader().setAttribute(HeaderAttributeConstants.PARTITION, partition);
        record.getHeader().setAttribute(HeaderAttributeConstants.OFFSET, String.valueOf(offset));

        records.add(record);
        record = parser.parse();
      }
    } catch (IOException|DataParserException ex) {
      Record record = getContext().createRecord(messageId);
      record.set(Field.create(payload));
      errorRecordHandler.onError(
          new OnRecordErrorException(
              record,
              KafkaErrors.KAFKA_37,
              messageId,
              ex.toString(),
              ex
          )
      );
    }
    if (conf.produceSingleRecordPerMessage) {
      List<Field> list = new ArrayList<>();
      records.forEach(record -> list.add(record.get()));
      if(!list.isEmpty()) {
        Record record = records.get(0);
        record.set(Field.create(Field.Type.LIST, list));
        records.clear();
        records.add(record);
      }
    }
    return records;
  }

}
