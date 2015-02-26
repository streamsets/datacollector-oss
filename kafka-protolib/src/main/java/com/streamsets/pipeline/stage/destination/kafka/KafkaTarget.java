/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.kafka;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.el.ELEvaluator;
import com.streamsets.pipeline.el.ELRecordSupport;
import com.streamsets.pipeline.el.ELStringSupport;
import com.streamsets.pipeline.lib.Errors;
import com.streamsets.pipeline.lib.KafkaBroker;
import com.streamsets.pipeline.lib.KafkaUtil;
import com.streamsets.pipeline.lib.recordserialization.CsvRecordToString;
import com.streamsets.pipeline.lib.recordserialization.DataCollectorRecordToString;
import com.streamsets.pipeline.lib.recordserialization.JsonRecordToString;
import com.streamsets.pipeline.lib.recordserialization.LogRecordToString;
import com.streamsets.pipeline.lib.recordserialization.RecordToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.jsp.el.ELException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class KafkaTarget extends BaseTarget {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaTarget.class);

  private final String metadataBrokerList;
  private final String topic;
  private final PartitionStrategy partitionStrategy;
  private final String partition;
  private final DataFormat payloadType;
  private final Map<String, String> kafkaProducerConfigs;
  private final CsvMode csvFileFormat;
  private final List<String> fieldPaths;
  private final String fieldPath;

  private KafkaProducer kafkaProducer;
  private long recordCounter = 0;
  private ELEvaluator elEvaluator;
  private ELEvaluator.Variables variables;
  private RecordToString recordToString;

  public KafkaTarget(String metadataBrokerList, String topic, PartitionStrategy partitionStrategy, String partition,
                     DataFormat payloadType, Map<String, String> kafkaProducerConfigs, CsvMode csvFileFormat,
                     List<String> fieldPaths, String fieldPath) {
    this.metadataBrokerList = metadataBrokerList;
    this.topic = topic;
    this.partitionStrategy = partitionStrategy;
    this.partition = partition;
    this.payloadType = payloadType;
    this.kafkaProducerConfigs = kafkaProducerConfigs;
    this.csvFileFormat = csvFileFormat;
    this.fieldPaths = fieldPaths;
    this.fieldPath = fieldPath;
  }

  @Override
  protected List<ConfigIssue> validateConfigs() throws StageException {
    List<ConfigIssue> issues = super.validateConfigs();

    //metadata broker list should be one or more <host>:<port> separated by a comma
    List<KafkaBroker> kafkaBrokers = KafkaUtil.validateBrokerList(issues, metadataBrokerList,
      Groups.KAFKA.name(), "metadataBrokerList", getContext());

    //topic should exist
    KafkaUtil.validateTopic(issues, kafkaBrokers, getContext(), Groups.KAFKA.name(), "topic", topic,
      metadataBrokerList);

    //validate partition expression
    validatePartitionExpression(issues);

    //payload type and payload specific configuration
    validateDataFormatAndSpecificConfig(issues, payloadType, getContext(), Groups.KAFKA.name(), "payloadType");

    //kafka producer configs
    //We do not validate this, just pass it down to Kafka

    return issues;
  }

  @Override
  public void init() throws StageException {
    kafkaProducer = new KafkaProducer(topic, metadataBrokerList,
      payloadType, partitionStrategy, kafkaProducerConfigs);
    kafkaProducer.init();

    createRecordToStringInstance(getFieldPathToNameMapping());
  }

  private Map<String, String> getFieldPathToNameMapping() {
    Map<String, String> fieldPathToNameMapping = new LinkedHashMap<>();
    if(fieldPaths != null && !fieldPaths.isEmpty()) {
      for (String fieldPath : fieldPaths) {
        fieldPathToNameMapping.put(fieldPath, null);
      }
    }
    return fieldPathToNameMapping;
  }

  private void createRecordToStringInstance(Map<String, String> fieldNameToPathMap) {
    switch(payloadType) {
      case SDC_JSON:
        recordToString = new DataCollectorRecordToString(getContext());
        break;
      case DELIMITED:
        recordToString = new CsvRecordToString(csvFileFormat.getFormat(), false);
        break;
      case TEXT:
        recordToString = new LogRecordToString(fieldPath);
        break;
      case JSON:
        recordToString = new JsonRecordToString();
        break;
    }
    recordToString.setFieldPathToNameMapping(fieldNameToPathMap);
  }

  @Override
  public void write(Batch batch) throws StageException {
    long batchRecordCounter = 0;
    Iterator<Record> records = batch.getRecords();
    if(records.hasNext()) {
      while (records.hasNext()) {
        Record r = records.next();

        String partitionKey = getPartitionKey(r);
        if(partitionKey == null) {
          //record in error
          continue;
        }
        if(!partitionKey.isEmpty() && !validatePartition(r, partitionKey)) {
          continue;
        }
        byte[] message;
        try {
          message = serializeRecord(r);
        } catch (StageException e) {
          LOG.warn(e.getMessage());
          getContext().toError(r, e.getMessage());
          continue;
        }
        kafkaProducer.enqueueMessage(message, partitionKey);
        batchRecordCounter++;
      }

      kafkaProducer.write();
      recordCounter += batchRecordCounter;
      LOG.info("Wrote {} records in this batch.", batchRecordCounter);
    }
  }

  private boolean validatePartition(Record r, String partitionKey) {
    int partition = -1;
    try {
      partition = Integer.parseInt(partitionKey);
    } catch (NumberFormatException e) {
      LOG.warn(Errors.KAFKA_55.getMessage(), partitionKey, topic, e.getMessage());
      getContext().toError(r, Errors.KAFKA_55, partitionKey, topic, e.getMessage(), e);
      return false;
    }
    //partition number is an integer starting from 0 ... n-1, where n is the number of partitions for topic t
    if(partition < 0 || partition >= kafkaProducer.getNumberOfPartitions()) {
      LOG.warn(Errors.KAFKA_56.getMessage(), partition, topic, kafkaProducer.getNumberOfPartitions());
      getContext().toError(r, Errors.KAFKA_56, partition, topic, kafkaProducer.getNumberOfPartitions());
      return false;
    }
    return true;
  }

  private String getPartitionKey(Record record) throws StageException {
    if(partitionStrategy == PartitionStrategy.EXPRESSION) {
      ELRecordSupport.setRecordInContext(variables, record);
      Object result;
      try {
        result = elEvaluator.eval(variables, partition);
      } catch (ELException e) {
        LOG.warn(Errors.KAFKA_54.getMessage(), partition, record.getHeader().getSourceId(), e.getMessage());
        getContext().toError(record, Errors.KAFKA_54, partition, record.getHeader().getSourceId(),
          e.getMessage(), e);
        return null;
      }
      return result.toString();
    }
    return "";
  }

  @Override
  public void destroy() {
    LOG.info("Wrote {} number of records to Kafka Broker", recordCounter);
    if(kafkaProducer != null) {
      kafkaProducer.destroy();
    }
  }

  private byte[] serializeRecord(Record r) throws StageException {
    LOG.debug("Serializing record {}", r.getHeader().getSourceId());
    String recordString = recordToString.toString(r);
    if(recordString != null) {
      return recordString.getBytes();
    }
    return null;
  }

  /****************************************************/
  /******** Validation Specific to Kafka Target *******/
  /****************************************************/

  private void validatePartitionExpression(List<ConfigIssue> issues) {
    if (partitionStrategy == PartitionStrategy.EXPRESSION) {
      variables = new ELEvaluator.Variables();
      elEvaluator = new ELEvaluator();
      ELRecordSupport.registerRecordFunctions(elEvaluator);
      ELStringSupport.registerStringFunctions(elEvaluator);
      validateExpressions(issues);
    }
  }

  private void validateExpressions(List<Stage.ConfigIssue> issues) {
    Record record = new Record(){
      @Override
      public Header getHeader() {
        return null;
      }

      @Override
      public Field get() {
        return null;
      }

      @Override
      public Field set(Field field) {
        return null;
      }

      @Override
      public Field get(String fieldPath) {
        return null;
      }

      @Override
      public Field delete(String fieldPath) {
        return null;
      }

      @Override
      public boolean has(String fieldPath) {
        return false;
      }

      @Override
      public Set<String> getFieldPaths() {
        return null;
      }

      @Override
      public Field set(String fieldPath, Field newField) {
        return null;
      }
    };

    ELRecordSupport.setRecordInContext(variables, record);
    try {
      elEvaluator.eval(variables, partition);
    } catch (ELException ex) {
      issues.add(getContext().createConfigIssue(Groups.KAFKA.name(), "partition",
        Errors.KAFKA_57, partition, ex.getMessage()));
    }
  }

  private void validateDataFormatAndSpecificConfig(List<Stage.ConfigIssue> issues, DataFormat dataFormat,
                                                   Stage.Context context, String groupName, String configName) {
    switch (dataFormat) {
      case TEXT:
        //required the field configuration to be set and it is "/" by default
        if(fieldPath == null || fieldPath.isEmpty()) {
          issues.add(getContext().createConfigIssue(Groups.TEXT.name(), "fieldPath", Errors.KAFKA_58));
        }
        break;
      case JSON:
        //no-op
        break;
      case DELIMITED:
        //delimiter format is dropdown and it has a default value
        //"fields" option must be specified
        if (fieldPaths == null || fieldPaths.isEmpty()) {
          issues.add(getContext().createConfigIssue(Groups.TEXT.name(), "fieldPaths", Errors.KAFKA_59, "fieldPaths"));
        }
        break;
      case SDC_JSON:
        //no-op
        break;
      case XML:
      default:
        issues.add(context.createConfigIssue(groupName, configName, Errors.KAFKA_02, dataFormat));
        //XML is not supported for KafkaTarget
    }
  }

}
