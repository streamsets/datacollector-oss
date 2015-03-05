/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.kafka;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.el.ELEvaluator;
import com.streamsets.pipeline.el.ELRecordSupport;
import com.streamsets.pipeline.el.ELStringSupport;
import com.streamsets.pipeline.lib.Errors;
import com.streamsets.pipeline.lib.KafkaBroker;
import com.streamsets.pipeline.lib.KafkaUtil;
import com.streamsets.pipeline.lib.generator.CharDataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.delimited.DelimitedCharDataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.text.TextCharDataGeneratorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.jsp.el.ELException;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class KafkaTarget extends BaseTarget {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaTarget.class);

  private final String metadataBrokerList;
  private final String topic;
  private final PartitionStrategy partitionStrategy;
  private final String partition;
  private final DataFormat payloadType;
  private final Map<String, String> kafkaProducerConfigs;
  private final CsvMode csvFileFormat;
  private final String textFieldPath;

  private KafkaProducer kafkaProducer;
  private long recordCounter = 0;
  private ELEvaluator elEvaluator;
  private ELEvaluator.Variables variables;
  private CharDataGeneratorFactory generatorFactory;

  public KafkaTarget(String metadataBrokerList, String topic, PartitionStrategy partitionStrategy, String partition,
                     DataFormat payloadType, Map<String, String> kafkaProducerConfigs, CsvMode csvFileFormat,
                      String textFieldPath) {
    this.metadataBrokerList = metadataBrokerList;
    this.topic = topic;
    this.partitionStrategy = partitionStrategy;
    this.partition = partition;
    this.payloadType = payloadType;
    this.kafkaProducerConfigs = kafkaProducerConfigs;
    this.csvFileFormat = csvFileFormat;
    this.textFieldPath = textFieldPath;
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

    generatorFactory = createDataGeneratorFactory();
  }

  private CharDataGeneratorFactory createDataGeneratorFactory() {
    CharDataGeneratorFactory.Builder builder = new CharDataGeneratorFactory.Builder(getContext(),
                                                                                    payloadType.getGeneratorFormat());
    switch(payloadType) {
      case SDC_JSON:
        break;
      case DELIMITED:
        builder.setMode(csvFileFormat);
        builder.setMode(CsvHeader.NO_HEADER);
        builder.setConfig(DelimitedCharDataGeneratorFactory.REPLACE_NEWLINES_KEY, false);
        break;
      case TEXT:
        builder.setConfig(TextCharDataGeneratorFactory.FIELD_PATH_KEY, textFieldPath);
        builder.setConfig(TextCharDataGeneratorFactory.EMPTY_LINE_IF_NULL_KEY, false);
        break;
      case JSON:
        builder.setMode(JsonMode.MULTIPLE_OBJECTS);
        break;
    }
    return builder.build();
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
        } catch (IOException| StageException e) {
          getContext().toError(r, e);
          continue;
        }
        kafkaProducer.enqueueMessage(message, partitionKey);
        batchRecordCounter++;
      }

      kafkaProducer.write();
      recordCounter += batchRecordCounter;
      LOG.debug("Wrote {} records in this batch.", batchRecordCounter);
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

  private byte[] serializeRecord(Record r) throws StageException, IOException {
    StringWriter sw = new StringWriter(1024);
    DataGenerator generator = generatorFactory.getGenerator(sw);
    generator.write(r);
    generator.close();
    return sw.toString().getBytes();
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
    Record record = getContext().createRecord("validateConfigs");

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
        if(textFieldPath == null || textFieldPath.isEmpty()) {
          issues.add(getContext().createConfigIssue(Groups.TEXT.name(), "fieldPath", Errors.KAFKA_58));
        }
        break;
      case JSON:
      case DELIMITED:
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
