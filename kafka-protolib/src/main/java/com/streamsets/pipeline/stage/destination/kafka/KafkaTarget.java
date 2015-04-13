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
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.lib.Errors;
import com.streamsets.pipeline.lib.KafkaBroker;
import com.streamsets.pipeline.lib.KafkaUtil;
import com.streamsets.pipeline.lib.el.ELUtils;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.generator.CharDataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactoryBuilder;
import com.streamsets.pipeline.lib.generator.delimited.DelimitedCharDataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.text.TextCharDataGeneratorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class KafkaTarget extends BaseTarget {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaTarget.class);

  private final String metadataBrokerList;
  private final String topic;
  private final PartitionStrategy partitionStrategy;
  private final String partition;
  private final DataFormat dataFormat;
  private final boolean singleMessagePerBatch;
  private final Map<String, String> kafkaProducerConfigs;
  private final CsvMode csvFileFormat;
  private final CsvHeader csvHeader;
  private final boolean csvReplaceNewLines;
  private final JsonMode jsonMode;
  private final String textFieldPath;
  private final boolean textEmptyLineIfNull;

  private KafkaProducer kafkaProducer;
  private long recordCounter = 0;
  private CharDataGeneratorFactory generatorFactory;
  private ELEval partitionEval;
  private ELVars variables;

  public KafkaTarget(String metadataBrokerList, String topic, PartitionStrategy partitionStrategy, String partition,
      DataFormat dataFormat, boolean singleMessagePerBatch, Map<String, String> kafkaProducerConfigs,
      CsvMode csvFileFormat, CsvHeader csvHeader, boolean csvReplaceNewLines, JsonMode jsonMode, String textFieldPath,
      boolean textEmptyLineIfNull) {
    this.metadataBrokerList = metadataBrokerList;
    this.topic = topic;
    this.partitionStrategy = partitionStrategy;
    this.partition = partition;
    this.dataFormat = dataFormat;
    this.singleMessagePerBatch = singleMessagePerBatch;
    this.kafkaProducerConfigs = kafkaProducerConfigs;
    this.csvFileFormat = csvFileFormat;
    this.csvHeader = csvHeader;
    this.csvReplaceNewLines = csvReplaceNewLines;
    this.jsonMode = jsonMode;
    this.textFieldPath = textFieldPath;
    this.textEmptyLineIfNull = textEmptyLineIfNull;
  }

  @Override
  protected List<ConfigIssue> validateConfigs() throws StageException {
    List<ConfigIssue> issues = super.validateConfigs();

    //metadata broker list should be one or more <host>:<port> separated by a comma
    List<KafkaBroker> kafkaBrokers = KafkaUtil.validateBrokerList(issues, metadataBrokerList,
                                                                  Groups.KAFKA.name(), "metadataBrokerList",
                                                                  getContext());

    //topic should exist
    KafkaUtil.validateTopic(issues, kafkaBrokers, getContext(), Groups.KAFKA.name(), "topic", topic,
                            metadataBrokerList);

    //validate partition expression
    validatePartitionExpression(issues);

    //payload type and payload specific configuration
    validateDataFormatAndSpecificConfig(issues, dataFormat, getContext(), Groups.KAFKA.name(), "dataFormat");

    //kafka producer configs
    //We do not validate this, just pass it down to Kafka

    return issues;
  }

  @Override
  public void init() throws StageException {
    kafkaProducer = new KafkaProducer(topic, metadataBrokerList,
                                      dataFormat, partitionStrategy, kafkaProducerConfigs);
    kafkaProducer.init();

    generatorFactory = createDataGeneratorFactory();
  }

  private CharDataGeneratorFactory createDataGeneratorFactory() {
    DataGeneratorFactoryBuilder builder = new DataGeneratorFactoryBuilder(getContext(),
      dataFormat.getGeneratorFormat());
    switch (dataFormat) {
      case SDC_JSON:
        break;
      case DELIMITED:
        builder.setMode(csvFileFormat);
        builder.setMode(csvHeader);
        builder.setConfig(DelimitedCharDataGeneratorFactory.REPLACE_NEWLINES_KEY, csvReplaceNewLines);
        break;
      case TEXT:
        builder.setConfig(TextCharDataGeneratorFactory.FIELD_PATH_KEY, textFieldPath);
        builder.setConfig(TextCharDataGeneratorFactory.EMPTY_LINE_IF_NULL_KEY, textEmptyLineIfNull);
        break;
      case JSON:
        builder.setMode(jsonMode);
        break;
    }
    return builder.build();
  }

  @Override
  public void write(Batch batch) throws StageException {
    if (singleMessagePerBatch) {
      writeOneMessagePerBatch(batch);
    } else {
      writeOneMessagePerRecord(batch);
    }
  }

  private void writeOneMessagePerBatch(Batch batch) throws StageException {
    int count = 0;
    Map<String, List<Record>> perPartition = new HashMap<>();
    Iterator<Record> records = batch.getRecords();
    while (records.hasNext()) {
      Record record = records.next();
      try {
        String partitionKey = getPartitionKey(record);
        List<Record> list = perPartition.get(partitionKey);
        if (list == null) {
          list = new ArrayList<>();
          perPartition.put(partitionKey, list);
        }
        list.add(record);
      } catch (Exception ex) {
        switch (getContext().getOnErrorRecord()) {
          case DISCARD:
            break;
          case TO_ERROR:
            getContext().toError(record, Errors.KAFKA_56, partition, topic, kafkaProducer.getNumberOfPartitions(),
                                 record.getHeader().getSourceId());
            break;
          case STOP_PIPELINE:
            throw new StageException(Errors.KAFKA_56, partition, topic, kafkaProducer.getNumberOfPartitions(),
                                     record.getHeader().getSourceId());
          default:
            throw new IllegalStateException(Utils.format("It should never happen. OnError '{}'",
              getContext().getOnErrorRecord()));
        }
      }
    }
    if (!perPartition.isEmpty()) {
      for (Map.Entry<String, List<Record>> entry : perPartition.entrySet()) {
        String partition = entry.getKey();
        List<Record> list = entry.getValue();
        StringWriter sw = new StringWriter(1024 * list.size());
        Record currentRecord = null;
        try {
          DataGenerator generator = generatorFactory.getGenerator(sw);
          for (Record record : list) {
            currentRecord = record;
            generator.write(record);
            count++;
          }
          currentRecord = null;
          generator.close();
          byte[] bytes = sw.toString().getBytes();
          kafkaProducer.enqueueMessage(bytes, partition);
          kafkaProducer.write();
          recordCounter += count;
          LOG.debug("Wrote {} records in this batch.", count);
        } catch (Exception ex) {
          String sourceId = (currentRecord == null) ? "<NONE>" : currentRecord.getHeader().getSourceId();
          switch (getContext().getOnErrorRecord()) {
            case DISCARD:
              LOG.warn("Could not serialize record '{}', all records from batch '{}' for partition '{}' are " +
                       "discarded, error: {}", sourceId, batch.getSourceOffset(), partition, ex.getMessage(), ex);
              break;
            case TO_ERROR:
              for (Record record : list) {
                getContext().toError(record, Errors.KAFKA_60, sourceId, batch.getSourceOffset(), partition,
                                     ex.getMessage(), ex);
              }
              break;
            case STOP_PIPELINE:
              throw new StageException(Errors.KAFKA_60, sourceId, batch.getSourceOffset(), partition, ex.getMessage(),
                                       ex);
            default:
              throw new IllegalStateException(Utils.format("It should never happen. OnError '{}'",
                                                           getContext().getOnErrorRecord()));
          }
        }
      }
    }
  }

  private void writeOneMessagePerRecord(Batch batch) throws StageException {
    long count = 0;
    Iterator<Record> records = batch.getRecords();
    while (records.hasNext()) {
      Record record = records.next();
      try {
        String partitionKey = getPartitionKey(record);
        StringWriter sw = new StringWriter(1024);
        DataGenerator generator = generatorFactory.getGenerator(sw);
        generator.write(record);
        generator.close();
        byte[] bytes = sw.toString().getBytes();
        kafkaProducer.enqueueMessage(bytes, partitionKey);
        count++;
      } catch (Exception ex) {
        switch (getContext().getOnErrorRecord()) {
          case DISCARD:
            break;
          case TO_ERROR:
            getContext().toError(record, ex);
            break;
          case STOP_PIPELINE:
            if (ex instanceof StageException) {
              throw (StageException) ex;
            } else {
              throw new StageException(Errors.KAFKA_51, record.getHeader().getSourceId(), ex.getMessage(), ex);
            }
          default:
            throw new IllegalStateException(Utils.format("It should never happen. OnError '{}'",
                                                         getContext().getOnErrorRecord()));
        }
      }
    }
    kafkaProducer.write();
    recordCounter += count;
    LOG.debug("Wrote {} records in this batch.", count);
  }

  private String getPartitionKey(Record record) throws StageException {
    String partitionKey = "";
    if(partitionStrategy == PartitionStrategy.EXPRESSION) {
      RecordEL.setRecordInContext(variables, record);
      try {
        int p = partitionEval.eval(variables, partition, Integer.class);
        if (p < 0 || p >= kafkaProducer.getNumberOfPartitions()) {
          throw new StageException(Errors.KAFKA_56, partition, topic, kafkaProducer.getNumberOfPartitions(),
                                   record.getHeader().getSourceId());
        }
        partitionKey = Integer.toString(p);
      } catch (ELEvalException e) {
        throw new StageException(Errors.KAFKA_54, partition, record.getHeader().getSourceId(), e.getMessage());
      }
    }
    return partitionKey;
  }

  @Override
  public void destroy() {
    LOG.info("Wrote {} number of records to Kafka Broker", recordCounter);
    if(kafkaProducer != null) {
      kafkaProducer.destroy();
    }
  }

  private ELEval createPartitionEval(ELContext elContext) {
    return elContext.createELEval("partition");
  }

  /****************************************************/
  /******** Validation Specific to Kafka Target *******/
  /****************************************************/

  private void validatePartitionExpression(List<ConfigIssue> issues) {
    if (partitionStrategy == PartitionStrategy.EXPRESSION) {
      partitionEval = createPartitionEval(getContext());
      //There is no scope to provide variables for kafka target as of today, create empty variables
      variables = getContext().createELVars();
      ELUtils.validateExpression(partitionEval, variables, partition, getContext(), Groups.KAFKA.name(),
                                 "partition", Errors.KAFKA_57, Object.class, issues);
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
      default:
        issues.add(context.createConfigIssue(groupName, configName, Errors.KAFKA_02, dataFormat));
        //XML is not supported for KafkaTarget
    }
  }

}
