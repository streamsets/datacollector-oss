/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.kafka;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.ChooserMode;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.FieldSelector;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.el.ELEvaluator;
import com.streamsets.pipeline.el.ELRecordSupport;
import com.streamsets.pipeline.el.ELStringSupport;
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

@GenerateResourceBundle
@StageDef(
    version = "1.0.0",
    label = "Kafka Producer",
    description = "Writes data to Kafka",
    icon = "kafka.png")
@ConfigGroups(value = KafkaTarget.Groups.class)
public class KafkaTarget extends BaseTarget {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaTarget.class);

  public enum Groups implements Label {
    KAFKA("Kafka"),
    DELIMITED("Delimited"),
    TEXT("Text")

    ;

    private final String label;

    private Groups(String label) {
      this.label = label;
    }

    public String getLabel() {
      return this.label;
    }
  }

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "localhost:9092",
      label = "Broker URIs",
      description = "Comma-separated list of URIs for brokers that write to the topic.  Use the format " +
                    "<HOST>:<PORT>. To ensure a connection, enter as many as possible.",
      displayPosition = 10,
      group = "KAFKA"
  )
  public String metadataBrokerList;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "topicName",
      label = "Topic",
      description = "",
      displayPosition = 20,
      group = "KAFKA"
  )
  public String topic;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "ROUND_ROBIN",
      label = "Partition Strategy",
      description = "Strategy to select a partition to write to",
      displayPosition = 30,
      group = "KAFKA"
  )
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = PartitionStrategyChooserValues.class)
  public PartitionStrategy partitionStrategy;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.EL_NUMBER,
      defaultValue = "${0}",
      label = "Partition Expression",
      description = "Expression that determines the partition to write to",
      displayPosition = 40,
      group = "KAFKA",
      dependsOn = "partitionStrategy",
      triggeredByValue = "EXPRESSION"
  )
  public String partition;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "SDC_JSON",
      label = "Data Format",
      description = "",
      displayPosition = 50,
      group = "KAFKA"
  )
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = ProducerDataFormatChooserValues.class)
  public DataFormat payloadType;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      defaultValue = "",
      label = "Kafka Configuration",
      description = "Additional Kafka properties to pass to the underlying Kafka producer",
      displayPosition = 60,
      group = "KAFKA"
  )
  public Map<String, String> kafkaProducerConfigs;

  /********  For DELIMITED Content  ***********/

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      defaultValue = "CSV",
      label = "Delimiter Format",
      description = "",
      displayPosition = 100,
      group = "DELIMITED",
      dependsOn = "payloadType",
      triggeredByValue = "DELIMITED"
  )
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = CvsFileModeChooserValues.class)
  public CsvFileMode csvFileFormat;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "TEXT",
      label = "Fields",
      description = "Fields to write to Kafka",
      displayPosition = 110,
      group = "DELIMITED",
      dependsOn = "payloadType",
      triggeredByValue = "DELIMITED"
  )
  @FieldSelector
  public List<String> fieldPaths;

  /********  For TEXT Content  ***********/

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "/",
    label = "Field",
    description = "Field to write data to Kafka",
    displayPosition = 120,
    group = "TEXT",
    dependsOn = "payloadType",
    triggeredByValue = "TEXT"
  )
  @FieldSelector(singleValued = true)
  public String fieldPath;

  private KafkaProducer kafkaProducer;
  private long recordCounter = 0;
  private ELEvaluator elEvaluator;
  private ELEvaluator.Variables variables;
  private RecordToString recordToString;

  @Override
  public void init() throws StageException {
    kafkaProducer = new KafkaProducer(topic, metadataBrokerList,
      payloadType, partitionStrategy, kafkaProducerConfigs);
    kafkaProducer.init();

    if (partitionStrategy == PartitionStrategy.EXPRESSION) {
      variables = new ELEvaluator.Variables();
      elEvaluator = new ELEvaluator();
      ELRecordSupport.registerRecordFunctions(elEvaluator);
      ELStringSupport.registerStringFunctions(elEvaluator);
      validateExpressions();
    }

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

  private void validateExpressions() throws StageException {
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
      LOG.error(Errors.KAFKA_23.getMessage(), partition, ex.getMessage(), ex);
      throw new StageException(Errors.KAFKA_23, partition, ex.getMessage(), ex);
    }
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
      LOG.warn(Errors.KAFKA_21.getMessage(), partitionKey, topic, e.getMessage());
      getContext().toError(r, Errors.KAFKA_21, partitionKey, topic, e.getMessage(), e);
      return false;
    }
    //partition number is an integer starting from 0 ... n-1, where n is the number of partitions for topic t
    if(partition < 0 || partition >= kafkaProducer.getNumberOfPartitions()) {
      LOG.warn(Errors.KAFKA_22.getMessage(), partition, topic, kafkaProducer.getNumberOfPartitions());
      getContext().toError(r, Errors.KAFKA_22, partition, topic, kafkaProducer.getNumberOfPartitions());
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
        LOG.warn(Errors.KAFKA_20.getMessage(), partition, record.getHeader().getSourceId(), e.getMessage());
        getContext().toError(record, Errors.KAFKA_20, partition, record.getHeader().getSourceId(),
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
}
