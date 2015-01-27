/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.kafka;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.ChooserMode;
import com.streamsets.pipeline.api.ComplexField;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.FieldSelector;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.el.ELBasicSupport;
import com.streamsets.pipeline.el.ELEvaluator;
import com.streamsets.pipeline.el.ELRecordSupport;
import com.streamsets.pipeline.el.ELStringSupport;
import com.streamsets.pipeline.el.ELUtils;
import com.streamsets.pipeline.lib.recordserialization.CsvRecordToString;
import com.streamsets.pipeline.lib.recordserialization.JsonRecordToString;
import com.streamsets.pipeline.lib.recordserialization.LogRecordToString;
import com.streamsets.pipeline.lib.recordserialization.RecordToString;
import com.streamsets.pipeline.lib.util.KafkaStageLibError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.jsp.el.ELException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@GenerateResourceBundle
@StageDef(version="0.0.1",
  label="Kafka Producer",
  icon="kafka.png")
@ConfigGroups(value = KafkaTarget.KafkaTargetConfigGroups.class)
public class KafkaTarget extends BaseTarget {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaTarget.class);

  @ConfigDef(required = true,
    type = ConfigDef.Type.STRING,
    description = "The Kafka topic from which the messages must be read",
    label = "Topic",
    defaultValue = "topicName",
    group = "KAFKA_CONNECTION_PROPERTIES")
  public String topic;

  @ConfigDef(required = true,
    type = ConfigDef.Type.MODEL,
    description = "Indicates the strategy to select a partition while writing a message." +
      "This option is activated only if a negative integer is supplied as the value for partition.",
    label = "Partition Strategy",
    defaultValue = "ROUND_ROBIN",
    group = "KAFKA_CONNECTION_PROPERTIES")
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = PartitionStrategyChooserValues.class)
  public PartitionStrategy partitionStrategy;

  @ConfigDef(required = false,
    type = ConfigDef.Type.EL_NUMBER,
    description = "Expression that determines the partition of Kafka topic to which the messages must be written",
    label = "Partition Expression",
    defaultValue = "${0}",
    dependsOn = "partitionStrategy",
    triggeredByValue = {"EXPRESSION"},
    group = "KAFKA_CONNECTION_PROPERTIES")
  public String partition;

  @ConfigDef(required = true,
    type = ConfigDef.Type.MAP,
    label = "Constants for expressions",
    description = "Defines constant values available to all expressions",
    dependsOn = "partitionStrategy",
    triggeredByValue = {"EXPRESSION"})
  public Map<String, ?> constants;

  @ConfigDef(required = true,
    type = ConfigDef.Type.STRING,
    description = "This is for bootstrapping and the producer will only use it for getting metadata " +
      "(topics, partitions and replicas). The socket connections for sending the actual data will be established " +
      "based on the broker information returned in the metadata. The format is host1:port1,host2:port2, and the list " +
      "can be a subset of brokers or a VIP pointing to a subset of brokers. " +
      "Please specify more than one broker information if known.",
    label = "Metadata Broker List",
    defaultValue = "localhost:9092",
  group = "KAFKA_CONNECTION_PROPERTIES")
  public String metadataBrokerList;

  @ConfigDef(required = true,
    type = ConfigDef.Type.MODEL,
    description = "Type of data sent as kafka message payload",
    label = "Payload Type",
    defaultValue = "LOG",
    group = "KAFKA_CONNECTION_PROPERTIES")
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = PayloadTypeChooserValues.class)
  public PayloadType payloadType;

  @ConfigDef(required = false,
    type = ConfigDef.Type.MAP,
    description = "Additional configuration properties which will be used by the underlying Kafka Producer. " +
     "The following options, if specified, are ignored : \"metadata.broker.list\", \"producer.type\", " +
      "\"key.serializer.class\", \"partitioner.class\", \"serializer.class\".",
    defaultValue = "",
    label = "Kafka Producer Configuration Properties",
   group = "KAFKA_ADVANCED_CONFIGURATION")
  public Map<String, String> kafkaProducerConfigs;

  /********  For CSV Content  ***********/

  @ConfigDef(required = false,
    type = ConfigDef.Type.MODEL,
    label = "CSV Format",
    description = "The specific CSV format of the files",
    defaultValue = "CSV",
    dependsOn = "payloadType", triggeredByValue = {"CSV"},
    group = "CSV_PROPERTIES")
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = CvsFileModeChooserValues.class)
  public CsvFileMode csvFileFormat;

  @ConfigDef(required = true,
    type = ConfigDef.Type.MODEL,
    description = "Field to columnName mapping configuration",
    label = "Field to Name Mapping",
    dependsOn = "payloadType",
    triggeredByValue = {"CSV"},
    defaultValue = "LOG",
    group = "CSV_PROPERTIES")
  @ComplexField
  public List<FieldPathToNameMappingConfig> fieldPathToNameMappingConfigList;


  public static class FieldPathToNameMappingConfig {

    @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      label = "Field Path",
      description = "The fields which must be written to the target")
    @FieldSelector(singleValued = true)
    public String fieldPath;

    @ConfigDef(required = true,
      type = ConfigDef.Type.STRING,
      label = "CSV column columnName",
      description = "The columnName which must be used for the fields in the target")
    public String columnName;
  }

  public enum KafkaTargetConfigGroups implements ConfigGroups.Groups {
    KAFKA_CONNECTION_PROPERTIES("Kafka Connection Configuration"),
    KAFKA_ADVANCED_CONFIGURATION("Kafka Advanced Configuration"),
    JSON_PROPERTIES("JSON Data Properties"),
    CSV_PROPERTIES("CSV Data Properties"),
    LOG_PROPERTIES("Log Data Properties");

    private final String label;

    private KafkaTargetConfigGroups(String label) {
      this.label = label;
    }

    public String getLabel() {
      return this.label;
    }
  }

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
      variables = ELUtils.parseConstants(constants);
      elEvaluator = new ELEvaluator();
      ELBasicSupport.registerBasicFunctions(elEvaluator);
      ELRecordSupport.registerRecordFunctions(elEvaluator);
      ELStringSupport.registerStringFunctions(elEvaluator);
      validateExpressions();
    }

    createRecordToStringInstance(getFieldPathToNameMapping());
  }

  private Map<String, String> getFieldPathToNameMapping() {
    Map<String, String> fieldPathToNameMapping = new LinkedHashMap<>();
    if(fieldPathToNameMappingConfigList != null && !fieldPathToNameMappingConfigList.isEmpty()) {
      for (FieldPathToNameMappingConfig fieldPathToNameMappingConfig : fieldPathToNameMappingConfigList) {
        fieldPathToNameMapping.put(fieldPathToNameMappingConfig.fieldPath, fieldPathToNameMappingConfig.columnName);
      }
    }
    return fieldPathToNameMapping;
  }

  private void createRecordToStringInstance(Map<String, String> fieldNameToPathMap) {
    switch(payloadType) {
      case JSON:
        recordToString = new JsonRecordToString();
        break;
      case CSV:
        recordToString = new CsvRecordToString(csvFileFormat.getFormat());
        break;
      case LOG:
        recordToString = new LogRecordToString();
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
      LOG.error(KafkaStageLibError.KFK_0357.getMessage(), partition, ex.getMessage());
      throw new StageException(KafkaStageLibError.KFK_0357, partition, ex.getMessage(), ex);
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
          getContext().toError(r, e);
          continue;
        }
        kafkaProducer.enqueueMessage(message, partitionKey);
        batchRecordCounter++;
      }

      kafkaProducer.write();
      recordCounter += batchRecordCounter;
    }
  }

  private boolean validatePartition(Record r, String partitionKey) {
    int partition = -1;
    try {
      partition = Integer.parseInt(partitionKey);
    } catch (NumberFormatException e) {
      LOG.warn(KafkaStageLibError.KFK_0355.getMessage(), partitionKey, topic, e.getMessage());
      getContext().toError(r, KafkaStageLibError.KFK_0355, partitionKey, topic, e.getMessage(), e);
      return false;
    }
    //partition number is an integer starting from 0 ... n-1, where n is the number of partitions for topic t
    if(partition < 0 || partition >= kafkaProducer.getNumberOfPartitions()) {
      LOG.warn(KafkaStageLibError.KFK_0356.getMessage(), partition, topic, kafkaProducer.getNumberOfPartitions());
      getContext().toError(r, KafkaStageLibError.KFK_0356, partition, topic, kafkaProducer.getNumberOfPartitions());
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
        LOG.warn(KafkaStageLibError.KFK_0354.getMessage(), partition, record.getHeader().getSourceId(), e.getMessage());
        getContext().toError(record, KafkaStageLibError.KFK_0354, partition, record.getHeader().getSourceId(),
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
    return recordToString.toString(r).getBytes();
  }
}
