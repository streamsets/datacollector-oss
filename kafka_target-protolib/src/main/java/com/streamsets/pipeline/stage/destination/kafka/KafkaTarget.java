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
import com.streamsets.pipeline.lib.KafkaConnectionException;
import com.streamsets.pipeline.lib.KafkaUtil;
import com.streamsets.pipeline.lib.el.ELUtils;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactoryBuilder;
import com.streamsets.pipeline.lib.generator.avro.AvroDataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.delimited.DelimitedDataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.text.TextDataGeneratorFactory;
import kafka.common.ErrorMapping;
import kafka.javaapi.TopicMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class KafkaTarget extends BaseTarget {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaTarget.class);

  private static final int TOPIC_WARN_SIZE = 500;
  private static final String MESSAGE_SEND_MAX_RETRIES_KEY = "message.send.max.retries";
  private static final int MESSAGE_SEND_MAX_RETRIES_DEFAULT = 10;
  private static final String RETRY_BACKOFF_MS_KEY = "retry.backoff.ms";
  private static final long RETRY_BACKOFF_MS_DEFAULT = 1000;

  private final String metadataBrokerList;
  private String topic;
  private String topicExpression;
  private final String topicWhiteList;
  private final boolean runtimeTopicResolution;
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
  private String charset;
  private final String avroSchema;
  private final  boolean includeSchema;

  private KafkaProducer kafkaProducer;
  private long recordCounter = 0;
  private DataGeneratorFactory generatorFactory;
  private Set<String> allowedTopics;
  private boolean allowAllTopics;
  private ELEval partitionEval;
  private ELVars partitionVars;
  private ELEval topicEval;
  private ELVars topicVars;

  /*cache topic name vs the number of partitions*/
  private Map<String, Integer> topicPartitionMap;
  /*cache invalid topic names encountered while resolving the topic names dynamically at runtime*/
  private Map<String, StageException> invalidTopicMap;
  private List<KafkaBroker> kafkaBrokers;
  /*holds the value of 'message.send.max.retries' supplied by the user or default value*/
  private int messageSendMaxRetries;
  /*holds the value of 'retry.backoff.ms' supplied by the user or the default value*/
  private long retryBackoffMs;

  public KafkaTarget(String metadataBrokerList, boolean runtimeTopicResolution, String topic, String topicExpression,
                     String topicWhiteList, PartitionStrategy partitionStrategy, String partition,
                     DataFormat dataFormat, String charset, boolean singleMessagePerBatch,
                     Map<String, String> kafkaProducerConfigs, CsvMode csvFileFormat, CsvHeader csvHeader,
                     boolean csvReplaceNewLines, JsonMode jsonMode, String textFieldPath, boolean textEmptyLineIfNull,
                     String avroSchema,  boolean includeSchema) {
    this.metadataBrokerList = metadataBrokerList;
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
    this.charset = charset;
    this.runtimeTopicResolution = runtimeTopicResolution;
    this.topic = topic;
    this.topicExpression = topicExpression;
    this.topicWhiteList = topicWhiteList;
    this.avroSchema = avroSchema;
    this.includeSchema = includeSchema;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    this.topicPartitionMap = new HashMap<>();
    this.allowedTopics = new HashSet<>();
    this.invalidTopicMap = new HashMap<>();
    allowAllTopics = false;

    //metadata broker list should be one or more <host>:<port> separated by a comma
    kafkaBrokers = KafkaUtil.validateConnectionString(issues, metadataBrokerList,
                                                                  Groups.KAFKA.name(), "metadataBrokerList",
                                                                  getContext());

    //check if the topic contains EL expression with record: functions
    //If yes, then validate the EL expression. Do not validate for existence of topic
    boolean validateTopicExists = !runtimeTopicResolution;

    if(runtimeTopicResolution) {
      //EL containing record: functions - make sure the expression is valid and parses correctly
      if(topicExpression == null || topicExpression.trim().isEmpty()) {
        issues.add(getContext().createConfigIssue(Groups.KAFKA.name(), "topicExpression", Errors.KAFKA_05));
      }
      validateTopicExpression(issues);
      //Also a topic white list is expected in this case, validate the list
      validateTopicWhiteList(issues, kafkaBrokers);
    } else {
      //check if the topic contains EL expression other than record: functions. It could be str: or constants
      //If yes, then evaluate expression as it is static.
      //Then validate for topic existence
      if (topic.startsWith("${")) {
        //EL with constants or String functions
        //evaluate expression and validate topic
        topicEval = getContext().createELEval("topic");
        topicVars = getContext().createELVars();
        try {
          topic = topicEval.eval(topicVars, topic, String.class);
        } catch (Exception ex) {
          validateTopicExists = false;
          issues.add(getContext().createConfigIssue(Groups.KAFKA.name(), "topic", Errors.KAFKA_61, topic,
            ex.getMessage(), ex));
        }
      }

      if(validateTopicExists) {
        validateTopicExistence(issues, topic);
      }
    }

    //validate partition expression
    validatePartitionExpression(issues);

    //payload type and payload specific configuration
    validateDataFormatAndSpecificConfig(issues, dataFormat, getContext(), Groups.KAFKA.name(), "dataFormat");

    //kafka producer configs
    validateKafkaProducerConfigs(issues);

    return issues;
  }

  @Override
  public void initX() throws StageException {
    kafkaProducer = new KafkaProducer(metadataBrokerList, dataFormat, partitionStrategy, kafkaProducerConfigs);
    kafkaProducer.init();
    generatorFactory = createDataGeneratorFactory();
  }

  private DataGeneratorFactory createDataGeneratorFactory() {
    DataGeneratorFactoryBuilder builder = new DataGeneratorFactoryBuilder(getContext(),
      dataFormat.getGeneratorFormat());
    if(charset == null || charset.trim().isEmpty()) {
      charset = "UTF-8";
    }
    builder.setCharset(Charset.forName(charset));
    switch (dataFormat) {
      case SDC_JSON:
        break;
      case DELIMITED:
        builder.setMode(csvFileFormat);
        builder.setMode(csvHeader);
        builder.setConfig(DelimitedDataGeneratorFactory.REPLACE_NEWLINES_KEY, csvReplaceNewLines);
        break;
      case TEXT:
        builder.setConfig(TextDataGeneratorFactory.FIELD_PATH_KEY, textFieldPath);
        builder.setConfig(TextDataGeneratorFactory.EMPTY_LINE_IF_NULL_KEY, textEmptyLineIfNull);
        break;
      case JSON:
        builder.setMode(jsonMode);
        break;
      case AVRO:
        builder.setConfig(AvroDataGeneratorFactory.SCHEMA_KEY, avroSchema);
        builder.setConfig(AvroDataGeneratorFactory.INCLUDE_SCHEMA_KEY, includeSchema);
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
    //Map of topic->(partition->Records)
    Map<String, Map<String, List<Record>>> perTopic = new HashMap<>();
    Iterator<Record> records = batch.getRecords();
    while (records.hasNext()) {
      boolean topicError = true;
      boolean partitionError = true;
      Record record = records.next();
      String topic = null;
      String partitionKey = null;
      try {
        topic = getTopic(record);
        topicError = false;
        partitionKey = getPartitionKey(record, topic);
        partitionError = false;
      } catch (KafkaConnectionException ex) {
        //Kafka connection exception is thrown when the client cannot connect to the list of brokers
        //even after retrying with backoff as specified in the retry and backoff config options
        //In this case we fail pipeline.
        throw ex;
      } catch (StageException ex) {
        switch (getContext().getOnErrorRecord()) {
          case DISCARD:
            break;
          case TO_ERROR:
            getContext().toError(record, ex);
            break;
          case STOP_PIPELINE:
            throw ex;
          default:
            throw new IllegalStateException(Utils.format("It should never happen. OnError '{}'",
              getContext().getOnErrorRecord()));
        }
      }
      if(!topicError && !partitionError) {
        Map<String, List<Record>> perPartition = perTopic.get(topic);
        if (perPartition == null) {
          perPartition = new HashMap<>();
          perTopic.put(topic, perPartition);
        }
        List<Record> list = perPartition.get(partitionKey);
        if (list == null) {
          list = new ArrayList<>();
          perPartition.put(partitionKey, list);
        }
        list.add(record);
      }
    }
    if (!perTopic.isEmpty()) {
      for( Map.Entry<String, Map<String, List<Record>>> topicEntry : perTopic.entrySet()) {
        String entryTopic = topicEntry.getKey();
        Map<String, List<Record>> perPartition = topicEntry.getValue();
        if(perPartition != null) {
          for (Map.Entry<String, List<Record>> entry : perPartition.entrySet()) {
            String partition = entry.getKey();
            List<Record> list = entry.getValue();
            ByteArrayOutputStream baos = new ByteArrayOutputStream(1024 * list.size());
            Record currentRecord = null;
            try {
              DataGenerator generator = generatorFactory.getGenerator(baos);
              for (Record record : list) {
                currentRecord = record;
                generator.write(record);
                count++;
              }
              currentRecord = null;
              generator.close();
              byte[] bytes = baos.toByteArray();
              kafkaProducer.enqueueMessage(entryTopic, bytes, partition);
            } catch (Exception ex) {
              //clear the message list
              kafkaProducer.getMessageList().clear();
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
            kafkaProducer.write();
            recordCounter += count;
            LOG.debug("Wrote {} records in this batch.", count);
          }
        }
      }
    }
  }

  private void writeOneMessagePerRecord(Batch batch) throws StageException {
    long count = 0;
    Iterator<Record> records = batch.getRecords();
    List<Record> recordList = new ArrayList<>();
    while (records.hasNext()) {
      Record record = records.next();
      recordList.add(record);
      try {
        String topic = getTopic(record);
        String partitionKey = getPartitionKey(record, topic);
        ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
        DataGenerator generator = generatorFactory.getGenerator(baos);
        generator.write(record);
        generator.close();
        byte[] bytes = baos.toByteArray();
        kafkaProducer.enqueueMessage(topic, bytes, partitionKey);
        count++;
      } catch (KafkaConnectionException ex) {
        //Kafka connection exception is thrown when the client cannot connect to the list of brokers
        //even after retrying with backoff as specified in the retry and backoff config options
        //In this case we fail pipeline.
        throw ex;
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

  private String getPartitionKey(Record record, String topic) throws StageException {
    String partitionKey = "";
    if(partitionStrategy == PartitionStrategy.EXPRESSION) {
      RecordEL.setRecordInContext(partitionVars, record);
      try {
        int p = partitionEval.eval(partitionVars, partition, Integer.class);
        if (p < 0 || p >= topicPartitionMap.get(topic)) {
          throw new StageException(Errors.KAFKA_56, partition, topic, topicPartitionMap.get(topic),
            record.getHeader().getSourceId());
        }
        partitionKey = Integer.toString(p);
      } catch (ELEvalException e) {
        throw new StageException(Errors.KAFKA_54, partition, record.getHeader().getSourceId(), e.getMessage());
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
  private String getTopic(Record record) throws StageException {
    String result = topic;
    if(runtimeTopicResolution) {
      RecordEL.setRecordInContext(topicVars, record);
      try {
        result = topicEval.eval(topicVars, topicExpression, String.class);
        if (result == null || result.isEmpty()) {
          throw new StageException(Errors.KAFKA_62, topicExpression, record.getHeader().getSourceId());
        }
        if (!allowedTopics.contains(result) && !allowAllTopics) {
          throw new StageException(Errors.KAFKA_65, result, record.getHeader().getSourceId());
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
          TopicMetadata topicMetadata =
              KafkaUtil.getTopicMetadata(kafkaBrokers, result, messageSendMaxRetries, retryBackoffMs);
          if (topicMetadata == null) {
            //Could not get topic metadata from any of the supplied brokers
            StageException s = new StageException(Errors.KAFKA_03, result, metadataBrokerList);
            //cache bad topic name and the exception
            invalidTopicMap.put(result, s);
            throw s;
          }
          if (topicMetadata.errorCode() == ErrorMapping.UnknownTopicOrPartitionCode()) {
            //Topic does not exist
            StageException s = new StageException(Errors.KAFKA_04, result);
            invalidTopicMap.put(result, s);
            throw s;
          }
          topicPartitionMap.put(result, topicMetadata.partitionsMetadata().size());
        }
        if (topicPartitionMap.keySet().size() % TOPIC_WARN_SIZE == 0) {
          LOG.warn("Encountered {} different topics while running the pipeline", topicPartitionMap.keySet().size());
        }
      } catch (IOException e) {
        throw new StageException(Errors.KAFKA_52, result, kafkaBrokers, e.getMessage());
      } catch (ELEvalException e) {
        throw new StageException(Errors.KAFKA_63, topicExpression, record.getHeader().getSourceId(), e.getMessage());
      }
    }
    return result;
  }

  @Override
  public void destroy() {
    LOG.info("Wrote {} number of records to Kafka Broker", recordCounter);
    if(kafkaProducer != null) {
      kafkaProducer.destroy();
    }
  }

  /****************************************************/
  /******** Validation Specific to Kafka Target *******/
  /****************************************************/

  private void validatePartitionExpression(List<ConfigIssue> issues) {
    if (partitionStrategy == PartitionStrategy.EXPRESSION) {
      partitionEval = getContext().createELEval("partition");
      partitionVars = getContext().createELVars();
      //There is no scope to provide partitionVars for kafka target as of today, create empty partitionVars
      ELUtils.validateExpression(partitionEval, getContext().createELVars(), partition, getContext(),
        Groups.KAFKA.name(), "partition", Errors.KAFKA_57, Object.class, issues);
    }
  }

  private void validateTopicExpression(List<ConfigIssue> issues) {
    topicEval = getContext().createELEval("topicExpression");
    topicVars = getContext().createELVars();
    ELUtils.validateExpression(topicEval, getContext().createELVars(), topicExpression, getContext(),
      Groups.KAFKA.name(), "topicExpression", Errors.KAFKA_61, Object.class, issues);
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
      case AVRO:
        //no-op
        break;
      default:
        issues.add(context.createConfigIssue(groupName, configName, Errors.KAFKA_02, dataFormat));
        //XML is not supported for KafkaTarget
    }
  }

  private void validateTopicExistence(List<ConfigIssue> issues, String topic) {
    if(topic == null || topic.isEmpty()) {
      issues.add(getContext().createConfigIssue(Groups.KAFKA.name(), "topic", Errors.KAFKA_05));
    } else {
      TopicMetadata topicMetadata;
      try {
        topicMetadata = KafkaUtil.getTopicMetadata(kafkaBrokers, topic, 1, 0);
      } catch (IOException e) {
        //Could not connect to kafka with the given metadata broker list
        issues.add(getContext().createConfigIssue(Groups.KAFKA.name(), "metadataBrokerList", Errors.KAFKA_67,
          metadataBrokerList));
        return;
      }

      if(topicMetadata == null) {
        //Could not get topic metadata from any of the supplied brokers
        issues.add(getContext().createConfigIssue(Groups.KAFKA.name(), "topic", Errors.KAFKA_03, topic,
          metadataBrokerList));
      } else if (topicMetadata.errorCode()== ErrorMapping.UnknownTopicOrPartitionCode()) {
        //Topic does not exist
        issues.add(getContext().createConfigIssue(Groups.KAFKA.name(), "topic", Errors.KAFKA_04, topic));
      } else {
        int numberOfPartitions = topicMetadata.partitionsMetadata().size();
        allowedTopics.add(topic);
        topicPartitionMap.put(topic, numberOfPartitions);
      }
    }
  }

  private void validateTopicWhiteList(List<ConfigIssue> issues, List<KafkaBroker> kafkaBrokers) {
    //if runtimeTopicResolution then topic white list cannot be empty
    if(topicWhiteList == null || topicWhiteList.isEmpty()) {
      issues.add(getContext().createConfigIssue(Groups.KAFKA.name(), "topicWhiteList", Errors.KAFKA_64));
    } else if (topicWhiteList.equals("*")) {
      allowAllTopics = true;
    } else {
      //Must be comma separated list of topic names
      if(kafkaBrokers != null && !kafkaBrokers.isEmpty()) {
        String[] topics = topicWhiteList.split(",");
        for (String t : topics) {
          t = t.trim();
          //validate sup0lied topic names in the white list
          validateTopicExistence(issues, t);
        }
      }
    }
  }

  private void validateKafkaProducerConfigs(List<ConfigIssue> issues) {
    if(kafkaProducerConfigs != null) {
      if(kafkaProducerConfigs.containsKey(MESSAGE_SEND_MAX_RETRIES_KEY)) {
        try {
          messageSendMaxRetries = Integer.parseInt(kafkaProducerConfigs.get(MESSAGE_SEND_MAX_RETRIES_KEY).trim());
        } catch (NullPointerException | NumberFormatException e) {
          issues.add(getContext().createConfigIssue(Groups.KAFKA.name(), "kafkaProducerConfigs", Errors.KAFKA_66,
            MESSAGE_SEND_MAX_RETRIES_KEY, "integer", e.getMessage(), e));
        }
        if(messageSendMaxRetries < 0) {
          issues.add(getContext().createConfigIssue(Groups.KAFKA.name(), "kafkaProducerConfigs", Errors.KAFKA_66,
            MESSAGE_SEND_MAX_RETRIES_KEY, "integer"));
        }
      } else {
        messageSendMaxRetries = MESSAGE_SEND_MAX_RETRIES_DEFAULT;
      }

      if(kafkaProducerConfigs.containsKey(RETRY_BACKOFF_MS_KEY)) {
        try {
          retryBackoffMs = Long.parseLong(kafkaProducerConfigs.get(RETRY_BACKOFF_MS_KEY).trim());
        } catch (NullPointerException | NumberFormatException e) {
          issues.add(getContext().createConfigIssue(Groups.KAFKA.name(), "kafkaProducerConfigs", Errors.KAFKA_66,
            RETRY_BACKOFF_MS_KEY, "long", e.getMessage(), e));
        }
        if(retryBackoffMs < 0) {
          issues.add(getContext().createConfigIssue(Groups.KAFKA.name(), "kafkaProducerConfigs", Errors.KAFKA_66,
            RETRY_BACKOFF_MS_KEY, "long"));
        }
      } else {
        retryBackoffMs = RETRY_BACKOFF_MS_DEFAULT;
      }
    }
  }

}
