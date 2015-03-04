/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.kafka;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.lib.Errors;
import com.streamsets.pipeline.lib.KafkaBroker;
import com.streamsets.pipeline.lib.KafkaUtil;

import com.streamsets.pipeline.lib.parser.CharDataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.xml.XmlCharDataParserFactory;
import org.apache.xerces.util.XMLChar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KafkaSource extends BaseSource implements OffsetCommitter {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);

  private final String zookeeperConnect;
  private final String consumerGroup;
  private final String topic;
  private final DataFormat dataFormat;
  private final int maxBatchSize;
  private final Map<String, String> kafkaConsumerConfigs;
  private final JsonMode jsonContent;
  private final boolean produceSingleRecord;
  private final CsvMode csvFileFormat;
  private final CsvHeader csvHeader;
  private final String xmlRecordElement;
  private int maxWaitTime;
  private KafkaConsumer kafkaConsumer;

  public KafkaSource(String zookeeperConnect, String consumerGroup, String topic,
                     DataFormat dataFormat, int maxBatchSize, int maxWaitTime,
                     Map<String, String> kafkaConsumerConfigs, JsonMode jsonContent,
                     boolean produceSingleRecord, CsvMode csvFileFormat, CsvHeader csvHeader,
      String xmlRecordElement) {
    this.zookeeperConnect = zookeeperConnect;
    this.consumerGroup = consumerGroup;
    this.topic = topic;
    this.dataFormat = dataFormat;
    this.maxBatchSize = maxBatchSize;
    this.maxWaitTime = maxWaitTime;
    this.kafkaConsumerConfigs = kafkaConsumerConfigs;
    this.jsonContent = jsonContent;
    this.produceSingleRecord = produceSingleRecord;
    this.csvFileFormat = csvFileFormat;
    this.csvHeader = csvHeader;
    this.xmlRecordElement = xmlRecordElement;
  }

  @Override
  protected List<ConfigIssue> validateConfigs() throws StageException {
    List<ConfigIssue> issues = super.validateConfigs();

    List<KafkaBroker> kafkaBrokers = KafkaUtil.validateBrokerList(issues, zookeeperConnect, Groups.KAFKA.name(),
      "zookeeperConnect", getContext());

    //topic should exist
    if(topic == null || topic.isEmpty()) {
      issues.add(getContext().createConfigIssue(Groups.KAFKA.name(), "topic",
        Errors.KAFKA_05));
    }

    //validate connecting to kafka
    if(kafkaBrokers != null && !kafkaBrokers.isEmpty() && topic !=null && !topic.isEmpty()) {
      kafkaConsumer = new KafkaConsumer(zookeeperConnect, topic, consumerGroup, maxBatchSize, maxWaitTime,
        kafkaConsumerConfigs, getContext());
      kafkaConsumer.validate(issues, getContext());
    }

    //consumerGroup
    if(consumerGroup == null || consumerGroup.isEmpty()) {
      issues.add(getContext().createConfigIssue(Groups.KAFKA.name(), "consumerGroup",
        Errors.KAFKA_33));
    }

    //maxBatchSize
    if(maxBatchSize < 1) {
      issues.add(getContext().createConfigIssue(Groups.KAFKA.name(), "maxBatchSize",
        Errors.KAFKA_34));
    }

    //maxWaitTime
    if(maxWaitTime < 1) {
      issues.add(getContext().createConfigIssue(Groups.KAFKA.name(), "maxWaitTime",
        Errors.KAFKA_35));
    }

    //payload type and payload specific configuration
    validateDataFormatAndSpecificConfig(issues);

    //kafka consumer configs
    //We do not validate this, just pass it down to Kafka

    return issues;
  }

  CharDataParserFactory parserFactory;

  @Override
  public void init() throws StageException {
    super.init();
    if(getContext().isPreview()) {
      //set fixed batch duration time of 1 second for preview.
      maxWaitTime = 1000;
    }
    kafkaConsumer.init();
    LOG.info("Successfully initialized Kafka Consumer");

    CharDataParserFactory.Builder builder =
        new CharDataParserFactory.Builder(getContext(), dataFormat.getFormat()).setMaxDataLen(-1);

    switch ((dataFormat)) {
      case TEXT:
        break;
      case JSON:
        builder.setMode(jsonContent);
        break;
      case DELIMITED:
        builder.setMode(csvFileFormat).setMode(csvHeader);
        break;
      case XML:
        builder.setConfig(XmlCharDataParserFactory.RECORD_ELEMENT_KEY, xmlRecordElement);
        break;
      case SDC_JSON:
        break;
    }
    parserFactory = builder.build();
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    int recordCounter = 0;
    int batchSize = this.maxBatchSize > maxBatchSize ? maxBatchSize : this.maxBatchSize;
    long startTime = System.currentTimeMillis();
    while(recordCounter < batchSize && (startTime + maxWaitTime) > System.currentTimeMillis()) {
      MessageAndOffset message = kafkaConsumer.read();
      if(message != null) {
        List<Record> records = processKafkaMessage(message);
        for(Record record : records) {
          batchMaker.addRecord(record);
        }
        recordCounter += records.size();
      }
    }
    return lastSourceOffset;
  }

  private final static Charset UTF8 = Charset.forName("UTF-8");

  @VisibleForTesting
  List<Record> processKafkaMessage(MessageAndOffset message) throws StageException {
    List<Record> records = new ArrayList<>();
    String messageStr = new String(message.getPayload(), UTF8);
    String messageId = getMessageID(message);
    try (DataParser parser = parserFactory.getParser(messageId, messageStr)) {
      Record record = parser.parse();
      while (record != null) {
        records.add(record);
        record = parser.parse();
      }
    } catch (IOException|DataParserException ex) {
      switch (getContext().getOnErrorRecord()) {
        case DISCARD:
          break;
        case TO_ERROR:
          getContext().reportError(Errors.KAFKA_37, messageId, ex.getMessage(), ex);
          break;
        case STOP_PIPELINE:
          if (ex instanceof StageException) {
            throw (StageException) ex;
          } else {
            throw new StageException(Errors.KAFKA_37, messageId, ex.getMessage(), ex);
          }
        default:
          throw new IllegalStateException(Utils.format("It should never happen. OnError '{}'",
                                                       getContext().getOnErrorRecord(), ex));
      }
    }
    if (produceSingleRecord && records.size() > 1) {
      List<Field> list = new ArrayList<>();
      for (Record record : records) {
        list.add(record.get());
      }
      Record record = records.get(0);
      record.set(Field.create(list));
      records.clear();
      records.add(record);
    }
    return records;
  }

  private String getMessageID(MessageAndOffset message) {
    return topic + "::" + message.getPartition() + "::" + message.getOffset();
  }

  @Override
  public void destroy() {
    kafkaConsumer.destroy();
    super.destroy();
  }

  @Override
  public void commit(String offset) throws StageException {
    kafkaConsumer.commit();
  }

  /****************************************************/
  /******** Validation Specific to Kafka Source *******/
  /****************************************************/

  private void validateDataFormatAndSpecificConfig(List<Stage.ConfigIssue> issues) {
    switch (dataFormat) {
      case XML:
        if (xmlRecordElement != null && !xmlRecordElement.isEmpty() && !XMLChar.isValidName(xmlRecordElement)) {
          issues.add(getContext().createConfigIssue(Groups.XML.name(), "xmlRecordElement", Errors.KAFKA_36,
                                                    xmlRecordElement));
        }
        break;
    }
  }
}
