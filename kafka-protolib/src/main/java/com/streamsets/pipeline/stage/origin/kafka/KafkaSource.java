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
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KafkaSource extends BaseSource implements OffsetCommitter {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);

  private final String zookeeperConnect;
  private final String consumerGroup;
  private final String topic;
  private final DataFormat dataFormat;
  private final String charset;
  public boolean produceSingleRecordPerMessage;
  private final int maxBatchSize;
  private final Map<String, String> kafkaConsumerConfigs;
  public int textMaxLineLen;
  private final JsonMode jsonContent;
  public int jsonMaxObjectLen;
  private final CsvMode csvFileFormat;
  private final CsvHeader csvHeader;
  public int csvMaxObjectLen;
  private final String xmlRecordElement;
  public int xmlMaxObjectLen;
  private int maxWaitTime;
  private KafkaConsumer kafkaConsumer;

  public KafkaSource(String zookeeperConnect, String consumerGroup, String topic, DataFormat dataFormat, String charset,
      boolean produceSingleRecordPerMessage, int maxBatchSize, int maxWaitTime, Map<String, String> kafkaConsumerConfigs,
      int textMaxLineLen, JsonMode jsonContent, int jsonMaxObjectLen, CsvMode csvFileFormat, CsvHeader csvHeader,
      int csvMaxObjectLen, String xmlRecordElement, int xmlMaxObjectLen) {
    this.zookeeperConnect = zookeeperConnect;
    this.consumerGroup = consumerGroup;
    this.topic = topic;
    this.dataFormat = dataFormat;
    this.charset = charset;
    this.produceSingleRecordPerMessage = produceSingleRecordPerMessage;
    this.maxBatchSize = maxBatchSize;
    this.maxWaitTime = maxWaitTime;
    this.kafkaConsumerConfigs = kafkaConsumerConfigs;
    this.textMaxLineLen = textMaxLineLen;
    this.jsonContent = jsonContent;
    this.jsonMaxObjectLen = jsonMaxObjectLen;
    this.csvFileFormat = csvFileFormat;
    this.csvHeader = csvHeader;
    this.csvMaxObjectLen = csvMaxObjectLen;
    this.xmlRecordElement = xmlRecordElement;
    this.xmlMaxObjectLen = xmlMaxObjectLen;
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

    switch (dataFormat) {
      case JSON:
        if (jsonMaxObjectLen < 1) {
          issues.add(getContext().createConfigIssue(Groups.JSON.name(), "maxJsonObjectLen", Errors.KAFKA_38));
        }
        break;
      case TEXT:
        if (textMaxLineLen < 1) {
          issues.add(getContext().createConfigIssue(Groups.TEXT.name(), "maxLogLineLength", Errors.KAFKA_38));
        }
        break;
      case DELIMITED:
        if (csvMaxObjectLen < 1) {
          issues.add(getContext().createConfigIssue(Groups.DELIMITED.name(), "csvMaxObjectLen", Errors.KAFKA_38));
        }
        break;
      case XML:
        if (produceSingleRecordPerMessage) {
          issues.add(getContext().createConfigIssue(Groups.KAFKA.name(), "produceSingleRecordPerMessage",
                                                    Errors.KAFKA_40));
        }
        if (xmlMaxObjectLen < 1) {
          issues.add(getContext().createConfigIssue(Groups.XML.name(), "maxXmlObjectLen", Errors.KAFKA_38));
        }
        if (xmlRecordElement != null && !xmlRecordElement.isEmpty() && !XMLChar.isValidName(xmlRecordElement)) {
          issues.add(getContext().createConfigIssue(Groups.XML.name(), "xmlRecordElement", Errors.KAFKA_36,
                                                    xmlRecordElement));
        }
        break;
      default:
        issues.add(getContext().createConfigIssue(Groups.KAFKA.name(), "dataFormat", Errors.KAFKA_39, dataFormat));
    }

    validateParserFactoryConfigs(issues);

    //kafka consumer configs
    //We do not validate this, just pass it down to Kafka

    return issues;
  }

  private void validateParserFactoryConfigs(List<ConfigIssue> issues) {
    CharDataParserFactory.Builder builder =
        new CharDataParserFactory.Builder(getContext(), dataFormat.getParserFormat());

    try {
      messageCharset = Charset.forName(charset);
    } catch (UnsupportedCharsetException ex) {
      // setting it to a valid one so the parser factory can be configured and tested for more errors
      messageCharset = Charset.forName("UTF-8");
      issues.add(getContext().createConfigIssue(Groups.KAFKA.name(), "charset", Errors.KAFKA_08, charset));
    }

    switch ((dataFormat)) {
      case TEXT:
        builder.setMaxDataLen(textMaxLineLen);
        break;
      case JSON:
        builder.setMode(jsonContent);
        builder.setMaxDataLen(jsonMaxObjectLen);
        break;
      case DELIMITED:
        builder.setMaxDataLen(csvMaxObjectLen);
        builder.setMode(csvFileFormat).setMode(csvHeader);
        break;
      case XML:
        builder.setMaxDataLen(xmlMaxObjectLen);
        builder.setConfig(XmlCharDataParserFactory.RECORD_ELEMENT_KEY, xmlRecordElement);
        break;
      case SDC_JSON:
        break;
    }
    parserFactory = builder.build();

  }

  Charset messageCharset;
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
          System.out.println("YYY");
          batchMaker.addRecord(record);
        }
        recordCounter += records.size();
      }
    }
    return lastSourceOffset;
  }

  @VisibleForTesting
  List<Record> processKafkaMessage(MessageAndOffset message) throws StageException {
    List<Record> records = new ArrayList<>();
    String messageStr = new String(message.getPayload(), messageCharset);
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
    if (produceSingleRecordPerMessage) {
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

}
