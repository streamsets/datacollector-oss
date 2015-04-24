/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin;

import com.google.common.base.Throwables;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;

import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.config.LogMode;
import com.streamsets.pipeline.config.OnParseError;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.log.LogDataFormatValidator;
import com.streamsets.pipeline.lib.parser.log.RegExConfig;
import com.streamsets.pipeline.lib.parser.xml.XmlDataParserFactory;

import org.apache.xerces.util.XMLChar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;


import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Ingests kafka produce data from spark streaming
 *
 */
public class SparkStreamingKafkaSource extends SparkStreamingSource implements OffsetCommitter {
  private static final Logger LOG = LoggerFactory.getLogger(SparkStreamingKafkaSource.class);
  private int recordsProduced = 0;

  private final String topic;
  private final DataFormat dataFormat;
  private final String charset;
  public boolean produceSingleRecordPerMessage;
  public int textMaxLineLen;
  private final JsonMode jsonContent;
  public int jsonMaxObjectLen;
  private final CsvMode csvFileFormat;
  private final CsvHeader csvHeader;
  public int csvMaxObjectLen;
  private final String xmlRecordElement;
  public int xmlMaxObjectLen;
  private int waitTime;
  private final LogMode logMode;
  private final int logMaxObjectLen;
  private final boolean logRetainOriginalLine;
  private final String customLogFormat;
  private final String regex;
  private final String grokPatternDefinition;
  private final String grokPattern;
  private final List<RegExConfig> fieldPathsToGroupName;
  private final boolean enableLog4jCustomLogFormat;
  private final String log4jCustomLogFormat;
  private final int maxStackTraceLines;
  private final OnParseError onParseError;
  private Charset messageCharset;
  private DataParserFactory parserFactory;
  private LogDataFormatValidator logDataFormatValidator;

  public SparkStreamingKafkaSource(String topic, DataFormat dataFormat,
    String charset, boolean produceSingleRecordPerMessage, int waitTime,
    int textMaxLineLen, JsonMode jsonContent, int jsonMaxObjectLen, CsvMode csvFileFormat, CsvHeader csvHeader,
    int csvMaxObjectLen, String xmlRecordElement, int xmlMaxObjectLen, LogMode logMode, int logMaxObjectLen,
    boolean retainOriginalLine, String customLogFormat, String regex, List<RegExConfig> fieldPathsToGroupName,
    String grokPatternDefinition, String grokPattern, boolean enableLog4jCustomLogFormat, String log4jCustomLogFormat,
    OnParseError onParseError, int maxStackTraceLines) {
    this.topic = topic;
    this.dataFormat = dataFormat;
    this.charset = charset;
    this.produceSingleRecordPerMessage = produceSingleRecordPerMessage;
    this.waitTime = waitTime;
    this.textMaxLineLen = textMaxLineLen;
    this.jsonContent = jsonContent;
    this.jsonMaxObjectLen = jsonMaxObjectLen;
    this.csvFileFormat = csvFileFormat;
    this.csvHeader = csvHeader;
    this.csvMaxObjectLen = csvMaxObjectLen;
    this.xmlRecordElement = xmlRecordElement;
    this.xmlMaxObjectLen = xmlMaxObjectLen;
    this.logMode = logMode;
    this.logMaxObjectLen = logMaxObjectLen;
    this.logRetainOriginalLine = retainOriginalLine;
    this.customLogFormat = customLogFormat;
    this.regex = regex;
    this.fieldPathsToGroupName = fieldPathsToGroupName;
    this.grokPatternDefinition = grokPatternDefinition;
    this.grokPattern = grokPattern;
    this.enableLog4jCustomLogFormat = enableLog4jCustomLogFormat;
    this.log4jCustomLogFormat = log4jCustomLogFormat;
    this.maxStackTraceLines = maxStackTraceLines;
    this.onParseError = onParseError;
  }

  @Override
  protected List<ConfigIssue> validateConfigs() throws StageException {
    List<ConfigIssue> issues = super.validateConfigs();

    //maxWaitTime
    if(waitTime < 1) {
      issues.add(getContext().createConfigIssue(Groups.SPARKSTREAMING_KAFKA.name(), "maxProcessingWaitTime",
        Errors.SPARK_02));
    }

    switch (dataFormat) {
      case JSON:
        if (jsonMaxObjectLen < 1) {
          issues.add(getContext().createConfigIssue(Groups.JSON.name(), "maxJsonObjectLen", Errors.SPARK_04));
        }
        break;
      case TEXT:
        if (textMaxLineLen < 1) {
          issues.add(getContext().createConfigIssue(Groups.TEXT.name(), "maxLogLineLength", Errors.SPARK_04));
        }
        break;
      case DELIMITED:
        if (csvMaxObjectLen < 1) {
          issues.add(getContext().createConfigIssue(Groups.DELIMITED.name(), "csvMaxObjectLen", Errors.SPARK_04));
        }
        break;
      case XML:
        if (produceSingleRecordPerMessage) {
          issues.add(getContext().createConfigIssue(Groups.SPARKSTREAMING_KAFKA.name(), "produceSingleRecordPerMessage",
                                                    Errors.SPARK_06));
        }
        if (xmlMaxObjectLen < 1) {
          issues.add(getContext().createConfigIssue(Groups.XML.name(), "s", Errors.SPARK_04));
        }
        if (xmlRecordElement != null && !xmlRecordElement.isEmpty() && !XMLChar.isValidName(xmlRecordElement)) {
          issues.add(getContext().createConfigIssue(Groups.XML.name(), "xmlRecordElement", Errors.SPARK_03,
                                                    xmlRecordElement));
        }
        break;
      case SDC_JSON:
        break;
      case LOG:
        logDataFormatValidator = new LogDataFormatValidator(logMode, logMaxObjectLen,
          logRetainOriginalLine, customLogFormat, regex, grokPatternDefinition, grokPattern,
          enableLog4jCustomLogFormat, log4jCustomLogFormat, onParseError, maxStackTraceLines,
          Groups.LOG.name(), getFieldPathToGroupMap(fieldPathsToGroupName));
        logDataFormatValidator.validateLogFormatConfig(issues, getContext());
        break;
      default:
        issues.add(getContext().createConfigIssue(Groups.SPARKSTREAMING_KAFKA.name(), "dataFormat", Errors.SPARK_05, dataFormat));
    }

    validateParserFactoryConfigs(issues);

    //kafka consumer configs
    //We do not validate this, just pass it down to Kafka

    return issues;
  }


  private void validateParserFactoryConfigs(List<ConfigIssue> issues) {
    DataParserFactoryBuilder builder =
      new DataParserFactoryBuilder(getContext(), dataFormat.getParserFormat()).setCharset(Charset.defaultCharset());
    try {
      messageCharset = Charset.forName(charset);
    } catch (UnsupportedCharsetException ex) {
      // setting it to a valid one so the parser factory can be configured and tested for more errors
      messageCharset = Charset.forName("UTF-8");
      issues.add(getContext().createConfigIssue(Groups.SPARKSTREAMING_KAFKA.name(), "charset", Errors.SPARK_07, charset));
    }
    builder.setCharset(messageCharset);

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
        builder.setConfig(XmlDataParserFactory.RECORD_ELEMENT_KEY, xmlRecordElement);
        break;
      case SDC_JSON:
        builder.setMaxDataLen(-1);
        break;
      case LOG:
        logDataFormatValidator.populateBuilder(builder);
        break;
    }
    parserFactory = builder.build();
  }

  private String getRecordId(String topic) {
    return "spark-streaming" + "::" + topic;
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    // Ignore the batch size
    Throwable error = null;
    LOG.info("Source is " + this);
    try {
      Object object;
      if ((object = getElement(waitTime)) != null) {
        List<MessageAndPartition> batch = null;
        if (object instanceof List) {
          batch = (List)object;
        } else {
          throw new IllegalStateException("Producer expects List, got " + object.getClass().getSimpleName());
        }
        for (MessageAndPartition messageAndPartition : batch) {
          List<Record> records = processKafkaMessage(messageAndPartition);
          for (Record record : records) {
            batchMaker.addRecord(record);
          }
          recordsProduced += records.size();
        }
      } else {
        LOG.debug("Didn't get any data, must be empty RDD");
      }
    } catch (Throwable throwable) {
      error = throwable;
    } finally {
      if (error != null) {
        try {
          putElement(error);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        if (error instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
      }
    }
    if (error != null) {
      Throwables.propagate(error);
    }
    return lastSourceOffset;
  }

  private List<Record> processKafkaMessage(MessageAndPartition messageAndPartition) throws StageException {
    List<Record> records = new ArrayList<>();
    String messageId = getRecordId(topic);
    try (DataParser parser =
      parserFactory.getParser(messageId, new ByteArrayInputStream(messageAndPartition.getPayload()), 0)) {
      Record record = parser.parse();
      while (record != null) {
        records.add(record);
        record = parser.parse();
      }
    } catch (IOException | DataParserException ex) {
      handleException(messageId, ex);
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

  private void handleException(String messageId, Exception ex) throws StageException {
    switch (getContext().getOnErrorRecord()) {
      case DISCARD:
        break;
      case TO_ERROR:
        getContext().reportError(Errors.SPARK_01, messageId, ex.getMessage(), ex);
        break;
      case STOP_PIPELINE:
        if (ex instanceof StageException) {
          throw (StageException) ex;
        } else {
          throw new StageException(Errors.SPARK_01, messageId, ex.getMessage(), ex);
        }
      default:
        throw new IllegalStateException(Utils.format("It should never happen. OnError '{}'", getContext()
          .getOnErrorRecord(), ex));
    }
  }

  @Override
  public long getRecordsProduced() {
    return recordsProduced;
  }

  private Map<String, Integer> getFieldPathToGroupMap(List<RegExConfig> fieldPathsToGroupName) {
    if(fieldPathsToGroupName == null) {
      return new HashMap<>();
    }
    Map<String, Integer> fieldPathToGroup = new HashMap<>();
    for(RegExConfig r : fieldPathsToGroupName) {
      fieldPathToGroup.put(r.fieldPath, r.group);
    }
    return fieldPathToGroup;
  }

}
