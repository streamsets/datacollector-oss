/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.kafka;

import com.streamsets.pipeline.BootstrapSpark;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ErrorListener;
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
import com.streamsets.pipeline.config.LogMode;
import com.streamsets.pipeline.config.OnParseError;
import com.streamsets.pipeline.lib.Errors;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.log.LogDataFormatValidator;
import com.streamsets.pipeline.lib.parser.log.RegExConfig;
import com.streamsets.pipeline.lib.parser.xml.XmlDataParserFactory;

import org.apache.xerces.util.XMLChar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BaseKafkaSource extends BaseSource implements OffsetCommitter, ErrorListener {
  private static final Logger LOG = LoggerFactory.getLogger(BaseKafkaSource.class);
  private static final String CLUSTER_MODE_CLASS = "com.streamsets.pipeline.stage.origin.spark.SparkStreamingKafkaSource";
  protected final String topic;
  private final DataFormat dataFormat;
  private final String charset;
  private boolean produceSingleRecordPerMessage;

  private int textMaxLineLen;
  private final JsonMode jsonContent;
  private int jsonMaxObjectLen;
  private final CsvMode csvFileFormat;
  private final CsvHeader csvHeader;
  private int csvMaxObjectLen;
  private final String xmlRecordElement;
  private int xmlMaxObjectLen;
  protected int maxWaitTime;

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
  private final String zookeeperConnect;
  private final String consumerGroup;
  private final int maxBatchSize;
  private final Map<String, String> kafkaConsumerConfigs;
  private StandaloneKafkaSource standaloneKafkaSource;
  private LogDataFormatValidator logDataFormatValidator;
  protected final String metadataBrokerList;
  private Class clusterModeClazz;
  private BaseKafkaSource clusterModeInstance;

  public BaseKafkaSource(String metadataBrokerList, String zookeeperConnect, String consumerGroup, String topic, DataFormat dataFormat, String charset,
      boolean produceSingleRecordPerMessage, int maxBatchSize, int maxWaitTime, Map<String, String> kafkaConsumerConfigs,
      int textMaxLineLen, JsonMode jsonContent, int jsonMaxObjectLen, CsvMode csvFileFormat, CsvHeader csvHeader,
      int csvMaxObjectLen, String xmlRecordElement, int xmlMaxObjectLen, LogMode logMode, int logMaxObjectLen,
      boolean retainOriginalLine, String customLogFormat, String regex, List<RegExConfig> fieldPathsToGroupName,
      String grokPatternDefinition, String grokPattern, boolean enableLog4jCustomLogFormat,
      String log4jCustomLogFormat, OnParseError onParseError, int maxStackTraceLines) {
    this.metadataBrokerList = metadataBrokerList;
    this.zookeeperConnect = zookeeperConnect;
    this.consumerGroup = consumerGroup;
    this.maxBatchSize = maxBatchSize;
    this.kafkaConsumerConfigs = kafkaConsumerConfigs;
    this.topic = topic;
    this.dataFormat = dataFormat;
    this.charset = charset;
    this.produceSingleRecordPerMessage = produceSingleRecordPerMessage;
    this.maxWaitTime = maxWaitTime;
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
    List<ConfigIssue> issues = new ArrayList<>();
    if (getContext().isPreview() || !getContext().isClusterMode()) {
      //preview can run only in standalone mode
      standaloneKafkaSource = new StandaloneKafkaSource(zookeeperConnect, consumerGroup, topic, dataFormat, charset,
          produceSingleRecordPerMessage, maxBatchSize, maxWaitTime, kafkaConsumerConfigs, textMaxLineLen, jsonContent,
          jsonMaxObjectLen, csvFileFormat, csvHeader, csvMaxObjectLen, xmlRecordElement, xmlMaxObjectLen, logMode,
          logMaxObjectLen, logRetainOriginalLine, customLogFormat, regex, fieldPathsToGroupName, grokPatternDefinition,
          grokPattern, enableLog4jCustomLogFormat, log4jCustomLogFormat, onParseError, maxStackTraceLines);
      issues.addAll(standaloneKafkaSource.validateConfigs(this.getInfo(), this.getContext()));
      //Should validate cluster mode configs even though preview is run in standalone mode
      if(getContext().isClusterMode()) {
        issues.addAll(validateClusterModeConfigs());
      }
    } else {
      issues.addAll(validateClusterModeConfigs());
    }
    return issues;
  }

  protected List<ConfigIssue> validateCommonConfigs(List<ConfigIssue> issues) throws StageException {
    if(topic == null || topic.isEmpty()) {
      issues.add(getContext().createConfigIssue(Groups.KAFKA.name(), "topic",
        Errors.KAFKA_05));
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
        issues.add(getContext().createConfigIssue(Groups.KAFKA.name(), "dataFormat", Errors.KAFKA_39, dataFormat));
    }

    validateParserFactoryConfigs(issues);

    //kafka consumer configs
    //We do not validate this, just pass it down to Kafka

    return issues;
  }

  private void validateParserFactoryConfigs(List<ConfigIssue> issues) {
    DataParserFactoryBuilder builder = new DataParserFactoryBuilder(getContext(), dataFormat.getParserFormat())
        .setCharset(Charset.defaultCharset());

    try {
      messageCharset = Charset.forName(charset);
    } catch (UnsupportedCharsetException ex) {
      // setting it to a valid one so the parser factory can be configured and tested for more errors
      messageCharset = Charset.forName("UTF-8");
      issues.add(getContext().createConfigIssue(Groups.KAFKA.name(), "charset", Errors.KAFKA_08, charset));
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
        parserFactory = builder.build();
        break;
    }
    parserFactory = builder.build();
  }

  Charset messageCharset;
  DataParserFactory parserFactory;

  @Override
  public void init() throws StageException {
    if (getContext().isPreview() || !getContext().isClusterMode()) {
      standaloneKafkaSource.init();
    } else {
      clusterModeInstance.init();
    }
  }


  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    if(getContext().isPreview() || !getContext().isClusterMode()) {
      return standaloneKafkaSource.produce(lastSourceOffset, maxBatchSize, batchMaker);
    } else {
      return clusterModeInstance.produce(lastSourceOffset, maxBatchSize, batchMaker);
    }
  }

  protected List<Record> processKafkaMessage(String messageId, byte[] payload) throws StageException {
    List<Record> records = new ArrayList<>();
    try (DataParser parser = parserFactory.getParser(messageId, new ByteArrayInputStream(payload), 0)) {
      Record record = parser.parse();
      while (record != null) {
        records.add(record);
        record = parser.parse();
      }
    } catch (IOException|DataParserException ex) {
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

  @Override
  public void commit(String offset) throws StageException {
    if(getContext().isPreview() || !getContext().isClusterMode()) {
      standaloneKafkaSource.commit(offset);
    } else {
      clusterModeInstance.commit(offset);
    }

  }

  @Override
  public void destroy() {
    if (getContext().isPreview() || !getContext().isClusterMode()) {
      standaloneKafkaSource.destroy();
    } else {
      clusterModeInstance.destroy();
    }
  }

  private Object createInstanceClusterMode(String className) {
    try {
      clusterModeClazz = Class.forName(className);
      Constructor ctor =
        clusterModeClazz.getConstructor(String.class, String.class, DataFormat.class, String.class, boolean.class,
          int.class, int.class, Map.class, int.class, JsonMode.class, int.class, CsvMode.class, CsvHeader.class,
          int.class, String.class, int.class, LogMode.class, int.class, boolean.class, String.class, String.class,
          List.class, String.class, String.class, boolean.class, String.class, OnParseError.class, int.class);
      Object[] arguments =
        { metadataBrokerList, topic, dataFormat, charset, produceSingleRecordPerMessage, maxBatchSize, maxWaitTime,
            kafkaConsumerConfigs, textMaxLineLen, jsonContent, jsonMaxObjectLen, csvFileFormat, csvHeader,
            csvMaxObjectLen, xmlRecordElement, xmlMaxObjectLen, logMode, logMaxObjectLen, logRetainOriginalLine,
            customLogFormat, regex, fieldPathsToGroupName, grokPatternDefinition, grokPattern,
            enableLog4jCustomLogFormat, log4jCustomLogFormat, onParseError, maxStackTraceLines };
      clusterModeInstance = (BaseKafkaSource) ctor.newInstance(arguments);
    } catch (Exception e) {
      throw new IllegalStateException("Exception while invoking kafka instance in cluster mode: " + e, e);
    }
    return clusterModeInstance;
  }

  public BaseKafkaSource getSource() {
    if (clusterModeInstance != null) {
      return clusterModeInstance;
    } else {
      return standaloneKafkaSource;
    }
  }

  @Override
  public void errorNotification(Throwable throwable) {
    BaseKafkaSource source = getSource();
    if (source == null) {
      String msg = "Source is null when throwing to handle error notification: " + throwable;
      LOG.error(msg, throwable);
    } else {
      source.errorNotification(throwable);
    }
  }

  private List<ConfigIssue> validateClusterModeConfigs() throws StageException {
    createInstanceClusterMode(CLUSTER_MODE_CLASS);
    return clusterModeInstance.validateConfigs(this.getInfo(), this.getContext());
  }

}
