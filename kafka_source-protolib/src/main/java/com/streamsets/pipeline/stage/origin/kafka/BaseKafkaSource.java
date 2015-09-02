/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.kafka;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.impl.XMLChar;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.CsvRecordType;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.config.LogMode;
import com.streamsets.pipeline.config.OnParseError;
import com.streamsets.pipeline.lib.kafka.KafkaErrors;
import com.streamsets.pipeline.lib.KafkaBroker;
import com.streamsets.pipeline.lib.KafkaUtil;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.avro.AvroDataParserFactory;
import com.streamsets.pipeline.lib.parser.delimited.DelimitedDataParserFactory;
import com.streamsets.pipeline.lib.parser.log.LogDataFormatValidator;
import com.streamsets.pipeline.lib.parser.log.RegExConfig;
import com.streamsets.pipeline.lib.parser.xml.XmlDataParserFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class BaseKafkaSource extends BaseSource implements OffsetCommitter {
  // required in children
  protected final String metadataBrokerList;
  protected final String zookeeperConnect;
  protected final String consumerGroup;
  protected final int maxBatchSize;
  protected final Map<String, String> kafkaConsumerConfigs;
  protected final String topic;
  protected final DataFormat dataFormat;
  protected final String charset;
  protected final boolean removeCtrlChars;
  protected final boolean produceSingleRecordPerMessage;
  // required only in self
  private final int textMaxLineLen;
  private final JsonMode jsonContent;
  private final int jsonMaxObjectLen;
  private final CsvMode csvFileFormat;
  private final CsvHeader csvHeader;
  private final int csvMaxObjectLen;
  private final String xmlRecordElement;
  private final int xmlMaxObjectLen;
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
  protected KafkaConsumer kafkaConsumer;
  private final boolean messageHasSchema;
  private final String avroSchema;
  private final int binaryMaxObjectLen;
  private final char csvCustomDelimiter;
  private final char csvCustomEscape;
  private final char csvCustomQuote;
  private final CsvRecordType csvRecordType;

  protected int maxWaitTime;
  private LogDataFormatValidator logDataFormatValidator;
  private Charset messageCharset;
  private DataParserFactory parserFactory;
  private int originParallelism = 0;

  public BaseKafkaSource(SourceArguments args) {
    this.metadataBrokerList = args.getMetadataBrokerList();
    this.zookeeperConnect = args.getZookeeperConnect();
    this.consumerGroup = args.getConsumerGroup();
    this.maxBatchSize = args.getMaxBatchSize();
    this.kafkaConsumerConfigs = args.getKafkaConsumerConfigs();
    this.topic = args.getTopic();
    this.dataFormat = args.getDataFormat();
    this.charset = args.getCharset();
    this.removeCtrlChars = args.getRemoveCtrlChars();
    this.produceSingleRecordPerMessage = args.isProduceSingleRecordPerMessage();
    this.maxWaitTime = args.getMaxWaitTime();
    this.textMaxLineLen = args.getTextMaxLineLen();
    this.jsonContent = args.getJsonContent();
    this.jsonMaxObjectLen = args.getJsonMaxObjectLen();
    this.csvFileFormat = args.getCsvFileFormat();
    this.csvHeader = args.getCsvHeader();
    this.csvMaxObjectLen = args.getCsvMaxObjectLen();
    this.xmlRecordElement = args.getXmlRecordElement();
    this.xmlMaxObjectLen = args.getXmlMaxObjectLen();
    this.logMode = args.getLogMode();
    this.logMaxObjectLen = args.getLogMaxObjectLen();
    this.logRetainOriginalLine = args.isRetainOriginalLine();
    this.customLogFormat = args.getCustomLogFormat();
    this.regex = args.getRegex();
    this.fieldPathsToGroupName = args.getFieldPathsToGroupName();
    this.grokPatternDefinition = args.getGrokPatternDefinition();
    this.grokPattern = args.getGrokPattern();
    this.enableLog4jCustomLogFormat = args.isEnableLog4jCustomLogFormat();
    this.log4jCustomLogFormat = args.getLog4jCustomLogFormat();
    this.maxStackTraceLines = args.getMaxStackTraceLines();
    this.onParseError = args.getOnParseError();
    this.messageHasSchema = args.isSchemaInMessage();
    this.avroSchema = args.getAvroSchema();
    this.binaryMaxObjectLen = args.getBinaryMaxObjectLen();
    this.csvCustomDelimiter = args.getCsvCustomDelimiter();
    this.csvCustomEscape = args.getCsvCustomEscape();
    this.csvCustomQuote = args.getCsvCustomQuote();
    this.csvRecordType = args.getCsvRecordType();
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = new ArrayList<ConfigIssue>();
    if(topic == null || topic.isEmpty()) {
      issues.add(getContext().createConfigIssue(Groups.KAFKA.name(), "topic",
        KafkaErrors.KAFKA_05));
    }
    //maxWaitTime
    if(maxWaitTime < 1) {
      issues.add(getContext().createConfigIssue(Groups.KAFKA.name(), "maxWaitTime",
        KafkaErrors.KAFKA_35));
    }

    switch (dataFormat) {
      case JSON:
        if (jsonMaxObjectLen < 1) {
          issues.add(getContext().createConfigIssue(Groups.JSON.name(), "maxJsonObjectLen", KafkaErrors.KAFKA_38));
        }
        break;
      case TEXT:
        if (textMaxLineLen < 1) {
          issues.add(getContext().createConfigIssue(Groups.TEXT.name(), "maxLogLineLength", KafkaErrors.KAFKA_38));
        }
        break;
      case DELIMITED:
        if (csvMaxObjectLen < 1) {
          issues.add(getContext().createConfigIssue(Groups.DELIMITED.name(), "csvMaxObjectLen", KafkaErrors.KAFKA_38));
        }
        break;
      case XML:
        if (produceSingleRecordPerMessage) {
          issues.add(getContext().createConfigIssue(Groups.KAFKA.name(), "produceSingleRecordPerMessage",
                                                    KafkaErrors.KAFKA_40));
        }
        if (xmlMaxObjectLen < 1) {
          issues.add(getContext().createConfigIssue(Groups.XML.name(), "maxXmlObjectLen", KafkaErrors.KAFKA_38));
        }
        if (xmlRecordElement != null && !xmlRecordElement.isEmpty() && !XMLChar.isValidName(xmlRecordElement)) {
          issues.add(getContext().createConfigIssue(Groups.XML.name(), "xmlRecordElement", KafkaErrors.KAFKA_36,
                                                    xmlRecordElement));
        }
        break;
      case SDC_JSON:
      case BINARY:
        break;
      case LOG:
        logDataFormatValidator = new LogDataFormatValidator(logMode, logMaxObjectLen,
          logRetainOriginalLine, customLogFormat, regex, grokPatternDefinition, grokPattern,
          enableLog4jCustomLogFormat, log4jCustomLogFormat, onParseError, maxStackTraceLines,
          Groups.LOG.name(), getFieldPathToGroupMap(fieldPathsToGroupName));
        logDataFormatValidator.validateLogFormatConfig(issues, getContext());
        break;
      case AVRO:
        if(!messageHasSchema && (avroSchema == null || avroSchema.isEmpty())) {
          issues.add(getContext().createConfigIssue(Groups.AVRO.name(), "avroSchema", KafkaErrors.KAFKA_43,
            avroSchema));
        }
        break;
      default:
        issues.add(getContext().createConfigIssue(Groups.KAFKA.name(), "dataFormat", KafkaErrors.KAFKA_39, dataFormat));
    }

    validateParserFactoryConfigs(issues);

    // Validate broker config
    try {
      int partitionCount = KafkaUtil.getPartitionCount(metadataBrokerList, topic, 3, 1000);
      if(partitionCount < 1) {
        issues.add(getContext().createConfigIssue(Groups.KAFKA.name(), "topic",
          KafkaErrors.KAFKA_42, topic));
      } else {
        //cache the partition count as parallelism for future use
        originParallelism = partitionCount;
      }
    } catch (IOException e) {
      issues.add(getContext().createConfigIssue(Groups.KAFKA.name(), "topic",
        KafkaErrors.KAFKA_41, topic, e.toString(), e));
    }

    // Validate zookeeper config
    List<KafkaBroker> kafkaBrokers = KafkaUtil.validateZkConnectionString(issues, zookeeperConnect,
      Groups.KAFKA.name(), "zookeeperConnect", getContext());

     //validate connecting to kafka
     if(kafkaBrokers != null && !kafkaBrokers.isEmpty() && topic !=null && !topic.isEmpty()) {
       kafkaConsumer = new KafkaConsumer(zookeeperConnect, topic, consumerGroup, maxBatchSize, maxWaitTime,
         kafkaConsumerConfigs, getContext());
       kafkaConsumer.validate(issues, getContext());
     }

     //consumerGroup
     if(consumerGroup == null || consumerGroup.isEmpty()) {
       issues.add(getContext().createConfigIssue(Groups.KAFKA.name(), "consumerGroup",
         KafkaErrors.KAFKA_33));
     }
     return issues;
  }

  @Override
  public int getParallelism() throws IOException {
    if(originParallelism == 0) {
      //origin parallelism is not yet calculated
      originParallelism = KafkaUtil.getPartitionCount(metadataBrokerList, topic, 3, 1000);
    }
    return originParallelism;
  }


  private void validateParserFactoryConfigs(List<ConfigIssue> issues) {
    DataParserFactoryBuilder builder = new DataParserFactoryBuilder(getContext(), dataFormat.getParserFormat())
      .setCharset(Charset.defaultCharset());
    if (charset == null) {
      messageCharset = StandardCharsets.UTF_8;
    } else {
      try {
        messageCharset = Charset.forName(charset);
      } catch (UnsupportedCharsetException ex) {
        // setting it to a valid one so the parser factory can be configured and tested for more errors
        messageCharset = StandardCharsets.UTF_8;
        issues.add(getContext().createConfigIssue(Groups.KAFKA.name(), "charset", KafkaErrors.KAFKA_08, charset));
      }
    }
    builder.setCharset(messageCharset).setRemoveCtrlChars(removeCtrlChars);

    switch ((dataFormat)) {
      case TEXT:
        builder.setMaxDataLen(textMaxLineLen);
        break;
      case JSON:
        builder.setMode(jsonContent);
        builder.setMaxDataLen(jsonMaxObjectLen);
        break;
      case DELIMITED:
        builder.setMaxDataLen(csvMaxObjectLen)
          .setMode(csvFileFormat).setMode(csvHeader).setMode(csvRecordType)
          .setConfig(DelimitedDataParserFactory.DELIMITER_CONFIG, csvCustomDelimiter)
          .setConfig(DelimitedDataParserFactory.ESCAPE_CONFIG, csvCustomEscape)
          .setConfig(DelimitedDataParserFactory.QUOTE_CONFIG, csvCustomQuote);
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
      case AVRO:
        builder.setMaxDataLen(Integer.MAX_VALUE).setConfig(AvroDataParserFactory.SCHEMA_KEY, avroSchema)
        .setConfig(AvroDataParserFactory.SCHEMA_IN_MESSAGE_KEY, messageHasSchema);
        break;
      case BINARY:
        builder.setMaxDataLen(binaryMaxObjectLen);
    }
    parserFactory = builder.build();
  }

  protected List<Record> processKafkaMessage(String messageId, byte[] payload) throws StageException {
    List<Record> records = new ArrayList<>();
    try (DataParser parser = parserFactory.getParser(messageId, payload)) {
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
        getContext().reportError(KafkaErrors.KAFKA_37, messageId, ex.toString(), ex);
        break;
      case STOP_PIPELINE:
        if (ex instanceof StageException) {
          throw (StageException) ex;
        } else {
          throw new StageException(KafkaErrors.KAFKA_37, messageId, ex.toString(), ex);
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

}
