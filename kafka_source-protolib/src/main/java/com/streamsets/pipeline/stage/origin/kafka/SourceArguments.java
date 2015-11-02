/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.CsvRecordType;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.config.LogMode;
import com.streamsets.pipeline.config.OnParseError;
import com.streamsets.pipeline.lib.parser.log.RegExConfig;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SourceArguments {
  protected final String metadataBrokerList;
  protected final String zookeeperConnect;
  protected final String consumerGroup;
  protected final String topic;
  protected final DataFormat dataFormat;
  protected final String charset;
  protected final boolean removeCtrlChars;
  protected final boolean produceSingleRecordPerMessage;

  protected final int textMaxLineLen;
  protected final JsonMode jsonContent;
  protected final int jsonMaxObjectLen;
  protected final CsvMode csvFileFormat;
  protected final CsvHeader csvHeader;
  protected final int csvMaxObjectLen;
  protected final char csvCustomDelimiter;
  protected final char csvCustomEscape;
  protected final char csvCustomQuote;
  protected final CsvRecordType csvRecordType;

  protected final String xmlRecordElement;
  protected final int xmlMaxObjectLen;
  protected final int maxWaitTime;

  protected final LogMode logMode;
  protected final int logMaxObjectLen;
  protected final boolean retainOriginalLine;
  protected final String customLogFormat;
  protected final String regex;
  protected final String grokPatternDefinition;

  protected final String grokPattern;
  protected final List<RegExConfig> fieldPathsToGroupName;
  protected final boolean enableLog4jCustomLogFormat;
  protected final String log4jCustomLogFormat;
  protected final int maxStackTraceLines;
  protected final OnParseError onParseError;
  protected final int maxBatchSize;
  protected final Map<String, String> kafkaConsumerConfigs;
  protected final boolean schemaInMessage;
  protected final String avroSchema;
  protected final int binaryMaxObjectLen;

  private final String protoDescriptorFile;
  private final String messageType;


  public SourceArguments(String metadataBrokerList, String zookeeperConnect, String consumerGroup, String topic,
                         DataFormat dataFormat, String charset, boolean removeCtrlChars,
                         boolean produceSingleRecordPerMessage, int maxBatchSize,
                         int maxWaitTime, int textMaxLineLen, JsonMode jsonContent, int jsonMaxObjectLen,
                         CsvMode csvFileFormat, CsvHeader csvHeader, int csvMaxObjectLen, String xmlRecordElement,
                         int xmlMaxObjectLen, LogMode logMode, int logMaxObjectLen, boolean retainOriginalLine,
                         String customLogFormat, String regex, String grokPatternDefinition, String grokPattern,
                         List<RegExConfig> fieldPathsToGroupName, boolean enableLog4jCustomLogFormat,
                         String log4jCustomLogFormat, int maxStackTraceLines, OnParseError onParseError,
                         Map<String, String> kafkaConsumerConfigs, boolean schemaInMessage, String avroSchema,
                         int binaryMaxObjectLen, char csvCustomDelimiter, char csvCustomEscape,
                         char csvCustomQuote, CsvRecordType csvRecordType, String protoDescriptorFile,
                         String messageType) {
    this.metadataBrokerList = metadataBrokerList;
    this.zookeeperConnect = zookeeperConnect;
    this.consumerGroup = consumerGroup;
    this.topic = topic;
    this.dataFormat = dataFormat;
    this.charset = charset;
    this.removeCtrlChars = removeCtrlChars;
    this.produceSingleRecordPerMessage = produceSingleRecordPerMessage;
    this.textMaxLineLen = textMaxLineLen;
    this.jsonContent = jsonContent;
    this.jsonMaxObjectLen = jsonMaxObjectLen;
    this.csvFileFormat = csvFileFormat;
    this.csvHeader = csvHeader;
    this.csvMaxObjectLen = csvMaxObjectLen;
    this.xmlRecordElement = xmlRecordElement;
    this.xmlMaxObjectLen = xmlMaxObjectLen;
    this.maxWaitTime = maxWaitTime;
    this.logMode = logMode;
    this.logMaxObjectLen = logMaxObjectLen;
    this.retainOriginalLine = retainOriginalLine;
    this.customLogFormat = customLogFormat;
    this.regex = regex;
    this.grokPatternDefinition = grokPatternDefinition;
    this.grokPattern = grokPattern;
    this.fieldPathsToGroupName = ImmutableList.copyOf(fieldPathsToGroupName == null ?
      Collections.<RegExConfig>emptyList() : fieldPathsToGroupName);
    this.enableLog4jCustomLogFormat = enableLog4jCustomLogFormat;
    this.log4jCustomLogFormat = log4jCustomLogFormat;
    this.maxStackTraceLines = maxStackTraceLines;
    this.onParseError = onParseError;
    this.maxBatchSize = maxBatchSize;
    this.kafkaConsumerConfigs = Maps.newHashMap(kafkaConsumerConfigs == null ?
      Collections.<String, String>emptyMap() : kafkaConsumerConfigs);
    this.schemaInMessage = schemaInMessage;
    this.avroSchema = avroSchema;
    this.binaryMaxObjectLen = binaryMaxObjectLen;
    this.csvCustomDelimiter = csvCustomDelimiter;
    this.csvCustomEscape = csvCustomEscape;
    this.csvCustomQuote = csvCustomQuote;
    this.csvRecordType = csvRecordType;
    this.protoDescriptorFile = protoDescriptorFile;
    this.messageType = messageType;
  }

  public String getMetadataBrokerList() {
    return metadataBrokerList;
  }

  public String getZookeeperConnect() {
    return zookeeperConnect;
  }

  public String getConsumerGroup() {
    return consumerGroup;
  }

  public String getTopic() {
    return topic;
  }

  public DataFormat getDataFormat() {
    return dataFormat;
  }

  public String getCharset() {
    return charset;
  }

  public boolean getRemoveCtrlChars() {
    return removeCtrlChars;
  }

  public boolean isProduceSingleRecordPerMessage() {
    return produceSingleRecordPerMessage;
  }

  public int getTextMaxLineLen() {
    return textMaxLineLen;
  }

  public JsonMode getJsonContent() {
    return jsonContent;
  }

  public int getJsonMaxObjectLen() {
    return jsonMaxObjectLen;
  }

  public CsvMode getCsvFileFormat() {
    return csvFileFormat;
  }

  public CsvHeader getCsvHeader() {
    return csvHeader;
  }

  public int getCsvMaxObjectLen() {
    return csvMaxObjectLen;
  }

  public String getXmlRecordElement() {
    return xmlRecordElement;
  }

  public int getXmlMaxObjectLen() {
    return xmlMaxObjectLen;
  }

  public int getMaxWaitTime() {
    return maxWaitTime;
  }

  public LogMode getLogMode() {
    return logMode;
  }

  public int getLogMaxObjectLen() {
    return logMaxObjectLen;
  }

  public boolean isRetainOriginalLine() {
    return retainOriginalLine;
  }

  public String getCustomLogFormat() {
    return customLogFormat;
  }

  public String getRegex() {
    return regex;
  }

  public String getGrokPatternDefinition() {
    return grokPatternDefinition;
  }

  public String getGrokPattern() {
    return grokPattern;
  }

  public List<RegExConfig> getFieldPathsToGroupName() {
    return fieldPathsToGroupName;
  }

  public boolean isEnableLog4jCustomLogFormat() {
    return enableLog4jCustomLogFormat;
  }

  public String getLog4jCustomLogFormat() {
    return log4jCustomLogFormat;
  }

  public int getMaxStackTraceLines() {
    return maxStackTraceLines;
  }

  public OnParseError getOnParseError() {
    return onParseError;
  }

  public int getMaxBatchSize() {
    return maxBatchSize;
  }

  public Map<String, String> getKafkaConsumerConfigs() {
    return kafkaConsumerConfigs;
  }

  public boolean isSchemaInMessage() {
    return schemaInMessage;
  }

  public String getAvroSchema() {
    return avroSchema;
  }

  public int getBinaryMaxObjectLen() {
    return binaryMaxObjectLen;
  }

  public char getCsvCustomDelimiter() {
    return csvCustomDelimiter;
  }

  public char getCsvCustomEscape() {
    return csvCustomEscape;
  }

  public char getCsvCustomQuote() {
    return csvCustomQuote;
  }

  public CsvRecordType getCsvRecordType() {
    return csvRecordType;
  }

  public boolean isRemoveCtrlChars() {
    return removeCtrlChars;
  }

  public String getProtoDescriptorFile() {
    return protoDescriptorFile;
  }

  public String getMessageType() {
    return messageType;
  }
}
