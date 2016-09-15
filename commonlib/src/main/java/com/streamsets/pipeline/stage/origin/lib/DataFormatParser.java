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
package com.streamsets.pipeline.stage.origin.lib;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.impl.XMLChar;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.avro.AvroDataParserFactory;
import com.streamsets.pipeline.lib.parser.log.LogDataFormatValidator;
import com.streamsets.pipeline.lib.parser.log.RegExConfig;
import com.streamsets.pipeline.lib.parser.text.TextDataParserFactory;
import com.streamsets.pipeline.lib.parser.xml.XmlDataParserFactory;
import com.streamsets.pipeline.lib.util.DelimitedDataConstants;
import com.streamsets.pipeline.lib.util.ProtobufConstants;
import com.streamsets.pipeline.stage.common.DataFormatErrors;
import com.streamsets.pipeline.stage.common.DataFormatGroups;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataFormatParser {
  public static final String DATA_FORMAT_CONFIG_PREFIX = "dataFormatConfig.";
  private final String parentName;
  private final DataParserFormatConfig dataFormatConfig;
  private final MessageConfig messageConfig;
  private final DataFormat dataFormat;
  private LogDataFormatValidator logDataFormatValidator;
  private Charset messageCharset;
  private DataParserFactory parserFactory;

  public DataFormatParser(
      String parentName,
      DataFormat dataFormat,
      DataParserFormatConfig dataFormatConfig,
      MessageConfig messageConfig
  ) {
    this.parentName = parentName;
    this.dataFormatConfig = dataFormatConfig;
    this.messageConfig = messageConfig;
    this.dataFormat = dataFormat;
  }

  public List<Stage.ConfigIssue> init(Source.Context context) {
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    switch (dataFormat) {
      case JSON:
        if (dataFormatConfig.jsonMaxObjectLen < 1) {
          issues.add(
              context.createConfigIssue(
                  DataFormat.JSON.name(),
                  DATA_FORMAT_CONFIG_PREFIX + "maxJsonObjectLen",
                  ParserErrors.PARSER_04
              )
          );
        }
        break;
      case TEXT:
        if (dataFormatConfig.textMaxLineLen < 1) {
          issues.add(
              context.createConfigIssue(
                  DataFormat.TEXT.name(),
                  DATA_FORMAT_CONFIG_PREFIX + "maxLogLineLength",
                  ParserErrors.PARSER_04
              )
          );
        }
        break;
      case DELIMITED:
        if (dataFormatConfig.csvMaxObjectLen < 1) {
          issues.add(
              context.createConfigIssue(
                  DataFormat.DELIMITED.name(),
                  DATA_FORMAT_CONFIG_PREFIX + "csvMaxObjectLen",
                  ParserErrors.PARSER_04
              )
          );
        }
        break;
      case XML:
        if (messageConfig != null && messageConfig.produceSingleRecordPerMessage) {
          issues.add(
              context.createConfigIssue(
                  parentName,
                  "messageConfig.produceSingleRecordPerMessage",
                  ParserErrors.PARSER_06
              )
          );
        }
        if (dataFormatConfig.xmlMaxObjectLen < 1) {
          issues.add(
              context.createConfigIssue(
                  DataFormat.XML.name(),
                  DATA_FORMAT_CONFIG_PREFIX + "maxXmlObjectLen",
                  ParserErrors.PARSER_04
              )
          );
        }
        if (dataFormatConfig.xmlRecordElement != null && !dataFormatConfig.xmlRecordElement.isEmpty() &&
          !XMLChar.isValidName(dataFormatConfig.xmlRecordElement)) {
          issues.add(
              context.createConfigIssue(
                  DataFormat.XML.name(),
                  DATA_FORMAT_CONFIG_PREFIX + "xmlRecordElement",
                  ParserErrors.PARSER_02,
                  dataFormatConfig.xmlRecordElement
              )
          );
        }
        break;
      case SDC_JSON:
        break;
      case LOG:
        logDataFormatValidator = new LogDataFormatValidator(
            dataFormatConfig.logMode,
            dataFormatConfig.logMaxObjectLen,
            dataFormatConfig.retainOriginalLine,
            dataFormatConfig.customLogFormat,
            dataFormatConfig.regex,
            dataFormatConfig.grokPatternDefinition,
            dataFormatConfig.grokPattern,
            dataFormatConfig.enableLog4jCustomLogFormat,
            dataFormatConfig.log4jCustomLogFormat,
            dataFormatConfig.onParseError,
            dataFormatConfig.maxStackTraceLines,
            DataFormat.LOG.name(),
            getFieldPathToGroupMap(dataFormatConfig.fieldPathsToGroupName)
        );
        logDataFormatValidator.validateLogFormatConfig(context, DATA_FORMAT_CONFIG_PREFIX, issues);
        break;
      case AVRO:
        if(!dataFormatConfig.schemaInMessage
            && (dataFormatConfig.avroSchema == null || dataFormatConfig.avroSchema.isEmpty())) {
          issues.add(
              context.createConfigIssue(
                  DataFormat.AVRO.name(),
                  DATA_FORMAT_CONFIG_PREFIX + "avroSchema",
                  ParserErrors.PARSER_07,
                  dataFormatConfig.avroSchema
              )
          );
        }
        break;
      case PROTOBUF:
        if (dataFormatConfig.protoDescriptorFile == null || dataFormatConfig.protoDescriptorFile.isEmpty()) {
          issues.add(
            context.createConfigIssue(
              DataFormatGroups.PROTOBUF.name(),
              DATA_FORMAT_CONFIG_PREFIX + "protoDescriptorFile",
              DataFormatErrors.DATA_FORMAT_07
            )
          );
        } else {
          File file = new File(context.getResourcesDirectory(), dataFormatConfig.protoDescriptorFile);
          if (!file.exists()) {
            issues.add(
              context.createConfigIssue(
                DataFormatGroups.PROTOBUF.name(),
                DATA_FORMAT_CONFIG_PREFIX + "protoDescriptorFile",
                DataFormatErrors.DATA_FORMAT_09,
                file.getAbsolutePath()
              )
            );
          }
          if (dataFormatConfig.messageType == null || dataFormatConfig.messageType.isEmpty()) {
            issues.add(
              context.createConfigIssue(
                DataFormatGroups.PROTOBUF.name(),
                DATA_FORMAT_CONFIG_PREFIX + "messageType",
                DataFormatErrors.DATA_FORMAT_08
              )
            );
          }
        }
        break;
      case WHOLE_FILE:
        if (dataFormatConfig.wholeFileMaxObjectLen < 1) {
          issues.add(
              context.createConfigIssue(
                  DataFormat.XML.name(),
                  DATA_FORMAT_CONFIG_PREFIX + "maxWholeFileObjectLen",
                  ParserErrors.PARSER_04
              )
          );
        }
        break;
      default:
        issues.add(
            context.createConfigIssue(
                parentName,
                "dataFormat",
                ParserErrors.PARSER_05,
                dataFormat
            )
        );
    }

    DataParserFactoryBuilder builder = new DataParserFactoryBuilder(context, dataFormat.getParserFormat())
      .setCharset(Charset.defaultCharset());
    if (dataFormatConfig.charset == null) {
      messageCharset = StandardCharsets.UTF_8;
    } else {
      try {
        messageCharset = Charset.forName(dataFormatConfig.charset);
      } catch (UnsupportedCharsetException ex) {
        // setting it to a valid one so the parser factory can be configured and tested for more errors
        messageCharset = StandardCharsets.UTF_8;
        issues.add(
            context.createConfigIssue(
                parentName,
                "charset",
                ParserErrors.PARSER_01,
                dataFormatConfig.charset
            )
        );
      }
    }
    builder.setCharset(messageCharset).setRemoveCtrlChars(dataFormatConfig.removeCtrlChars);

    switch (dataFormat) {
      case TEXT:
        builder.setMaxDataLen(dataFormatConfig.textMaxLineLen)
            .setConfig(TextDataParserFactory.USE_CUSTOM_DELIMITER_KEY, dataFormatConfig.useCustomDelimiter)
            .setConfig(TextDataParserFactory.CUSTOM_DELIMITER_KEY, dataFormatConfig.customDelimiter)
            .setConfig(TextDataParserFactory.INCLUDE_CUSTOM_DELIMITER_IN_TEXT_KEY, dataFormatConfig.includeCustomDelimiterInTheText);
        break;
      case JSON:
        builder.setMode(dataFormatConfig.jsonContent);
        builder.setMaxDataLen(dataFormatConfig.jsonMaxObjectLen);
        break;
      case DELIMITED:
        builder.setMaxDataLen(dataFormatConfig.csvMaxObjectLen)
          .setMode(dataFormatConfig.csvFileFormat).setMode(dataFormatConfig.csvHeader)
          .setMode(dataFormatConfig.csvRecordType)
          .setConfig(DelimitedDataConstants.DELIMITER_CONFIG, dataFormatConfig.csvCustomDelimiter)
          .setConfig(DelimitedDataConstants.ESCAPE_CONFIG, dataFormatConfig.csvCustomEscape)
          .setConfig(DelimitedDataConstants.QUOTE_CONFIG, dataFormatConfig.csvCustomQuote)
          .setConfig(DelimitedDataConstants.PARSE_NULL, dataFormatConfig.parseNull)
          .setConfig(DelimitedDataConstants.NULL_CONSTANT, dataFormatConfig.nullConstant);
        break;
      case XML:
        builder.setMaxDataLen(dataFormatConfig.xmlMaxObjectLen);
        builder.setConfig(XmlDataParserFactory.RECORD_ELEMENT_KEY, dataFormatConfig.xmlRecordElement);
        break;
      case SDC_JSON:
        builder.setMaxDataLen(-1);
        break;
      case LOG:
        logDataFormatValidator.populateBuilder(builder);
        break;
      case AVRO:
        builder.setMaxDataLen(Integer.MAX_VALUE)
          .setConfig(AvroDataParserFactory.SCHEMA_KEY, dataFormatConfig.avroSchema)
          .setConfig(AvroDataParserFactory.SCHEMA_IN_MESSAGE_KEY, dataFormatConfig.schemaInMessage);
        break;
      case PROTOBUF:
        builder
          .setConfig(ProtobufConstants.PROTO_DESCRIPTOR_FILE_KEY, dataFormatConfig.protoDescriptorFile)
          .setConfig(ProtobufConstants.MESSAGE_TYPE_KEY, dataFormatConfig.messageType)
          .setConfig(ProtobufConstants.DELIMITED_KEY, dataFormatConfig.isDelimited)
          .setMaxDataLen(-1);
        break;
      case WHOLE_FILE:
        builder.setMaxDataLen(dataFormatConfig.wholeFileMaxObjectLen);
        break;
      default:
        throw new IllegalStateException(Utils.format("Unknown data format: {}", dataFormat));
    }
    try {
      parserFactory = builder.build();
    } catch (Exception ex) {
      issues.add(context.createConfigIssue(null, null, DataFormatErrors.DATA_FORMAT_06, ex.toString(), ex));
    }
    return issues;
  }

  public List<Record> parse(Source.Context context, String messageId, byte[] payload) throws StageException {
    List<Record> records = new ArrayList<>();
    try (DataParser parser = parserFactory.getParser(messageId, payload)) {
      Record record = parser.parse();
      while (record != null) {
        records.add(record);
        record = parser.parse();
      }
    } catch (IOException |DataParserException ex) {
      Record record = context.createRecord(messageId);
      record.set(Field.create(payload));
      handleException(context, messageId, ex, record);
    }
    if (messageConfig != null && messageConfig.produceSingleRecordPerMessage) {
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

  private void handleException(Source.Context context, String messageId, Exception ex, Record record)
    throws StageException {
    switch (context.getOnErrorRecord()) {
      case DISCARD:
        break;
      case TO_ERROR:
        context.toError(record, ParserErrors.PARSER_03, messageId, ex.toString(), ex);
        break;
      case STOP_PIPELINE:
        if (ex instanceof StageException) {
          throw (StageException) ex;
        } else {
          throw new StageException(ParserErrors.PARSER_03, messageId, ex.toString(), ex);
        }
      default:
        throw new IllegalStateException(Utils.format("Unknown on error value '{}'",
          context.getOnErrorRecord(), ex));
    }
  }

  public Charset getCharset() {
    return messageCharset;
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
