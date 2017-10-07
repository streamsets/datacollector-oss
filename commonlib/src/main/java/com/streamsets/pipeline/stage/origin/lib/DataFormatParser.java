/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ToErrorContext;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.DatagramMode;
import com.streamsets.pipeline.config.OriginAvroSchemaSource;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.RecoverableDataParserException;
import com.streamsets.pipeline.lib.parser.log.LogDataFormatValidator;
import com.streamsets.pipeline.lib.parser.log.RegExConfig;
import com.streamsets.pipeline.lib.parser.net.netflow.NetflowDataParserFactory;
import com.streamsets.pipeline.lib.parser.text.TextDataParserFactory;
import com.streamsets.pipeline.lib.parser.xml.XmlDataParserFactory;
import com.streamsets.pipeline.lib.util.DelimitedDataConstants;
import com.streamsets.pipeline.lib.util.ProtobufConstants;
import com.streamsets.pipeline.lib.xml.xpath.XPathValidatorUtil;
import com.streamsets.pipeline.stage.common.DataFormatErrors;
import com.streamsets.pipeline.stage.common.DataFormatGroups;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.streamsets.pipeline.lib.util.AvroSchemaHelper.SCHEMA_ID_KEY;
import static com.streamsets.pipeline.lib.util.AvroSchemaHelper.SCHEMA_KEY;
import static com.streamsets.pipeline.lib.util.AvroSchemaHelper.SCHEMA_REPO_URLS_KEY;
import static com.streamsets.pipeline.lib.util.AvroSchemaHelper.SCHEMA_SOURCE_KEY;
import static com.streamsets.pipeline.lib.util.AvroSchemaHelper.SUBJECT_KEY;
import static org.apache.commons.lang.StringUtils.isEmpty;

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

  public List<Stage.ConfigIssue> init(Stage.Context context) {
    return init(context, "");
  }

  public List<Stage.ConfigIssue> init(Stage.Context context, String configPrefix) {
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    final String prefix = configPrefix + DATA_FORMAT_CONFIG_PREFIX;
    switch (dataFormat) {
      case JSON:
        if (dataFormatConfig.jsonMaxObjectLen < 1) {
          issues.add(
              context.createConfigIssue(
                  DataFormat.JSON.name(),
                  prefix + "maxJsonObjectLen",
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
                  prefix + "maxLogLineLength",
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
                  prefix + "csvMaxObjectLen",
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
                  configPrefix + "messageConfig.produceSingleRecordPerMessage",
                  ParserErrors.PARSER_06
              )
          );
        }
        if (dataFormatConfig.xmlMaxObjectLen < 1) {
          issues.add(
              context.createConfigIssue(
                  DataFormatGroups.DATA_FORMAT.name(),
                  prefix + "maxXmlObjectLen",
                  ParserErrors.PARSER_04
              )
          );
        }
        if (StringUtils.isNotBlank(dataFormatConfig.xmlRecordElement)) {
          String invalidXPathError = XPathValidatorUtil.getXPathValidationError(dataFormatConfig.xmlRecordElement);
          if (StringUtils.isNotBlank(invalidXPathError)) {
            issues.add(context.createConfigIssue(
                DataFormatGroups.DATA_FORMAT.name(),
                prefix + "xmlRecordElement",
                ParserErrors.PARSER_02,
                dataFormatConfig.xmlRecordElement,
                invalidXPathError
            ));
          } else {
            final Set<String> nsPrefixes = XPathValidatorUtil.getNamespacePrefixes(dataFormatConfig.xmlRecordElement);
            nsPrefixes.removeAll(dataFormatConfig.xPathNamespaceContext.keySet());
            if (!nsPrefixes.isEmpty()) {
              issues.add(context.createConfigIssue(
                  DataFormatGroups.DATA_FORMAT.name(),
                  prefix + "xPathNamespaceContext",
                  ParserErrors.PARSER_09,
                  StringUtils.join(nsPrefixes, ", ")
              ));
            }
          }
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
        logDataFormatValidator.validateLogFormatConfig(context, prefix, issues);
        break;
      case AVRO:
        if(dataFormatConfig.avroSchemaSource == OriginAvroSchemaSource.INLINE && isEmpty(dataFormatConfig.avroSchema)) {
          issues.add(
              context.createConfigIssue(
                  DataFormat.AVRO.name(),
                  prefix + "avroSchema",
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
              DataFormatGroups.DATA_FORMAT.name(),
                prefix + "protoDescriptorFile",
              DataFormatErrors.DATA_FORMAT_07
            )
          );
        } else {
          File file = new File(context.getResourcesDirectory(), dataFormatConfig.protoDescriptorFile);
          if (!file.exists()) {
            issues.add(
              context.createConfigIssue(
                DataFormatGroups.DATA_FORMAT.name(),
                  prefix + "protoDescriptorFile",
                DataFormatErrors.DATA_FORMAT_09,
                file.getAbsolutePath()
              )
            );
          }
          if (dataFormatConfig.messageType == null || dataFormatConfig.messageType.isEmpty()) {
            issues.add(
              context.createConfigIssue(
                DataFormatGroups.DATA_FORMAT.name(),
                  prefix + "messageType",
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
                  prefix + "maxWholeFileObjectLen",
                  ParserErrors.PARSER_04
              )
          );
        }
        break;
      case BINARY:
        if (dataFormatConfig.binaryMaxObjectLen < 1) {
          issues.add(
              context.createConfigIssue(
                  DataFormatGroups.DATA_FORMAT.name(),
                  prefix + "binaryMaxObjectLen",
                  ParserErrors.PARSER_04
              )
          );
        }
        break;
      case DATAGRAM:
        if (dataFormatConfig.datagramMode == DatagramMode.COLLECTD) {
          dataFormatConfig.checkCollectdParserConfigs(context, prefix, issues);
        } else if (dataFormatConfig.datagramMode == DatagramMode.NETFLOW) {
          NetflowDataParserFactory.validateConfigs(
              context,
              issues,
              DataFormatGroups.DATA_FORMAT.name(),
              prefix,
              dataFormatConfig.maxTemplateCacheSizeDatagram,
              dataFormatConfig.templateCacheTimeoutMsDatagram,
              "maxTemplateCacheSizeDatagram",
              "templateCacheTimeoutMsDatagram"
          );
        }
        break;
      case NETFLOW:
        NetflowDataParserFactory.validateConfigs(
            context,
            issues,
            DataFormatGroups.DATA_FORMAT.name(),
            prefix,
            dataFormatConfig.maxTemplateCacheSize,
            dataFormatConfig.templateCacheTimeoutMs
        );
        break;
      case SYSLOG:
        // nothing to validate
        break;
      default:
        issues.add(
            context.createConfigIssue(
                parentName,
                configPrefix +"dataFormat",
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
          .setConfig(DelimitedDataConstants.NULL_CONSTANT, dataFormatConfig.nullConstant)
          .setConfig(DelimitedDataConstants.COMMENT_ALLOWED_CONFIG, dataFormatConfig.csvEnableComments)
          .setConfig(DelimitedDataConstants.COMMENT_MARKER_CONFIG, dataFormatConfig.csvCommentMarker)
          .setConfig(DelimitedDataConstants.IGNORE_EMPTY_LINES_CONFIG, dataFormatConfig.csvIgnoreEmptyLines)
          .setConfig(DelimitedDataConstants.ALLOW_EXTRA_COLUMNS, dataFormatConfig.csvAllowExtraColumns)
          .setConfig(DelimitedDataConstants.EXTRA_COLUMN_PREFIX, dataFormatConfig.csvExtraColumnPrefix)
          ;
        break;
      case XML:
        builder.setMaxDataLen(dataFormatConfig.xmlMaxObjectLen);
        builder.setConfig(XmlDataParserFactory.RECORD_ELEMENT_KEY, dataFormatConfig.xmlRecordElement);
        builder.setConfig(XmlDataParserFactory.INCLUDE_FIELD_XPATH_ATTRIBUTES_KEY,
            dataFormatConfig.includeFieldXpathAttributes);
        builder.setConfig(XmlDataParserFactory.RECORD_ELEMENT_XPATH_NAMESPACES_KEY, dataFormatConfig.xPathNamespaceContext);
        builder.setConfig(XmlDataParserFactory.USE_FIELD_ATTRIBUTES, dataFormatConfig.outputFieldAttributes);
        break;
      case SDC_JSON:
        builder.setMaxDataLen(-1);
        break;
      case LOG:
        logDataFormatValidator.populateBuilder(builder);
        break;
      case AVRO:
        builder
            .setMaxDataLen(Integer.MAX_VALUE)
            .setConfig(SCHEMA_KEY, dataFormatConfig.avroSchema)
            .setConfig(SUBJECT_KEY, dataFormatConfig.subject)
            .setConfig(SCHEMA_ID_KEY, dataFormatConfig.schemaId)
            .setConfig(SCHEMA_SOURCE_KEY, dataFormatConfig.avroSchemaSource)
            .setConfig(SCHEMA_REPO_URLS_KEY, dataFormatConfig.schemaRegistryUrls);
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
      case BINARY:
        builder.setMaxDataLen(dataFormatConfig.binaryMaxObjectLen);
        break;
      case DATAGRAM:
        dataFormatConfig.buildDatagramParser(builder);
        break;
      case SYSLOG:
        builder.setMaxDataLen(-1);
        break;
      case NETFLOW:
        builder
          .setMaxDataLen(-1)
          .setConfig(NetflowDataParserFactory.OUTPUT_VALUES_MODE_KEY, dataFormatConfig.netflowOutputValuesMode)
          .setConfig(NetflowDataParserFactory.MAX_TEMPLATE_CACHE_SIZE_KEY, dataFormatConfig.maxTemplateCacheSize)
          .setConfig(NetflowDataParserFactory.TEMPLATE_CACHE_TIMEOUT_MS_KEY, dataFormatConfig.templateCacheTimeoutMs);
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

  public <CT extends Stage.Context & ToErrorContext> List<Record> parse(CT context, String messageId, byte[] payload) throws StageException {
    List<Record> records = new ArrayList<>();
    try (DataParser parser = parserFactory.getParser(messageId, payload)) {
      Record record = null;
      do {
        try {
          record = parser.parse();
        } catch (RecoverableDataParserException e) {
          handleException(context, messageId, e, e.getUnparsedRecord());
          //Go to next record
          continue;
        }
        if (record != null) {
          records.add(record);
        }
      } while (record != null);
    } catch (IOException |DataParserException ex) {
      Record record = context.createRecord(messageId);
      record.set(Field.create(payload));
      handleException(context, messageId, ex, record);
      return records;
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

  private <CT extends Stage.Context & ToErrorContext> void handleException(CT context, String messageId, Exception ex, Record record)
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
