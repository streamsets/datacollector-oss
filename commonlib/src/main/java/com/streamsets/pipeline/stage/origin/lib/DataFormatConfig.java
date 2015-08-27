/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.lib;

import com.streamsets.pipeline.api.ComplexField;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.api.impl.XMLChar;
import com.streamsets.pipeline.common.DataFormatConstants;
import com.streamsets.pipeline.config.CharsetChooserValues;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvHeaderChooserValues;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.CsvModeChooserValues;
import com.streamsets.pipeline.config.CsvRecordType;
import com.streamsets.pipeline.config.CsvRecordTypeChooserValues;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.DataFormatChooserValues;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.config.JsonModeChooserValues;
import com.streamsets.pipeline.config.LogMode;
import com.streamsets.pipeline.config.LogModeChooserValues;
import com.streamsets.pipeline.config.OnParseError;
import com.streamsets.pipeline.config.OnParseErrorChooserValues;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.avro.AvroDataParserFactory;
import com.streamsets.pipeline.lib.parser.delimited.DelimitedDataParserFactory;
import com.streamsets.pipeline.lib.parser.log.LogDataFormatValidator;
import com.streamsets.pipeline.lib.parser.log.RegExConfig;
import com.streamsets.pipeline.lib.parser.xml.XmlDataParserFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataFormatConfig {

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    label = "Data Format",
    displayPosition = 3000,
    group = "#0"
  )
  @ValueChooser(DataFormatChooserValues.class)
  public DataFormat dataFormat;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "UTF-8",
    label = "Messages Charset",
    displayPosition = 3010,
    group = "#0",
    dependsOn = "dataFormat",
    triggeredByValue = {"TEXT", "JSON", "DELIMITED", "XML", "LOG"}
  )
  @ValueChooser(CharsetChooserValues.class)
  public String charset;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.BOOLEAN,
    defaultValue = "false",
    label = "Ignore Control Characters",
    description = "Use only if required as it impacts reading performance",
    displayPosition = 3020,
    group = "#0"
  )
  public boolean removeCtrlChars;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.NUMBER,
    defaultValue = "1024",
    label = "Max Line Length",
    description = "Longer lines are truncated",
    displayPosition = 100,
    group = "TEXT",
    dependsOn = "dataFormat",
    triggeredByValue = "TEXT",
    min = 1,
    max = Integer.MAX_VALUE
  )
  public int textMaxLineLen = 1024;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "MULTIPLE_OBJECTS",
    label = "JSON Content",
    description = "",
    displayPosition = 100,
    group = "JSON",
    dependsOn = "dataFormat",
    triggeredByValue = "JSON"
  )
  @ValueChooser(JsonModeChooserValues.class)
  public JsonMode jsonContent;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.NUMBER,
    defaultValue = "4096",
    label = "Max Object Length (chars)",
    description = "Larger objects are not processed",
    displayPosition = 110,
    group = "JSON",
    dependsOn = "dataFormat",
    triggeredByValue = "JSON",
    min = 1,
    max = Integer.MAX_VALUE
  )
  public int jsonMaxObjectLen;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "CSV",
    label = "Delimiter Format Type",
    description = "",
    displayPosition = 200,
    group = "DELIMITED",
    dependsOn = "dataFormat",
    triggeredByValue = "DELIMITED"
  )
  @ValueChooser(CsvModeChooserValues.class)
  public CsvMode csvFileFormat;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "NO_HEADER",
    label = "Header Line",
    description = "",
    displayPosition = 210,
    group = "DELIMITED",
    dependsOn = "dataFormat",
    triggeredByValue = "DELIMITED"
  )
  @ValueChooser(CsvHeaderChooserValues.class)
  public CsvHeader csvHeader;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.NUMBER,
    defaultValue = "1024",
    label = "Max Record Length (chars)",
    description = "Larger objects are not processed",
    displayPosition = 220,
    group = "DELIMITED",
    dependsOn = "dataFormat",
    triggeredByValue = "DELIMITED",
    min = 1,
    max = Integer.MAX_VALUE
  )
  public int csvMaxObjectLen;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.CHARACTER,
    defaultValue = "|",
    label = "Delimiter Character",
    displayPosition = 330,
    group = "DELIMITED",
    dependsOn = "csvFileFormat",
    triggeredByValue = "CUSTOM"
  )
  public char csvCustomDelimiter;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.CHARACTER,
    defaultValue = "\\",
    label = "Escape Character",
    displayPosition = 340,
    group = "DELIMITED",
    dependsOn = "csvFileFormat",
    triggeredByValue = "CUSTOM"
  )
  public char csvCustomEscape;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.CHARACTER,
    defaultValue = "\"",
    label = "Quote Character",
    displayPosition = 350,
    group = "DELIMITED",
    dependsOn = "csvFileFormat",
    triggeredByValue = "CUSTOM"
  )
  public char csvCustomQuote;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "LIST",
    label = "Record Type",
    description = "",
    displayPosition = 310,
    group = "DELIMITED",
    dependsOn = "dataFormat",
    triggeredByValue = "DELIMITED"
  )
  @ValueChooser(CsvRecordTypeChooserValues.class)
  public CsvRecordType csvRecordType;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.STRING,
    label = "Delimiter Element",
    defaultValue = "",
    description = "XML element that acts as a record delimiter. No delimiter will treat the whole XML document as one record.",
    displayPosition = 300,
    group = "XML",
    dependsOn = "dataFormat",
    triggeredByValue = "XML"
  )
  public String xmlRecordElement;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.NUMBER,
    defaultValue = "4096",
    label = "Max Record Length (chars)",
    description = "Larger records are not processed",
    displayPosition = 310,
    group = "XML",
    dependsOn = "dataFormat",
    triggeredByValue = "XML",
    min = 1,
    max = Integer.MAX_VALUE
  )
  public int xmlMaxObjectLen;

  // LOG Configuration

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "COMMON_LOG_FORMAT",
    label = "Log Format",
    description = "",
    displayPosition = 700,
    group = "LOG",
    dependsOn = "dataFormat",
    triggeredByValue = "LOG"
  )
  @ValueChooser(LogModeChooserValues.class)
  public LogMode logMode;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.NUMBER,
    defaultValue = "1024",
    label = "Max Line Length",
    description = "Longer lines are truncated",
    displayPosition = 710,
    group = "LOG",
    dependsOn = "dataFormat",
    triggeredByValue = "LOG",
    min = 1,
    max = Integer.MAX_VALUE
  )
  public int logMaxObjectLen;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.BOOLEAN,
    defaultValue = "false",
    label = "Retain Original Line",
    description = "Indicates if the original line of log should be retained in the record",
    displayPosition = 720,
    group = "LOG",
    dependsOn = "dataFormat",
    triggeredByValue = "LOG"
  )
  public boolean retainOriginalLine;

  //APACHE_CUSTOM_LOG_FORMAT
  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "%h %l %u %t \"%r\" %>s %b",
    label = "Custom Log Format",
    description = "",
    displayPosition = 730,
    group = "LOG",
    dependsOn = "logMode",
    triggeredByValue = "APACHE_CUSTOM_LOG_FORMAT"
  )
  public String customLogFormat;

  //REGEX

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+)",
    label = "Regular Expression",
    description = "The regular expression which is used to parse the log line.",
    displayPosition = 740,
    group = "LOG",
    dependsOn = "logMode",
    triggeredByValue = "REGEX"
  )
  public String regex;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "",
    label = "Field Path To RegEx Group Mapping",
    description = "Map groups in the regular expression to field paths",
    displayPosition = 750,
    group = "LOG",
    dependsOn = "logMode",
    triggeredByValue = "REGEX"
  )
  @ComplexField
  public List<RegExConfig> fieldPathsToGroupName;

  //GROK

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.TEXT,
    defaultValue = "",
    label = "Grok Pattern Definition",
    description = "Define your own grok patterns which will be used to parse the logs",
    displayPosition = 760,
    group = "LOG",
    dependsOn = "logMode",
    triggeredByValue = "GROK",
    mode = ConfigDef.Mode.PLAIN_TEXT
  )
  public String grokPatternDefinition;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "%{COMMONAPACHELOG}",
    label = "Grok Pattern",
    description = "The grok pattern which is used to parse the log line",
    displayPosition = 780,
    group = "LOG",
    dependsOn = "logMode",
    triggeredByValue = "GROK"
  )
  public String grokPattern;

  //LOG4J

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "ERROR",
    label = "On Parse Error",
    description = "",
    displayPosition = 790,
    group = "LOG",
    dependsOn = "logMode",
    triggeredByValue = "LOG4J"
  )
  @ValueChooser(OnParseErrorChooserValues.class)
  public OnParseError onParseError;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.NUMBER,
    defaultValue = "50",
    label = "Trim Stack Trace to Length",
    description = "Any line that does not match the expected pattern will be treated as a Stack trace as long as it " +
      "is part of the same message. The stack trace will be trimmed to the specified number of lines.",
    displayPosition = 800,
    group = "LOG",
    dependsOn = "onParseError",
    triggeredByValue = "INCLUDE_AS_STACK_TRACE",
    min = 0,
    max = Integer.MAX_VALUE
  )
  public int maxStackTraceLines;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.BOOLEAN,
    defaultValue = "false",
    label = "Use Custom Log Format",
    description = "",
    displayPosition = 810,
    group = "LOG",
    dependsOn = "logMode",
    triggeredByValue = "LOG4J"
  )
  public boolean enableLog4jCustomLogFormat;


  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "%r [%t] %-5p %c %x - %m%n",
    label = "Custom Log4J Format",
    description = "Specify your own custom log4j format.",
    displayPosition = 820,
    group = "LOG",
    dependsOn = "enableLog4jCustomLogFormat",
    triggeredByValue = "true"
  )
  public String log4jCustomLogFormat;

  //AVRO

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.BOOLEAN,
    defaultValue = "true",
    label = "Message includes Schema",
    description = "The Kafka message includes the Avro schema",
    displayPosition = 830,
    group = "AVRO",
    dependsOn = "dataFormat",
    triggeredByValue = "AVRO"
  )
  public boolean schemaInMessage;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.TEXT,
    defaultValue = "",
    label = "Avro Schema",
    description = "Overrides the schema associated with the message. Optionally use the runtime:loadResource function to use a schema stored in a file",
    displayPosition = 840,
    group = "AVRO",
    dependsOn = "dataFormat",
    triggeredByValue = "AVRO",
    mode = ConfigDef.Mode.JSON
  )
  public String avroSchema;

  public boolean init(Stage.Context context, List<Stage.ConfigIssue> issues, String stageGroup) {
    return init(context, issues, stageGroup, DataFormatConstants.MAX_OVERRUN_LIMIT);
  }

  public boolean init(Stage.Context context, List<Stage.ConfigIssue> issues, String stageGroup, int overrunLimit) {
    boolean valid = true;
    switch (dataFormat) {
      case JSON:
        if (jsonMaxObjectLen < 1) {
          issues.add(context.createConfigIssue(DataFormatGroups.JSON.name(), "jsonMaxObjectLen",
            DataFormatErrors.DATA_FORMAT_01));
          valid &= false;
        }
        break;
      case TEXT:
        if (textMaxLineLen < 1) {
          issues.add(context.createConfigIssue(DataFormatGroups.TEXT.name(), "textMaxLineLen",
            DataFormatErrors.DATA_FORMAT_01));
          valid &= false;
        }
        break;
      case DELIMITED:
        if (csvMaxObjectLen < 1) {
          issues.add(context.createConfigIssue(DataFormatGroups.DELIMITED.name(), "csvMaxObjectLen",
            DataFormatErrors.DATA_FORMAT_01));
          valid &= false;
        }
        break;
      case XML:
        if (xmlMaxObjectLen < 1) {
          issues.add(context.createConfigIssue(DataFormatGroups.XML.name(), "xmlMaxObjectLen",
            DataFormatErrors.DATA_FORMAT_01));
          valid &= false;
        }
        if (xmlRecordElement == null || xmlRecordElement.isEmpty()) {
          issues.add(context.createConfigIssue(DataFormatGroups.XML.name(), "xmlRecordElement",
            DataFormatErrors.DATA_FORMAT_02));
          valid &= false;
        } else if (!XMLChar.isValidName(xmlRecordElement)) {
          issues.add(context.createConfigIssue(DataFormatGroups.XML.name(), "xmlRecordElement",
            DataFormatErrors.DATA_FORMAT_03,
            xmlRecordElement));
          valid &= false;
        }
        break;
      case SDC_JSON:
        break;
      case AVRO:
        break;
      case LOG:
        logDataFormatValidator = new LogDataFormatValidator(logMode, logMaxObjectLen,
          retainOriginalLine, customLogFormat, regex,
          grokPatternDefinition, grokPattern,
          enableLog4jCustomLogFormat, log4jCustomLogFormat,
          onParseError, maxStackTraceLines, DataFormatGroups.LOG.name(),
          getFieldPathToGroupMap(fieldPathsToGroupName));
        logDataFormatValidator.validateLogFormatConfig(issues, context);
        break;
      default:
        issues.add(context.createConfigIssue(stageGroup, "dataFormat", DataFormatErrors.DATA_FORMAT_04,
          dataFormat));
        valid &= false;
        break;
    }

    valid &= validateDataParser(context, issues, stageGroup, overrunLimit);

    return valid;
  }

  private boolean validateDataParser(Stage.Context context, List<Stage.ConfigIssue> issues, String stageGroup,
    int overrunLimit) {
    boolean valid = true;
    DataParserFactoryBuilder builder = new DataParserFactoryBuilder(context,
      dataFormat.getParserFormat());

    try {
      fileCharset = Charset.forName(charset);
    } catch (UnsupportedCharsetException ex) {
      // setting it to a valid one so the parser factory can be configured and tested for more errors
      fileCharset = StandardCharsets.UTF_8;
      issues.add(context.createConfigIssue(stageGroup, "charset", DataFormatErrors.DATA_FORMAT_05, charset));
      valid &= false;
    }
    builder.setCharset(fileCharset);
    builder.setOverRunLimit(overrunLimit);
    builder.setRemoveCtrlChars(removeCtrlChars);

    switch (dataFormat) {
      case TEXT:
        builder.setMaxDataLen(textMaxLineLen);
        break;
      case JSON:
        builder.setMaxDataLen(jsonMaxObjectLen).setMode(jsonContent);
        break;
      case DELIMITED:
        builder.setMaxDataLen(csvMaxObjectLen).setMode(csvFileFormat)
          .setMode(csvHeader)
          .setConfig(DelimitedDataParserFactory.DELIMITER_CONFIG, csvCustomDelimiter)
          .setConfig(DelimitedDataParserFactory.ESCAPE_CONFIG, csvCustomEscape)
          .setConfig(DelimitedDataParserFactory.QUOTE_CONFIG, csvCustomQuote);
        break;
      case XML:
        builder.setMaxDataLen(xmlMaxObjectLen).setConfig(XmlDataParserFactory.RECORD_ELEMENT_KEY,
          xmlRecordElement);
        break;
      case SDC_JSON:
        builder.setMaxDataLen(-1);
        break;
      case LOG:
        logDataFormatValidator.populateBuilder(builder);
        break;
      case AVRO:
        builder.setMaxDataLen(-1).setConfig(AvroDataParserFactory.SCHEMA_KEY, avroSchema);
        break;
    }
    try {
      parserFactory = builder.build();
    } catch (Exception ex) {
      issues.add(context.createConfigIssue(null, null, DataFormatErrors.DATA_FORMAT_06, ex.toString(), ex));
      valid &= false;
    }
    return valid;
  }

  public DataParserFactory getParserFactory() {
    return parserFactory;
  }

  private LogDataFormatValidator logDataFormatValidator;
  private Charset fileCharset;
  private DataParserFactory parserFactory;

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
