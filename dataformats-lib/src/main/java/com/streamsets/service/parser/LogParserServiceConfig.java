/*
 * Copyright 2019 StreamSets Inc.
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

package com.streamsets.service.parser;

import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.ProtoConfigurableEntity;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.common.DataFormatConstants;
import com.streamsets.pipeline.config.CharsetChooserValues;
import com.streamsets.pipeline.config.Compression;
import com.streamsets.pipeline.config.CompressionChooserValues;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.LogMode;
import com.streamsets.pipeline.config.LogModeChooserValues;
import com.streamsets.pipeline.config.OnParseError;
import com.streamsets.pipeline.config.OnParseErrorChooserValues;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.log.LogDataFormatValidator;
import com.streamsets.pipeline.lib.parser.log.LogDataParserFactory;
import com.streamsets.pipeline.lib.parser.log.RegExConfig;
import com.streamsets.pipeline.stage.common.DataFormatErrors;
import com.streamsets.pipeline.stage.common.DataFormatGroups;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LogParserServiceConfig {

  private static final Logger LOG = LoggerFactory.getLogger(LogParserServiceConfig.class);

  private static final String DEFAULT_REGEX =
      "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+)";
  private static final String DEFAULT_APACHE_CUSTOM_LOG_FORMAT = "%h %l %u %t \"%r\" %>s %b";
  private static final String DEFAULT_GROK_PATTERN = "%{COMMONAPACHELOG}";
  private static final String DEFAULT_LOG4J_CUSTOM_FORMAT = "%r [%t] %-5p %c %x - %m%n";

  private LogDataFormatValidator logDataFormatValidator;
  private DataParserFactory parserFactory;
  // Size of StringBuilder pool maintained by Text and Log Data Parser Factories.
  // It is equal to the max number of runners in multi-threaded pipelines configured in sdc.properties,
  // with a default value of 50.
  public int stringBuilderPoolSize;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "COMMON_LOG_FORMAT",
      label = "Log Format",
      description = "",
      displayPosition = 30,
      group = "DATA_FORMAT"
  )
  @ValueChooserModel(LogModeChooserValues.class)
  public LogMode logMode = LogMode.COMMON_LOG_FORMAT;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1024",
      label = "Max Line Length",
      description = "Longer lines are truncated",
      displayPosition = 40,
      group = "DATA_FORMAT",
      min = 1,
      max = Integer.MAX_VALUE
  )
  public int logMaxObjectLen = 1024;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Retain Original Line",
      description = "Indicates if the original line of log should be retained in the record",
      displayPosition = 50,
      group = "DATA_FORMAT"
  )
  public boolean retainOriginalLine = false;

  //APACHE_CUSTOM_LOG_FORMAT

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = DEFAULT_APACHE_CUSTOM_LOG_FORMAT,
      label = "Custom Log Format",
      description = "",
      displayPosition = 60,
      group = "DATA_FORMAT",
      dependsOn = "logMode",
      triggeredByValue = "APACHE_CUSTOM_LOG_FORMAT"
  )
  public String customLogFormat = DEFAULT_APACHE_CUSTOM_LOG_FORMAT;

  //REGEX

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = DEFAULT_REGEX,
      label = "Regular Expression",
      description = "The regular expression which is used to parse the log line.",
      displayPosition = 70,
      group = "DATA_FORMAT",
      dependsOn = "logMode",
      triggeredByValue = "REGEX"
  )
  public String regex = DEFAULT_REGEX;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "",
      label = "Field Path To RegEx Group Mapping",
      description = "Map groups in the regular expression to field paths",
      displayPosition = 80,
      group = "DATA_FORMAT",
      dependsOn = "logMode",
      triggeredByValue = "REGEX"
  )
  @ListBeanModel
  public List<RegExConfig> fieldPathsToGroupName = new ArrayList<>();

  //GROK

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.TEXT,
      defaultValue = "",
      label = "Grok Pattern Definition",
      description = "Define your own grok patterns which will be used to parse the logs",
      displayPosition = 90,
      group = "DATA_FORMAT",
      dependsOn = "logMode",
      triggeredByValue = "GROK",
      mode = ConfigDef.Mode.PLAIN_TEXT
  )
  public String grokPatternDefinition = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.LIST,
      defaultValue = "[\"" + DEFAULT_GROK_PATTERN + "\"]",
      label = "Grok Patterns",
      description = "List of grok patterns. The first pattern that matches the log line is used to parse it",
      displayPosition = 100,
      group = "DATA_FORMAT",
      dependsOn = "logMode",
      triggeredByValue = "GROK"
  )
  public List<String> grokPatternList = Arrays.asList(DEFAULT_GROK_PATTERN);

  //LOG4J

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "ERROR",
      label = "On Parse Error",
      description = "",
      displayPosition = 110,
      group = "DATA_FORMAT",
      dependsOn = "logMode",
      triggeredByValue = "LOG4J"
  )
  @ValueChooserModel(OnParseErrorChooserValues.class)
  public OnParseError onParseError = OnParseError.ERROR;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "50",
      label = "Trim Stack Trace to Length",
      description = "Any line that does not match the expected pattern will be treated as a Stack trace as long as it " +
          "is part of the same message. The stack trace will be trimmed to the specified number of lines.",
      displayPosition = 120,
      group = "DATA_FORMAT",
      dependsOn = "onParseError",
      triggeredByValue = "INCLUDE_AS_STACK_TRACE",
      min = 0,
      max = Integer.MAX_VALUE
  )
  public int maxStackTraceLines = 50;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Use Custom Log Format",
      description = "",
      displayPosition = 130,
      group = "DATA_FORMAT",
      dependsOn = "logMode",
      triggeredByValue = "LOG4J"
  )
  public boolean enableLog4jCustomLogFormat = false;


  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = DEFAULT_LOG4J_CUSTOM_FORMAT,
      label = "Custom Log4J Format",
      description = "Specify your own custom log4j format.",
      displayPosition = 140,
      group = "DATA_FORMAT",
      dependsOn = "enableLog4jCustomLogFormat",
      triggeredByValue = "true"
  )
  public String log4jCustomLogFormat = DEFAULT_LOG4J_CUSTOM_FORMAT;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "UTF-8",
      label = "Charset",
      displayPosition = 150,
      group = "DATA_FORMAT"
  )
  @ValueChooserModel(CharsetChooserValues.class)
  public String charset = "UTF-8";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Ignore Control Characters",
      description = "Use only if required as it impacts reading performance",
      displayPosition = 160,
      group = "DATA_FORMAT"
  )
  public boolean removeCtrlChars = false;

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

  public List<RegExConfig> getFieldPathsToGroupName() {
    return fieldPathsToGroupName;
  }

  public String getGrokPatternDefinition() {
    return grokPatternDefinition;
  }

  public List<String> getGrokPatternList() {
    // remove empty patterns
    return grokPatternList.stream().filter(grokPattern -> (grokPattern != null && !grokPattern.isEmpty())).collect(
        Collectors.toList());
  }

  public OnParseError getOnParseError() {
    return onParseError;
  }

  public int getMaxStackTraceLines() {
    return maxStackTraceLines;
  }

  public boolean isEnableLog4jCustomLogFormat() {
    return enableLog4jCustomLogFormat;
  }

  public String getLog4jCustomLogFormat() {
    return log4jCustomLogFormat;
  }

  public String getCharset() {
    return charset;
  }

  public boolean isRemoveCtrlChars() {
    return removeCtrlChars;
  }

  /**
   * Returns the DataParserFactory instance.
   *
   * The DataParserFactory instance is not thread safe.
   * To improve performance the DataParserFactory instance may share a buffer among the data parser instances that
   * it creates.
   *
   * @return
   */
  public DataParserFactory getParserFactory() {
    return parserFactory;
  }

  public boolean init(
      ProtoConfigurableEntity.Context context,
      DataFormat dataFormat,
      String stageGroup,
      List<Stage.ConfigIssue> issues
  ) {
    return init(context,
        dataFormat,
        stageGroup,
        DataFormatConstants.MAX_OVERRUN_LIMIT,
        false,
        issues);
  }

  public boolean init(
      ProtoConfigurableEntity.Context context,
      DataFormat dataFormat,
      String stageGroup,
      int overrunLimit,
      boolean multiLines,
      List<Stage.ConfigIssue> issues
  ) {
    Preconditions.checkState(dataFormat != null, "dataFormat cannot be NULL");

    stringBuilderPoolSize = context.getConfiguration().get(
        DataFormatConstants.MAX_RUNNERS_CONFIG_KEY,
        DataFormatConstants.DEFAULT_STRING_BUILDER_POOL_SIZE
    );

    validateLogFormat(context, issues);

    return validateDataParser(
        context,
        dataFormat,
        stageGroup,
        overrunLimit,
        multiLines,
        issues
    );
  }

  private void validateLogFormat(
      ProtoConfigurableEntity.Context context,
      List<Stage.ConfigIssue> issues
  ) {
    logDataFormatValidator = new LogDataFormatValidator(
        logMode,
        logMaxObjectLen,
        retainOriginalLine,
        customLogFormat,
        regex,
        grokPatternDefinition,
        getGrokPatternList(),
        enableLog4jCustomLogFormat,
        log4jCustomLogFormat,
        onParseError,
        maxStackTraceLines,
        DataFormatGroups.DATA_FORMAT.name(),
        getFieldPathToGroupMap(fieldPathsToGroupName)
    );
    logDataFormatValidator.validateLogFormatConfig(context, issues);
  }

  private boolean validateDataParser(
      ProtoConfigurableEntity.Context context,
      DataFormat dataFormat,
      String stageGroup,
      int overrunLimit,
      boolean multiLines,
      List<Stage.ConfigIssue> issues
  ) {
    boolean valid = true;
    DataParserFactoryBuilder builder = new DataParserFactoryBuilder(context, dataFormat.getParserFormat());
    Charset fileCharset;

    try {
      fileCharset = Charset.forName(charset);
    } catch (UnsupportedCharsetException ignored) { // NOSONAR
      // setting it to a valid one so the parser factory can be configured and tested for more errors
      fileCharset = StandardCharsets.UTF_8;
      issues.add(context.createConfigIssue(stageGroup,
          "charset",
          DataFormatErrors.DATA_FORMAT_05,
          charset
      ));
      valid = false;
    }
    builder.setCharset(fileCharset);
    builder.setOverRunLimit(overrunLimit);
    builder.setRemoveCtrlChars(removeCtrlChars);

    buildLogParser(builder, multiLines);

    try {
      parserFactory = builder.build();
    } catch (Exception ex) {
      LOG.error("Can't create parserFactory", ex);
      issues.add(context.createConfigIssue(null, null, DataFormatErrors.DATA_FORMAT_06, ex.toString(), ex));
      valid = false;
    }

    return valid;
  }

  private void buildLogParser(DataParserFactoryBuilder builder, boolean multiLines) {
    builder
        .setStringBuilderPoolSize(stringBuilderPoolSize)
        .setConfig(LogDataParserFactory.MULTI_LINES_KEY, multiLines);
    logDataFormatValidator.populateBuilder(builder);
  }

  private static Map<String, Integer> getFieldPathToGroupMap(List<RegExConfig> fieldPathsToGroupName) {
    if (fieldPathsToGroupName == null) {
      return new HashMap<>();
    }
    Map<String, Integer> fieldPathToGroup = new HashMap<>();
    for (RegExConfig r : fieldPathsToGroupName) {
      fieldPathToGroup.put(r.fieldPath, r.group);
    }
    return fieldPathToGroup;
  }
}
