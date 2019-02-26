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

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.CharsetChooserValues;
import com.streamsets.pipeline.config.Compression;
import com.streamsets.pipeline.config.CompressionChooserValues;
import com.streamsets.pipeline.config.LogMode;
import com.streamsets.pipeline.config.LogModeChooserValues;
import com.streamsets.pipeline.config.OnParseError;
import com.streamsets.pipeline.config.OnParseErrorChooserValues;
import com.streamsets.pipeline.lib.parser.log.RegExConfig;

import java.util.ArrayList;
import java.util.List;

public class LogParserServiceConfig {

  private static final String DEFAULT_REGEX =
      "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+)";
  private static final String DEFAULT_APACHE_CUSTOM_LOG_FORMAT = "%h %l %u %t \"%r\" %>s %b";
  private static final String DEFAULT_GROK_PATTERN = "%{COMMONAPACHELOG}";
  private static final String DEFAULT_LOG4J_CUSTOM_FORMAT = "%r [%t] %-5p %c %x - %m%n";

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
      type = ConfigDef.Type.STRING,
      defaultValue = DEFAULT_GROK_PATTERN,
      label = "Grok Pattern",
      description = "The grok pattern which is used to parse the log line",
      displayPosition = 100,
      group = "DATA_FORMAT",
      dependsOn = "logMode",
      triggeredByValue = "GROK"
  )
  public String grokPattern = DEFAULT_GROK_PATTERN;

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

  public String getGrokPattern() {
    return grokPattern;
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
}
