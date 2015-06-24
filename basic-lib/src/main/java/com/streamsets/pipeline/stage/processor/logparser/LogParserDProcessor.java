/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.logparser;

import com.streamsets.pipeline.api.ComplexField;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.config.LogMode;
import com.streamsets.pipeline.config.LogModeChooserValues;
import com.streamsets.pipeline.configurablestage.DProcessor;
import com.streamsets.pipeline.lib.parser.log.RegExConfig;

import java.util.List;

@StageDef(
    version="1.0.0",
    label="Log Parser",
    description = "Parses a string field which contains a Log line",
    icon="logparser.png"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class LogParserDProcessor extends DProcessor {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "/text",
      label = "Field to Parse",
      description = "String field that contains a LOG line",
      displayPosition = 10,
      group = "LOG"
  )
  public String fieldPathToParse;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Ignore Control Characters",
      description = "Use only if required as it impacts reading performance",
      displayPosition = 15,
      group = "LOG"
  )
  public boolean removeCtrlChars;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "/",
    label = "New Parsed Field",
    description="Name of the new field to set the parsed JSON data",
    displayPosition = 20,
    group = "LOG"
  )
  public String parsedFieldPath;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "COMMON_LOG_FORMAT",
    label = "Log Format",
    description = "",
    displayPosition = 40,
    group = "LOG"
  )
  @ValueChooser(LogModeChooserValues.class)
  public LogMode logMode;

  //APACHE_CUSTOM_LOG_FORMAT
  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "%h %l %u %t \"%r\" %>s %b",
    label = "Custom Log Format",
    description = "",
    displayPosition = 60,
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
    description = "The regular expression which is used to parse the log line",
    displayPosition = 70,
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
    displayPosition = 80,
    group = "LOG",
    dependsOn = "logMode",
    triggeredByValue = "REGEX"
  )
  @ComplexField(RegExConfig.class)
  public List<RegExConfig> fieldPathsToGroupName;

  //GROK

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.TEXT,
    defaultValue = "",
    label = "Grok Pattern Definition",
    description = "Define your own grok patterns which will be used to parse the logs",
    displayPosition = 90,
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
    description = "The grok pattern which is used to parse the log line.",
    displayPosition = 100,
    group = "LOG",
    dependsOn = "logMode",
    triggeredByValue = "GROK"
  )
  public String grokPattern;

  //LOG4J

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
    label = "Custom Format",
    description = "Specify your own custom log4j format.",
    displayPosition = 820,
    group = "LOG",
    dependsOn = "enableLog4jCustomLogFormat",
    triggeredByValue = "true"
  )
  public String log4jCustomLogFormat;

  @Override
  protected Processor createProcessor() {
    return new LogParserProcessor(fieldPathToParse, removeCtrlChars, parsedFieldPath,logMode, customLogFormat,
      regex, fieldPathsToGroupName, grokPatternDefinition, grokPattern, enableLog4jCustomLogFormat,
      log4jCustomLogFormat);
  }
}
