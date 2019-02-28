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
package com.streamsets.pipeline.lib.parser.log;

import com.streamsets.pipeline.api.ProtoConfigurableEntity;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.config.LogMode;
import com.streamsets.pipeline.config.OnParseError;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.shaded.org.aicer.grok.dictionary.GrokDictionary;
import com.streamsets.pipeline.lib.parser.shaded.org.aicer.grok.exception.GrokCompilationException;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class LogDataFormatValidator {

  private final LogMode logMode;
  private final int logMaxObjectLen;
  private final boolean logRetainOriginalLine;
  private final String customLogFormat;
  private final String regex;
  private final String grokPatternDefinition;
  private final List<String> grokPatternList;
  private final Map<String, Integer> fieldPathsToGroupName;
  private final boolean enableLog4jCustomLogFormat;
  private final String log4jCustomLogFormat;
  private final int maxStackTraceLines;
  private final OnParseError onParseError;
  private final String groupName;

  public LogDataFormatValidator(
      LogMode logMode,
      int logMaxObjectLen,
      boolean logRetainOriginalLine,
      String customLogFormat,
      String regex,
      String grokPatternDefinition,
      List<String> grokPatternList,
      boolean enableLog4jCustomLogFormat,
      String log4jCustomLogFormat,
      OnParseError onParseError,
      int maxStackTraceLines,
      String groupName,
      Map<String, Integer> fieldPathsToGroupName
  ) {
    this.logMode = logMode;
    this.logMaxObjectLen = logMaxObjectLen;
    this.logRetainOriginalLine = logRetainOriginalLine;
    this.customLogFormat = customLogFormat;
    this.regex = regex;
    this.grokPatternDefinition = grokPatternDefinition;
    this.grokPatternList = grokPatternList;
    this.enableLog4jCustomLogFormat = enableLog4jCustomLogFormat;
    this.log4jCustomLogFormat = log4jCustomLogFormat;
    this.maxStackTraceLines = maxStackTraceLines;
    this.groupName = groupName;
    this.fieldPathsToGroupName = fieldPathsToGroupName;
    this.onParseError = onParseError;
  }

  public void populateBuilder(DataParserFactoryBuilder builder) {
    builder.setMaxDataLen(logMaxObjectLen)
      .setConfig(LogDataParserFactory.RETAIN_ORIGINAL_TEXT_KEY, logRetainOriginalLine)
      .setConfig(LogDataParserFactory.APACHE_CUSTOMLOG_FORMAT_KEY, customLogFormat)
      .setConfig(LogDataParserFactory.REGEX_KEY, regex)
      .setConfig(LogDataParserFactory.REGEX_FIELD_PATH_TO_GROUP_KEY, fieldPathsToGroupName)
      .setConfig(LogDataParserFactory.GROK_PATTERN_DEFINITION_KEY, grokPatternDefinition)
      .setConfig(LogDataParserFactory.GROK_PATTERN_KEY, grokPatternList)
      .setConfig(LogDataParserFactory.LOG4J_FORMAT_KEY, log4jCustomLogFormat)
      .setConfig(LogDataParserFactory.ON_PARSE_ERROR_KEY, onParseError)
      .setConfig(LogDataParserFactory.LOG4J_TRIM_STACK_TRACES_TO_LENGTH_KEY, maxStackTraceLines)
      .setMode(logMode);
  }

  public void validateLogFormatConfig(ProtoConfigurableEntity.Context context, List<Stage.ConfigIssue> issues) {
    validateLogFormatConfig(context, "", issues);
  }

  public void validateLogFormatConfig(ProtoConfigurableEntity.Context context, String configPrefix, List<Stage.ConfigIssue> issues) {
    if (logMaxObjectLen == 0 || logMaxObjectLen < -1) {
      issues.add(
          context.createConfigIssue(
              groupName,
              configPrefix + "logMaxObjectLen",
              Errors.LOG_PARSER_04,
              logMaxObjectLen
          )
      );
    }
    if(maxStackTraceLines < 0) {
      issues.add(
          context.createConfigIssue(
              groupName,
              configPrefix + "maxStackTraceLines",
              Errors.LOG_PARSER_10,
              maxStackTraceLines
          )
      );
    }
    if(logMode == LogMode.APACHE_CUSTOM_LOG_FORMAT) {
      validateApacheCustomLogFormat(context, configPrefix, issues);
    } else if(logMode == LogMode.REGEX) {
      validateRegExFormat(context, configPrefix, issues);
    } else if(logMode == LogMode.GROK) {
      validateGrokPattern(context, configPrefix, issues);
    } else if (logMode == LogMode.LOG4J) {
      validateLog4jCustomLogFormat(context, configPrefix, issues);
    }
  }

  private void validateApacheCustomLogFormat(
      ProtoConfigurableEntity.Context context,
      String configPrefix,
      List<Stage.ConfigIssue> issues
  ) {
    if(customLogFormat == null || customLogFormat.isEmpty()) {
      issues.add(
          context.createConfigIssue(
              groupName,
              configPrefix + "customLogFormat",
              Errors.LOG_PARSER_05,
              customLogFormat
          )
      );
      return;
    }
    try {
      ApacheCustomLogHelper.translateApacheLayoutToGrok(customLogFormat);
    } catch (DataParserException e) {
      issues.add(
          context.createConfigIssue(
              groupName,
              configPrefix + "customLogFormat",
              Errors.LOG_PARSER_06,
              customLogFormat,
              e.toString(),
              e
          )
      );
    }
  }

  private void validateLog4jCustomLogFormat(
      ProtoConfigurableEntity.Context context,
      String configPrefix,
      List<Stage.ConfigIssue> issues
  ) {
    if(enableLog4jCustomLogFormat) {
      if (log4jCustomLogFormat == null || log4jCustomLogFormat.isEmpty()) {
        issues.add(
            context.createConfigIssue(
                groupName,
                configPrefix + "log4jCustomLogFormat",
                Errors.LOG_PARSER_05,
                log4jCustomLogFormat
            )
        );
        return;
      }
      try {
        Log4jHelper.translateLog4jLayoutToGrok(log4jCustomLogFormat);
      } catch (DataParserException e) {
        issues.add(
            context.createConfigIssue(
                groupName,
                configPrefix + "log4jCustomLogFormat",
                Errors.LOG_PARSER_06,
                log4jCustomLogFormat,
                e.toString(),
                e
            )
        );
      }
    }
  }

  private void validateRegExFormat(
      ProtoConfigurableEntity.Context context,
      String configPrefix,
      List<Stage.ConfigIssue> issues
  ) {
    try {
      Pattern compile = Pattern.compile(regex);
      Matcher matcher = compile.matcher(" ");
      int groupCount = matcher.groupCount();

      for(int group : fieldPathsToGroupName.values()) {
        if(group > groupCount) {
          issues.add(
              context.createConfigIssue(
                  groupName,
                  configPrefix + "fieldPathsToGroupName",
                  Errors.LOG_PARSER_08,
                  regex,
                  groupCount,
                  group
              )
          );
        }
      }
    } catch (PatternSyntaxException e) {
      issues.add(
          context.createConfigIssue(
              groupName,
              configPrefix + "regex",
              Errors.LOG_PARSER_07,
              regex,
              e.toString(),
              e
          )
      );
    }
  }

  private void validateGrokPattern(
      ProtoConfigurableEntity.Context context,
      String configPrefix,
      List<Stage.ConfigIssue> issues
  ) {
    try(
      InputStream grogPatterns = getClass().getClassLoader().getResourceAsStream(Constants.GROK_PATTERNS_FILE_NAME);
      InputStream javaPatterns = getClass().getClassLoader().getResourceAsStream(Constants.GROK_JAVA_LOG_PATTERNS_FILE_NAME);
    ) {
      GrokDictionary grokDictionary = new GrokDictionary();

      grokDictionary.addDictionary(grogPatterns);
      grokDictionary.addDictionary(javaPatterns);
      if(grokPatternDefinition != null && !grokPatternDefinition.isEmpty()) {
        grokDictionary.addDictionary(new StringReader(grokPatternDefinition));
      }
      grokDictionary.bind();
      for (String grokPattern : grokPatternList) {
        grokDictionary.compileExpression(grokPattern);
      }
    } catch (GrokCompilationException|IOException e){
      issues.add(
        context.createConfigIssue(
          groupName,
          configPrefix + "grokPatternDefinition",
          Errors.LOG_PARSER_11,
          grokPatternDefinition,
          e.toString(),
          e
        )
      );
    }
    catch (PatternSyntaxException e) {
      issues.add(
          context.createConfigIssue(
              groupName,
              configPrefix + "regex",
              Errors.LOG_PARSER_09,
              regex,
              e.toString(),
              e
          )
      );
    }
  }

}
