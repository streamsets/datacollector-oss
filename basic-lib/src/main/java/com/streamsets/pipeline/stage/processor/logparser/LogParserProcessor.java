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
package com.streamsets.pipeline.stage.processor.logparser;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.common.DataFormatConstants;
import com.streamsets.pipeline.config.LogMode;
import com.streamsets.pipeline.config.OnParseError;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.DataParserFormat;
import com.streamsets.pipeline.lib.parser.log.LogDataFormatValidator;
import com.streamsets.pipeline.lib.parser.log.RegExConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LogParserProcessor extends SingleLaneRecordProcessor {

  private final String fieldPathToParse;
  private final boolean removeCtrlChars;
  private final String parsedFieldPath;
  private final LogMode logMode;
  private final int logMaxObjectLen;
  private final String customLogFormat;
  private final String regex;
  private final List<RegExConfig> fieldPathsToGroupName;
  private final String grokPatternDefinition;
  private final String grokPattern;
  private final boolean enableLog4jCustomLogFormat;
  private final String log4jCustomLogFormat;
  private final OnParseError onParseError;
  private final int maxStackTraceLines;

  public LogParserProcessor(String fieldPathToParse, boolean removeCtrlChars, String parsedFieldPath, LogMode logMode,
      String customLogFormat,
      String regex, List<RegExConfig> fieldPathsToGroupName, String grokPatternDefinition,
      String grokPattern, boolean enableLog4jCustomLogFormat, String log4jCustomLogFormat) {
    this.fieldPathToParse = fieldPathToParse;
    this.removeCtrlChars = removeCtrlChars;
    this.parsedFieldPath = parsedFieldPath;
    this.logMode = logMode;
    this.logMaxObjectLen = -1;
    this.customLogFormat = customLogFormat;
    this.regex = regex;
    this.fieldPathsToGroupName = fieldPathsToGroupName;
    this.grokPatternDefinition = grokPatternDefinition;
    this.grokPattern = grokPattern;
    this.enableLog4jCustomLogFormat = enableLog4jCustomLogFormat;
    this.log4jCustomLogFormat = log4jCustomLogFormat;
    this.onParseError = OnParseError.ERROR;
    this.maxStackTraceLines = 0;
  }

  private LogDataFormatValidator logDataFormatValidator;
  private DataParserFactory parserFactory;

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    logDataFormatValidator = new LogDataFormatValidator(logMode, logMaxObjectLen,
      false, customLogFormat, regex, grokPatternDefinition, grokPattern,
      enableLog4jCustomLogFormat, log4jCustomLogFormat, onParseError, maxStackTraceLines,
      com.streamsets.pipeline.stage.origin.spooldir.Groups.DATA_FORMAT.name(),
      getFieldPathToGroupMap(fieldPathsToGroupName));
    logDataFormatValidator.validateLogFormatConfig(getContext(), "", issues);

    DataParserFactoryBuilder builder = new DataParserFactoryBuilder(getContext(), DataParserFormat.LOG);
    builder.setOverRunLimit(DataFormatConstants.MAX_OVERRUN_LIMIT);
    builder.setRemoveCtrlChars(removeCtrlChars);
    logDataFormatValidator.populateBuilder(builder);

    try {
      parserFactory = builder.build();
    } catch (Exception ex) {
      issues.add(
          getContext().createConfigIssue(
              null,
              null,
              Errors.LOGP_04,
              ex.toString(),
              ex
          )
      );
    }
    return issues;
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    Field field = record.get(fieldPathToParse);
    if (field == null) {
      throw new OnRecordErrorException(Errors.LOGP_00, fieldPathToParse, record.getHeader().getSourceId());
    } else {
      String value = field.getValueAsString();
      if (value == null) {
        throw new OnRecordErrorException(Errors.LOGP_01, fieldPathToParse, record.getHeader().getSourceId());
      }
      try (DataParser parser = parserFactory.getParser(record.getHeader().getSourceId(), value)){
        Record r = parser.parse();
        if(r != null) {
          Field parsed = r.get();
          record.set(parsedFieldPath, parsed);
        }
      } catch (IOException | DataParserException ex) {
        throw new OnRecordErrorException(
            Errors.LOGP_03,
            fieldPathToParse,
            record.getHeader().getSourceId(),
            ex.toString(),
            ex
        );
      }
      if (!record.has(parsedFieldPath)) {
        throw new OnRecordErrorException(Errors.LOGP_02, parsedFieldPath, record.getHeader().getSourceId());
      }
      batchMaker.addRecord(record);
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
