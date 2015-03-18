/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.log;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.LogMode;
import com.streamsets.pipeline.lib.io.OverrunReader;
import com.streamsets.pipeline.lib.parser.CharDataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class LogCharDataParserFactory extends CharDataParserFactory {

  static final String KEY_PREFIX = "log.";
  public static final String RETAIN_ORIGINAL_TEXT_KEY = KEY_PREFIX + "retain.original.text";
  static final boolean RETAIN_ORIGINAL_TEXT_DEFAULT = false;
  public static final String APACHE_CUSTOMLOG_FORMAT_KEY = KEY_PREFIX + "apache.custom.log.format";
  static final String APACHE_CUSTOMLOG_FORMAT_DEFAULT = "%h %l %u %t \"%r\" %>s %b";
  public static final String REGEX_KEY = KEY_PREFIX + "regex";
  static final String REGEX_DEFAULT =
    "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+)";
  public static final String REGEX_FIELD_PATH_TO_GROUP_KEY = KEY_PREFIX + "regex.fieldPath.to.group.name";
  static final Map<String, Integer> REGEX_FIELD_PATH_TO_GROUP_DEFAULT = new HashMap<>();


  public static Map<String, Object> registerConfigs(Map<String, Object> configs) {
    configs.put(RETAIN_ORIGINAL_TEXT_KEY, RETAIN_ORIGINAL_TEXT_DEFAULT);
    configs.put(APACHE_CUSTOMLOG_FORMAT_KEY, APACHE_CUSTOMLOG_FORMAT_DEFAULT);
    configs.put(REGEX_KEY, REGEX_DEFAULT);
    configs.put(REGEX_FIELD_PATH_TO_GROUP_KEY, REGEX_FIELD_PATH_TO_GROUP_DEFAULT);
    return configs;
  }

  private final Stage.Context context;
  private final int maxObjectLen;
  private final LogMode logMode;
  private final boolean retainOriginalText;
  private final String customLogFormat;
  private final String regex;
  private final Map<String, Integer> fieldPathToGroup;

  public LogCharDataParserFactory(Stage.Context context, int maxObjectLen, LogMode logMode,
                                  Map<String, Object> configs) {
    this.context = context;
    this.maxObjectLen = maxObjectLen;
    this.logMode = logMode;
    this.retainOriginalText = (boolean) configs.get(RETAIN_ORIGINAL_TEXT_KEY);
    this.customLogFormat = (String) configs.get(APACHE_CUSTOMLOG_FORMAT_KEY);
    this.regex = (String) configs.get(REGEX_KEY);
    this.fieldPathToGroup = (Map<String, Integer>) configs.get(REGEX_FIELD_PATH_TO_GROUP_KEY);

  }

  @Override
  public DataParser getParser(String id, OverrunReader reader, long readerOffset) throws DataParserException {
    Utils.checkState(reader.getPos() == 0, Utils.formatL("reader must be in position '0', it is at '{}'",
      reader.getPos()));
    try {
      switch (logMode) {
        case COMMON_LOG_FORMAT:
          return new CommonLogParser(context, id, reader, readerOffset, maxObjectLen, retainOriginalText);
        case COMBINED_LOG_FORMAT:
          return new CombinedLogParser(context, id, reader, readerOffset, maxObjectLen, retainOriginalText);
        case APACHE_CUSTOM_LOG_FORMAT:
          return new ApacheCustomAccessLogParser(context, id, reader, readerOffset, maxObjectLen, retainOriginalText,
            customLogFormat);
        case APACHE_ERROR_LOG_FORMAT:
          return new ApacheErrorLogParserV22(context, id, reader, readerOffset, maxObjectLen, retainOriginalText);
        case REGEX:
          return new RegexParser(context, id, reader, readerOffset, maxObjectLen, retainOriginalText, regex,
            fieldPathToGroup);
        default :
          return null;
      }
    } catch (IOException ex) {
      throw new DataParserException(Errors.LOG_PARSER_00, id, readerOffset, ex.getMessage(), ex);
    }
  }


}
