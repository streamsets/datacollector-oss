/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.log;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.io.OverrunReader;
import com.streamsets.pipeline.lib.parser.DataParserException;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ApacheCustomAccessLogParser extends LogDataParser {

  private final Pattern pattern;
  private final Map<String, Integer> fieldToGroupMap;
  private final String customLogFormat;

  public ApacheCustomAccessLogParser(Stage.Context context, String readerId, OverrunReader reader, long readerOffset,
                                     int maxObjectLen, boolean retainOriginalText, String customLogFormat)
    throws IOException, DataParserException {
    super(context, readerId, reader, readerOffset, maxObjectLen, retainOriginalText);
    this.customLogFormat = customLogFormat;
    fieldToGroupMap = new LinkedHashMap<>();
    pattern = Pattern.compile(ApacheAccessLogHelper.convertLogFormatToRegEx(customLogFormat, fieldToGroupMap));
  }

  @Override
  protected Map<String, Field> parseLogLine(StringBuilder sb) throws DataParserException {
    Matcher m = pattern.matcher(sb.toString());
    if (!m.find()) {
      throw new DataParserException(Errors.LOG_PARSER_03, sb.toString(),
        "Apache Custom Log Format - " + customLogFormat);
    }
    Map<String, Field> map = new HashMap<>();
    for(Map.Entry<String, Integer> e : fieldToGroupMap.entrySet()) {
      map.put(e.getKey(), Field.create(m.group(e.getValue())));
    }
    return map;
  }
}
