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
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexParser extends LogDataParser {

  private final String regex;
  private final Pattern pattern;
  private final Map<String, Integer> fieldToGroupMap;

  public RegexParser(Stage.Context context, String readerId, OverrunReader reader, long readerOffset, int maxObjectLen,
                     boolean retainOriginalText, String regex, Map<String, Integer> fieldToGroupMap) throws IOException {
    super(context, readerId, reader, readerOffset, maxObjectLen, retainOriginalText);
    this.regex = regex;
    this.fieldToGroupMap = fieldToGroupMap;
    this.pattern = Pattern.compile(regex);
  }

  @Override
  protected Map<String, Field> parseLogLine(StringBuilder sb) throws DataParserException {
    Matcher m = pattern.matcher(sb.toString());
    if (!m.find()) {
      if (!m.find()) {
        throw new DataParserException(Errors.LOG_PARSER_03, sb.toString(), "Regular Expression - " + regex);
      }
    }
    Map<String, Field> map = new HashMap<>();
    for(Map.Entry<String, Integer> e : fieldToGroupMap.entrySet()) {
      map.put(e.getKey(), Field.create(m.group(e.getValue())));
    }
    return map;
  }
}
