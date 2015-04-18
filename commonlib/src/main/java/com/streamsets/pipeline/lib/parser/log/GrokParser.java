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
import com.streamsets.pipeline.lib.parser.shaded.org.aicer.grok.util.Grok;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class GrokParser extends LogCharDataParser {

  private final Grok compiledPattern;
  private final String formatName;

  public GrokParser(Stage.Context context, String readerId, OverrunReader reader, long readerOffset, int maxObjectLen,
                    boolean retainOriginalText, int maxStackTraceLines, Grok compiledPattern, String formatName)
    throws IOException {
    super(context, readerId, reader, readerOffset, maxObjectLen, retainOriginalText, maxStackTraceLines);
    this.compiledPattern = compiledPattern;
    this.formatName = formatName;
  }

  @Override
  public Map<String, Field> parseLogLine(StringBuilder logLine) throws DataParserException {
    Map<String, String> namedGroupToValuesMap = compiledPattern.extractNamedGroups(logLine.toString());
    if(namedGroupToValuesMap == null) {
      //Did not match
      handleNoMatch(logLine.toString());
    }
    Map<String, Field> map = new LinkedHashMap<>();
    for(Map.Entry<String, String> e : namedGroupToValuesMap.entrySet()) {
      map.put(e.getKey(), Field.create(e.getValue()));
    }
    return map;
  }

  protected void handleNoMatch(String logLine) throws DataParserException {
    throw new DataParserException(Errors.LOG_PARSER_03, logLine, formatName);
  }

}
