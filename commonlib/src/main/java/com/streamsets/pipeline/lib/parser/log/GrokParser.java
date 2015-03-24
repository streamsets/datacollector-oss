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
import com.streamsets.pipeline.lib.parser.shaded.org.aicer.grok.dictionary.GrokDictionary;
import com.streamsets.pipeline.lib.parser.shaded.org.aicer.grok.util.Grok;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GrokParser extends LogDataParser {

  private final Grok compiledPattern;

  public GrokParser(Stage.Context context, String readerId, OverrunReader reader, long readerOffset, int maxObjectLen,
                    boolean retainOriginalText, String grokPatternDefinition, String grokPattern, List<String> dictionaries) throws IOException {
    super(context, readerId, reader, readerOffset, maxObjectLen, retainOriginalText);
    GrokDictionary grokDictionary = new GrokDictionary();
    //Add grok patterns and Java patterns by default
    grokDictionary.addDictionary(getClass().getClassLoader().getResourceAsStream(Constants.GROK_PATTERNS_FILE_NAME));
    grokDictionary.addDictionary(getClass().getClassLoader().getResourceAsStream(
      Constants.GROK_JAVA_LOG_PATTERNS_FILE_NAME));
    for(String dictionary : dictionaries) {
      grokDictionary.addDictionary(getClass().getClassLoader().getResourceAsStream(dictionary));
    }
    if(grokPatternDefinition != null || !grokPatternDefinition.isEmpty()) {
      grokDictionary.addDictionary(new StringReader(grokPatternDefinition));
    }
    // Resolve all expressions loaded
    grokDictionary.bind();

    compiledPattern = grokDictionary.compileExpression(grokPattern);

  }

  @Override
  public Map<String, Field> parseLogLine(StringBuilder logLine) throws DataParserException {
    Map<String, String> namedGroupToValuesMap = compiledPattern.extractNamedGroups(logLine.toString());
    if(namedGroupToValuesMap == null) {
      //Did not match
      handleNoMatch(logLine.toString());
    }
    Map<String, Field> map = new HashMap<>();
    for(Map.Entry<String, String> e : namedGroupToValuesMap.entrySet()) {
      map.put(e.getKey(), Field.create(e.getValue()));
    }
    return map;
  }

  protected void handleNoMatch(String logLine) throws DataParserException {
    throw new DataParserException(Errors.LOG_PARSER_03, logLine, "Grok Format");
  }

}
