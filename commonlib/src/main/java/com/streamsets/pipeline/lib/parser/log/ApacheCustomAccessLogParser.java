/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.log;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.io.OverrunReader;
import com.streamsets.pipeline.lib.parser.DataParserException;

import java.io.IOException;

public class ApacheCustomAccessLogParser extends GrokParser {

  public ApacheCustomAccessLogParser(Stage.Context context, String readerId, OverrunReader reader, long readerOffset,
                         int maxObjectLen, boolean retainOriginalText, String customLogFormat)
    throws IOException, DataParserException {
    super(context, readerId, reader, readerOffset, maxObjectLen, retainOriginalText, "",
      ApacheCustomLogHelper.translateApacheLayoutToGrok(customLogFormat),
      ImmutableList.of(Constants.GROK_PATTERNS_FILE_NAME), -1);
  }

  @Override
  protected void handleNoMatch(String logLine) throws DataParserException {
    throw new DataParserException(Errors.LOG_PARSER_03, logLine, "Common Log Format");
  }
}
