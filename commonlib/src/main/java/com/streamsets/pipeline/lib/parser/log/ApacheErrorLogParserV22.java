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

public class ApacheErrorLogParserV22 extends GrokParser {

  public ApacheErrorLogParserV22(Stage.Context context, String readerId, OverrunReader reader, long readerOffset,
                                 int maxObjectLen, boolean retainOriginalText) throws IOException {
    super(context, readerId, reader, readerOffset, maxObjectLen, retainOriginalText, "",
      Constants.GROK_APACHE_ERROR_LOG_FORMAT,
      ImmutableList.of(Constants.GROK_PATTERNS_FILE_NAME, Constants.GROK_APACHE_ERROR_LOG_PATTERNS_FILE_NAME), -1);
  }

  @Override
  protected void handleNoMatch(String logLine) throws DataParserException {
    throw new DataParserException(Errors.LOG_PARSER_03, logLine, "Apache Error Log Format Version 2.2");
  }

}
