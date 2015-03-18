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

public class ApacheErrorLogParserV22 extends LogDataParser {

  //Visible for testing
  static final String DATE_TIME = "dateTime";
  static final String SEVERITY = "severity";
  static final String CLIENT_IP_ADDRESS = "clientIpAddress";
  static final String MESSAGE = "message";

  // Example error log format line:
  //Apache http server version 2.2
  // [Wed Oct 11 14:32:52 2000] [error] [client 127.0.0.1] client denied by server configuration: /export/home/live/ap/htdocs/test
  private static final String ERROR_LOG_PATTERN =
    "^\\[([^\\]]+)\\] \\[(\\S+)\\] \\[client (\\S+)\\] (.*)$";
  private static final Pattern PATTERN = Pattern.compile(ERROR_LOG_PATTERN);


  public ApacheErrorLogParserV22(Stage.Context context, String readerId, OverrunReader reader, long readerOffset,
                                 int maxObjectLen, boolean retainOriginalText) throws IOException {
    super(context, readerId, reader, readerOffset, maxObjectLen, retainOriginalText);
  }

  @Override
  protected Map<String, Field> parseLogLine(StringBuilder sb) throws DataParserException {
    Matcher m = PATTERN.matcher(sb.toString());
    if (!m.find()) {
      if (!m.find()) {
        throw new DataParserException(Errors.LOG_PARSER_03, sb.toString(), "Apache Error Log Format");
      }
    }
    Map<String, Field> map = new HashMap<>();

    map.put(DATE_TIME, Field.create(m.group(1)));
    map.put(SEVERITY, Field.create(m.group(2)));
    map.put(CLIENT_IP_ADDRESS, Field.create(m.group(3)));
    map.put(MESSAGE, Field.create(m.group(4)));

    return map;
  }
}
