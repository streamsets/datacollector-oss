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

public class CombinedLogParser extends LogDataParser {

  // Example combined log format line:
  // 127.0.0.1 user-identifier frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326
  // "http://www.example.com/start.html" "Mozilla/4.08 [en] (Win98; I ;Nav)"

  //Apache log format string
  //%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-agent}i\"
  private static final String COMBINED_LOG_PATTERN =
    "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+) \"([^\"]*)\" \"([^\"]*)\"";
  private static final Pattern PATTERN = Pattern.compile(COMBINED_LOG_PATTERN);

  public CombinedLogParser(Stage.Context context, String readerId, OverrunReader reader, long readerOffset,
                           int maxObjectLen, boolean retainOriginalText) throws IOException {
    super(context, readerId, reader, readerOffset, maxObjectLen, retainOriginalText);
  }

  @Override
  protected Map<String, Field> parseLogLine(StringBuilder sb) throws DataParserException {
    Matcher m = PATTERN.matcher(sb.toString());
    if (!m.find()) {
      throw new DataParserException(Errors.LOG_PARSER_03, sb.toString(), "Combined Log Format");
    }
    Map<String, Field> map = new HashMap<>();
    map.put(ApacheAccessLogHelper.formatToFieldNameMap.get(ApacheAccessLogHelper.LOG_FORMAT_REMOTE_HOST),
      Field.create(m.group(1)));
    map.put(ApacheAccessLogHelper.formatToFieldNameMap.get(ApacheAccessLogHelper.LOG_FORMAT_REMOTE_LOGNAME),
      Field.create(m.group(2)));
    map.put(ApacheAccessLogHelper.formatToFieldNameMap.get(ApacheAccessLogHelper.LOG_FORMAT_REMOTE_USER),
      Field.create(m.group(3)));
    map.put(ApacheAccessLogHelper.formatToFieldNameMap.get(ApacheAccessLogHelper.LOG_FORMAT_REQUEST_RECEIVED_TIME),
      Field.create(m.group(4)));
    map.put(ApacheAccessLogHelper.formatToFieldNameMap.get(ApacheAccessLogHelper.LOG_FORMAT_REQUEST_METHOD),
      Field.create(m.group(5)));
    map.put(ApacheAccessLogHelper.formatToFieldNameMap.get(ApacheAccessLogHelper.LOG_FORMAT_URL_PATH),
      Field.create(m.group(6)));
    map.put(ApacheAccessLogHelper.formatToFieldNameMap.get(ApacheAccessLogHelper.LOG_FORMAT_REQUEST_PROTOCOL),
      Field.create(m.group(7)));
    map.put(ApacheAccessLogHelper.formatToFieldNameMap.get(ApacheAccessLogHelper.LOG_FORMAT_FINAL_REQUEST_STATUS),
      Field.create(m.group(8)));
    map.put(ApacheAccessLogHelper.formatToFieldNameMap.get(ApacheAccessLogHelper.LOG_FORMAT_RESPONSE_SIZE),
      Field.create(m.group(9)));
    map.put(ApacheAccessLogHelper.formatToFieldNameMap.get(ApacheAccessLogHelper.LOG_FORMAT_REFERER),
      Field.create(m.group(10)));
    map.put(ApacheAccessLogHelper.formatToFieldNameMap.get(ApacheAccessLogHelper.LOG_FORMAT_USER_AGENT),
      Field.create(m.group(11)));
    return map;
  }
}
