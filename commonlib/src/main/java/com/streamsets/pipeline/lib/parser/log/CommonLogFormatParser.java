/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.log;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.io.OverrunReader;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//TODO<Hari>: Replace logic with a specific parser as pattern matching is slow
public class CommonLogFormatParser extends LogDataParser {

  // Example Common log format line:
  // 127.0.0.1 user-identifier frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326
  private static final String COMMON_LOG_PATTERN =
    "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+)";
  private static final Pattern PATTERN = Pattern.compile(COMMON_LOG_PATTERN);

  public CommonLogFormatParser(Stage.Context context, String readerId, OverrunReader reader, long readerOffset,
                               int maxObjectLen, boolean retainOriginalText) throws IOException {
    super(context, readerId, reader, readerOffset, maxObjectLen, retainOriginalText);
  }

  @Override
  protected Map<String, Field> parseLogLine(StringBuilder sb) {
    Matcher m = PATTERN.matcher(sb.toString());
    if (!m.find()) {
      return null;
    }
    Map<String, Field> map = new HashMap<>();
    //IP address of the client (remote host) which made the request to the server
    map.put("ipAddress", Field.create(m.group(1)));
    //RFC 1413 identity of the client determined by identd on the clients machine
    map.put("clientId", Field.create(m.group(2)));
    //userid of the person requesting the document as determined by HTTP authentication
    map.put("userId", Field.create(m.group(3)));
    //time that the request was received
    /*
      [day/month/year:hour:minute:second zone]
      day = 2*digit
      month = 3*letter
      year = 4*digit
      hour = 2*digit
      minute = 2*digit
      second = 2*digit
      zone = (`+' | `-') 4*digit
     */
    map.put("dateTime", Field.create(m.group(4)));
    //method used by the client in request line
    map.put("method", Field.create(m.group(5)));
    //path and query string
    map.put("request", Field.create(m.group(6)));
    //protocol
    map.put("protocol", Field.create(m.group(7)));
    //the status code that the server sends back to the client
    map.put("responseCode", Field.create(m.group(8)));
    //size of the object returned to the client, not including the response headers
    map.put("size", Field.create(m.group(9)));

    return map;
  }
}
