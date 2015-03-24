/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.log;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.io.OverrunReader;

import java.io.IOException;

public class CombinedLogParser extends GrokParser {

  // Example combined log format line:
  // 127.0.0.1 user-identifier frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326
  // "http://www.example.com/start.html" "Mozilla/4.08 [en] (Win98; I ;Nav)"

  //Apache log format string
  //%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-agent}i\"
  public CombinedLogParser (Stage.Context context, String readerId, OverrunReader reader, long readerOffset,
                         int maxObjectLen, boolean retainOriginalText) throws IOException {
    super(context, readerId, reader, readerOffset, maxObjectLen, retainOriginalText, "",
      Constants.GROK_COMBINED_APACHE_LOG_FORMAT, ImmutableList.of(Constants.GROK_PATTERNS_FILE_NAME));
  }
}
