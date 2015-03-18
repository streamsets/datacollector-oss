/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.log;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.config.LogMode;
import com.streamsets.pipeline.lib.io.OverrunReader;
import com.streamsets.pipeline.lib.parser.CharDataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TestLogCharDataParserFactory {

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR, Collections.<String>emptyList());
  }

  @Test
  public void testGetParserStringWithRetainOriginalText() throws Exception {
    Map<String, Object> configs = LogCharDataParserFactory.registerConfigs(new HashMap<String, Object>());
    configs.put(LogCharDataParserFactory.RETAIN_ORIGINAL_TEXT_KEY, true);
    CharDataParserFactory factory = new LogCharDataParserFactory(getContext(), 100, LogMode.COMMON_LOG_FORMAT, configs);
    DataParser parser = factory.getParser("id",
      "127.0.0.1 ss h [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326");

    //should get an instance of CommonLogFormatParser
    Assert.assertTrue(parser instanceof CommonLogParser);

    Assert.assertEquals(0, parser.getOffset());
    Record record = parser.parse();
    Assert.assertTrue(record.has("/originalLine"));

    Assert.assertTrue(record.has("/remoteHost"));
    Assert.assertEquals("127.0.0.1", record.get("/remoteHost").getValueAsString());

    Assert.assertTrue(record.has("/logName"));
    Assert.assertEquals("ss", record.get("/logName").getValueAsString());

    Assert.assertTrue(record.has("/remoteUser"));
    Assert.assertEquals("h", record.get("/remoteUser").getValueAsString());

    Assert.assertTrue(record.has("/requestTime"));
    Assert.assertEquals("10/Oct/2000:13:55:36 -0700", record.get("/requestTime").getValueAsString());

    Assert.assertTrue(record.has("/requestMethod"));
    Assert.assertEquals("GET", record.get("/requestMethod").getValueAsString());

    Assert.assertTrue(record.has("/urlPath"));
    Assert.assertEquals("/apache_pb.gif", record.get("/urlPath").getValueAsString());

    Assert.assertTrue(record.has("/requestProtocol"));
    Assert.assertEquals("HTTP/1.0", record.get("/requestProtocol").getValueAsString());

    Assert.assertTrue(record.has("/status"));
    Assert.assertEquals("200", record.get("/status").getValueAsString());

    Assert.assertTrue(record.has("/bytesSent"));
    Assert.assertEquals("2326", record.get("/bytesSent").getValueAsString());

    Assert.assertEquals(82, parser.getOffset()); //The log line is 82 characters treating '\"' as a single character
    parser.close();
  }

  @Test
  public void testGetParserStringWithOutRetainOriginalText() throws Exception {
    Map<String, Object> configs = LogCharDataParserFactory.registerConfigs(new HashMap<String, Object>());
    configs.put(LogCharDataParserFactory.RETAIN_ORIGINAL_TEXT_KEY, false);
    CharDataParserFactory factory = new LogCharDataParserFactory(getContext(), 100, LogMode.COMMON_LOG_FORMAT, configs);
    DataParser parser = factory.getParser("id",
      "127.0.0.1 ss h [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326");
    //should get an instance of CommonLogFormatParser
    Assert.assertTrue(parser instanceof CommonLogParser);

    Assert.assertEquals(0, parser.getOffset());
    Record record = parser.parse();
    Assert.assertFalse(record.has("/originalLine"));

    Assert.assertTrue(record.has("/remoteHost"));
    Assert.assertEquals("127.0.0.1", record.get("/remoteHost").getValueAsString());

    Assert.assertTrue(record.has("/logName"));
    Assert.assertEquals("ss", record.get("/logName").getValueAsString());

    Assert.assertTrue(record.has("/remoteUser"));
    Assert.assertEquals("h", record.get("/remoteUser").getValueAsString());

    Assert.assertTrue(record.has("/requestTime"));
    Assert.assertEquals("10/Oct/2000:13:55:36 -0700", record.get("/requestTime").getValueAsString());

    Assert.assertTrue(record.has("/requestMethod"));
    Assert.assertEquals("GET", record.get("/requestMethod").getValueAsString());

    Assert.assertTrue(record.has("/urlPath"));
    Assert.assertEquals("/apache_pb.gif", record.get("/urlPath").getValueAsString());

    Assert.assertTrue(record.has("/requestProtocol"));
    Assert.assertEquals("HTTP/1.0", record.get("/requestProtocol").getValueAsString());

    Assert.assertTrue(record.has("/status"));
    Assert.assertEquals("200", record.get("/status").getValueAsString());

    Assert.assertTrue(record.has("/bytesSent"));
    Assert.assertEquals("2326", record.get("/bytesSent").getValueAsString());
    Assert.assertEquals(82, parser.getOffset()); //The log line is 82 characters treating '\"' as a single character
    parser.close();
  }

  @Test
  public void testGetParserReader() throws Exception {
    Map<String, Object> configs = LogCharDataParserFactory.registerConfigs(new HashMap<String, Object>());
    configs.put(LogCharDataParserFactory.RETAIN_ORIGINAL_TEXT_KEY, true);
    CharDataParserFactory factory = new LogCharDataParserFactory(getContext(), 100, LogMode.COMMON_LOG_FORMAT, configs);
    OverrunReader reader = new OverrunReader(
      new StringReader("127.0.0.1 ss h [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326"), 100,
      true);
    DataParser parser = factory.getParser("id", reader, 0);
    //should get an instance of CommonLogFormatParser
    Assert.assertTrue(parser instanceof CommonLogParser);

    Assert.assertEquals(0, parser.getOffset());
    Record record = parser.parse();
    Assert.assertTrue(record.has("/originalLine"));

    Assert.assertTrue(record.has("/remoteHost"));
    Assert.assertEquals("127.0.0.1", record.get("/remoteHost").getValueAsString());

    Assert.assertTrue(record.has("/logName"));
    Assert.assertEquals("ss", record.get("/logName").getValueAsString());

    Assert.assertTrue(record.has("/remoteUser"));
    Assert.assertEquals("h", record.get("/remoteUser").getValueAsString());

    Assert.assertTrue(record.has("/requestTime"));
    Assert.assertEquals("10/Oct/2000:13:55:36 -0700", record.get("/requestTime").getValueAsString());

    Assert.assertTrue(record.has("/requestMethod"));
    Assert.assertEquals("GET", record.get("/requestMethod").getValueAsString());

    Assert.assertTrue(record.has("/urlPath"));
    Assert.assertEquals("/apache_pb.gif", record.get("/urlPath").getValueAsString());

    Assert.assertTrue(record.has("/requestProtocol"));
    Assert.assertEquals("HTTP/1.0", record.get("/requestProtocol").getValueAsString());

    Assert.assertTrue(record.has("/status"));
    Assert.assertEquals("200", record.get("/status").getValueAsString());

    Assert.assertTrue(record.has("/bytesSent"));
    Assert.assertEquals("2326", record.get("/bytesSent").getValueAsString());

    Assert.assertEquals(82, parser.getOffset()); //The log line is 82 characters treating '\"' as a single character
    parser.close();
  }

  @Test(expected = DataParserException.class)
  public void testGetParserReaderLogLineCutShort() throws Exception {
    Map<String, Object> configs = LogCharDataParserFactory.registerConfigs(new HashMap<String, Object>());
    configs.put(LogCharDataParserFactory.RETAIN_ORIGINAL_TEXT_KEY, true);
    CharDataParserFactory factory = new LogCharDataParserFactory(getContext(),
      25 /*Cut short the log line by setting smaller limit*/ , LogMode.COMMON_LOG_FORMAT, configs);
    OverrunReader reader = new OverrunReader(
      new StringReader("127.0.0.1 ss h [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326"), 100,
      true);
    DataParser parser = factory.getParser("id", reader, 0);
    //should get an instance of CommonLogFormatParser
    Assert.assertTrue(parser instanceof CommonLogParser);

    Assert.assertEquals(0, parser.getOffset());
    try {
      parser.parse();
    } finally {
      parser.close();
    }
  }

  @Test
  public void testGetParserReaderWithOffset() throws Exception {
    Map<String, Object> configs = LogCharDataParserFactory.registerConfigs(new HashMap<String, Object>());
    configs.put(LogCharDataParserFactory.RETAIN_ORIGINAL_TEXT_KEY, false);
    CharDataParserFactory factory = new LogCharDataParserFactory(getContext(), 150, LogMode.COMMON_LOG_FORMAT, configs);
    OverrunReader reader = new OverrunReader(new StringReader(
      "Hello\n127.0.0.1 ss h [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326"), 1000, true);

    DataParser parser = factory.getParser("id", reader, 6);
    //should get an instance of CommonLogFormatParser
    Assert.assertTrue(parser instanceof CommonLogParser);

    Assert.assertEquals(6, parser.getOffset());

    Record record = parser.parse();

    Assert.assertFalse(record.has("/originalLine")); //do not retain original line

    Assert.assertTrue(record.has("/remoteHost"));
    Assert.assertEquals("127.0.0.1", record.get("/remoteHost").getValueAsString());

    Assert.assertTrue(record.has("/logName"));
    Assert.assertEquals("ss", record.get("/logName").getValueAsString());

    Assert.assertTrue(record.has("/remoteUser"));
    Assert.assertEquals("h", record.get("/remoteUser").getValueAsString());

    Assert.assertTrue(record.has("/requestTime"));
    Assert.assertEquals("10/Oct/2000:13:55:36 -0700", record.get("/requestTime").getValueAsString());

    Assert.assertTrue(record.has("/requestMethod"));
    Assert.assertEquals("GET", record.get("/requestMethod").getValueAsString());

    Assert.assertTrue(record.has("/urlPath"));
    Assert.assertEquals("/apache_pb.gif", record.get("/urlPath").getValueAsString());

    Assert.assertTrue(record.has("/requestProtocol"));
    Assert.assertEquals("HTTP/1.0", record.get("/requestProtocol").getValueAsString());

    Assert.assertTrue(record.has("/status"));
    Assert.assertEquals("200", record.get("/status").getValueAsString());

    Assert.assertTrue(record.has("/bytesSent"));
    Assert.assertEquals("2326", record.get("/bytesSent").getValueAsString());

    Assert.assertEquals(88, parser.getOffset()); //starts from offset 6 and reads a line 82 characters long
    parser.close();
  }

  @Test
  public void testFactoryCombinedLogFormatParser() throws DataParserException, IOException {
    Map<String, Object> configs = LogCharDataParserFactory.registerConfigs(new HashMap<String, Object>());
    configs.put(LogCharDataParserFactory.RETAIN_ORIGINAL_TEXT_KEY, true);
    CharDataParserFactory factory = new LogCharDataParserFactory(getContext(), 1000, LogMode.COMBINED_LOG_FORMAT,
      configs);
    DataParser parser = factory.getParser("id",
      "127.0.0.1 ss h [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326 " +
        "\"http://www.example.com/start.html\" \"Mozilla/4.08 [en] (Win98; I ;Nav)\"");

    //should get an instance of CommonLogFormatParser
    Assert.assertTrue(parser instanceof CombinedLogParser);

    Assert.assertEquals(0, parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);

    Assert.assertEquals("id::0", record.getHeader().getSourceId());

    Assert.assertEquals("127.0.0.1 ss h [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326 " +
        "\"http://www.example.com/start.html\" \"Mozilla/4.08 [en] (Win98; I ;Nav)\"",
      record.get().getValueAsMap().get("originalLine").getValueAsString());

    Assert.assertFalse(record.has("/truncated"));

    Assert.assertEquals(154, parser.getOffset());

    Assert.assertTrue(record.has("/remoteHost"));
    Assert.assertEquals("127.0.0.1", record.get("/remoteHost").getValueAsString());

    Assert.assertTrue(record.has("/logName"));
    Assert.assertEquals("ss", record.get("/logName").getValueAsString());

    Assert.assertTrue(record.has("/remoteUser"));
    Assert.assertEquals("h", record.get("/remoteUser").getValueAsString());

    Assert.assertTrue(record.has("/requestTime"));
    Assert.assertEquals("10/Oct/2000:13:55:36 -0700", record.get("/requestTime").getValueAsString());

    Assert.assertTrue(record.has("/requestMethod"));
    Assert.assertEquals("GET", record.get("/requestMethod").getValueAsString());

    Assert.assertTrue(record.has("/urlPath"));
    Assert.assertEquals("/apache_pb.gif", record.get("/urlPath").getValueAsString());

    Assert.assertTrue(record.has("/requestProtocol"));
    Assert.assertEquals("HTTP/1.0", record.get("/requestProtocol").getValueAsString());

    Assert.assertTrue(record.has("/status"));
    Assert.assertEquals("200", record.get("/status").getValueAsString());

    Assert.assertTrue(record.has("/bytesSent"));
    Assert.assertEquals("2326", record.get("/bytesSent").getValueAsString());

    Assert.assertTrue(record.has("/referer"));
    Assert.assertEquals("http://www.example.com/start.html", record.get("/referer").getValueAsString());

    Assert.assertTrue(record.has("/userAgent"));
    Assert.assertEquals("Mozilla/4.08 [en] (Win98; I ;Nav)", record.get("/userAgent").getValueAsString());

    parser.close();
  }

  @Test
  public void testFactoryApacheErrorLogFormatParser() throws DataParserException, IOException {
    Map<String, Object> configs = LogCharDataParserFactory.registerConfigs(new HashMap<String, Object>());
    configs.put(LogCharDataParserFactory.RETAIN_ORIGINAL_TEXT_KEY, true);
    CharDataParserFactory factory = new LogCharDataParserFactory(getContext(), 1000, LogMode.APACHE_ERROR_LOG_FORMAT,
      configs);
    DataParser parser = factory.getParser("id",
      "[Wed Oct 11 14:32:52 2000] [error] [client 127.0.0.1] client denied " +
        "by server configuration: /export/home/live/ap/htdocs/test");

    //should get an instance of CommonLogFormatParser
    Assert.assertTrue(parser instanceof ApacheErrorLogParserV22);

    Assert.assertEquals(0, parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);

    Assert.assertEquals("id::0", record.getHeader().getSourceId());

    Assert.assertEquals("[Wed Oct 11 14:32:52 2000] [error] [client 127.0.0.1] client denied " +
      "by server configuration: /export/home/live/ap/htdocs/test",
      record.get().getValueAsMap().get("originalLine").getValueAsString());

    Assert.assertFalse(record.has("/truncated"));

    Assert.assertEquals(125, parser.getOffset());

    Assert.assertTrue(record.has("/dateTime"));
    Assert.assertEquals("Wed Oct 11 14:32:52 2000", record.get("/dateTime").getValueAsString());

    Assert.assertTrue(record.has("/severity"));
    Assert.assertEquals("error", record.get("/severity").getValueAsString());

    Assert.assertTrue(record.has("/clientIpAddress"));
    Assert.assertEquals("127.0.0.1", record.get("/clientIpAddress").getValueAsString());

    Assert.assertTrue(record.has("/message"));
    Assert.assertEquals("client denied by server configuration: /export/home/live/ap/htdocs/test",
      record.get("/message").getValueAsString());

    parser.close();
  }

  @Test
  public void testFactoryApacheCustomFormatParser() throws DataParserException, IOException {

    String logLine = "127.0.0.1 ss h [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326 " +
      "\"http://www.example.com/start.html\" \"Mozilla/4.08 [en] (Win98; I ;Nav)\"";

    Map<String, Object> configs = LogCharDataParserFactory.registerConfigs(new HashMap<String, Object>());
    configs.put(LogCharDataParserFactory.RETAIN_ORIGINAL_TEXT_KEY, true);
    configs.put(LogCharDataParserFactory.APACHE_CUSTOMLOG_FORMAT_KEY,
      "%h %l %u %t \"%m %U %H\" %>s %b \"%{Referer}i\" \"%{User-agent}i\"");
    CharDataParserFactory factory = new LogCharDataParserFactory(getContext(), 1000, LogMode.APACHE_CUSTOM_LOG_FORMAT,
      configs);
    DataParser parser = factory.getParser("id", logLine);

    //should get an instance of CommonLogFormatParser
    Assert.assertTrue(parser instanceof ApacheCustomAccessLogParser);

    Assert.assertEquals(0, parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);

    Assert.assertEquals("id::0", record.getHeader().getSourceId());

    Assert.assertEquals(logLine, record.get().getValueAsMap().get("originalLine").getValueAsString());

    Assert.assertFalse(record.has("/truncated"));

    Assert.assertEquals(154, parser.getOffset());

    Assert.assertTrue(record.has("/remoteHost"));
    Assert.assertEquals("127.0.0.1", record.get("/remoteHost").getValueAsString());

    Assert.assertTrue(record.has("/logName"));
    Assert.assertEquals("ss", record.get("/logName").getValueAsString());

    Assert.assertTrue(record.has("/remoteUser"));
    Assert.assertEquals("h", record.get("/remoteUser").getValueAsString());

    Assert.assertTrue(record.has("/requestTime"));
    Assert.assertEquals("10/Oct/2000:13:55:36 -0700", record.get("/requestTime").getValueAsString());

    Assert.assertTrue(record.has("/requestMethod"));
    Assert.assertEquals("GET", record.get("/requestMethod").getValueAsString());

    Assert.assertTrue(record.has("/urlPath"));
    Assert.assertEquals("/apache_pb.gif", record.get("/urlPath").getValueAsString());

    Assert.assertTrue(record.has("/requestProtocol"));
    Assert.assertEquals("HTTP/1.0", record.get("/requestProtocol").getValueAsString());

    Assert.assertTrue(record.has("/status"));
    Assert.assertEquals("200", record.get("/status").getValueAsString());

    Assert.assertTrue(record.has("/bytesSent"));
    Assert.assertEquals("2326", record.get("/bytesSent").getValueAsString());

    Assert.assertTrue(record.has("/referer"));
    Assert.assertEquals("http://www.example.com/start.html", record.get("/referer").getValueAsString());

    Assert.assertTrue(record.has("/userAgent"));
    Assert.assertEquals("Mozilla/4.08 [en] (Win98; I ;Nav)", record.get("/userAgent").getValueAsString());

    parser.close();
  }

  @Test
  public void testFactoryRegexParser() throws DataParserException, IOException {

    String logLine = "127.0.0.1 ss h [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" " +
      "200 2326 Hello";
    String regex = "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+ \\S+ \\S+)\" (\\d{3}) (\\d+)";

    Map<String, Integer> fieldToGroupMap = new HashMap<>();

    fieldToGroupMap.put("remoteHost", 1);
    fieldToGroupMap.put("logName", 2);
    fieldToGroupMap.put("remoteUser", 3);
    fieldToGroupMap.put("requestTime", 4);
    fieldToGroupMap.put("request", 5);
    fieldToGroupMap.put("status", 6);
    fieldToGroupMap.put("bytesSent", 7);

    Map<String, Object> configs = LogCharDataParserFactory.registerConfigs(new HashMap<String, Object>());
    configs.put(LogCharDataParserFactory.RETAIN_ORIGINAL_TEXT_KEY, true);
    configs.put(LogCharDataParserFactory.REGEX_KEY, regex);
    configs.put(LogCharDataParserFactory.REGEX_FIELD_PATH_TO_GROUP_KEY, fieldToGroupMap);

    CharDataParserFactory factory = new LogCharDataParserFactory(getContext(), 1000, LogMode.REGEX,
      configs);
    DataParser parser = factory.getParser("id", logLine);

    //should get an instance of CommonLogFormatParser
    Assert.assertTrue(parser instanceof RegexParser);

    Assert.assertEquals(0, parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);

    Assert.assertEquals("id::0", record.getHeader().getSourceId());

    Assert.assertEquals(logLine, record.get().getValueAsMap().get("originalLine").getValueAsString());

    Assert.assertFalse(record.has("/truncated"));

    Assert.assertEquals(88, parser.getOffset());

    Assert.assertTrue(record.has("/remoteHost"));
    Assert.assertEquals("127.0.0.1", record.get("/remoteHost").getValueAsString());

    Assert.assertTrue(record.has("/logName"));
    Assert.assertEquals("ss", record.get("/logName").getValueAsString());

    Assert.assertTrue(record.has("/remoteUser"));
    Assert.assertEquals("h", record.get("/remoteUser").getValueAsString());

    Assert.assertTrue(record.has("/requestTime"));
    Assert.assertEquals("10/Oct/2000:13:55:36 -0700", record.get("/requestTime").getValueAsString());

    Assert.assertTrue(record.has("/request"));
    Assert.assertEquals("GET /apache_pb.gif HTTP/1.0", record.get("/request").getValueAsString());

    Assert.assertTrue(record.has("/status"));
    Assert.assertEquals("200", record.get("/status").getValueAsString());

    Assert.assertTrue(record.has("/bytesSent"));
    Assert.assertEquals("2326", record.get("/bytesSent").getValueAsString());

    parser.close();
  }

}
