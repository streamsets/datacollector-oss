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
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.junit.Assert;
import org.junit.Test;

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
    Assert.assertTrue(parser instanceof CommonLogFormatParser);

    Assert.assertEquals(0, parser.getOffset());
    Record record = parser.parse();
    Assert.assertTrue(record.has("/text"));

    Assert.assertTrue(record.has("/ipAddress"));
    Assert.assertEquals("127.0.0.1", record.get("/ipAddress").getValueAsString());

    Assert.assertTrue(record.has("/clientId"));
    Assert.assertEquals("ss", record.get("/clientId").getValueAsString());

    Assert.assertTrue(record.has("/userId"));
    Assert.assertEquals("h", record.get("/userId").getValueAsString());

    Assert.assertTrue(record.has("/dateTime"));
    Assert.assertEquals("10/Oct/2000:13:55:36 -0700", record.get("/dateTime").getValueAsString());

    Assert.assertTrue(record.has("/method"));
    Assert.assertEquals("GET", record.get("/method").getValueAsString());

    Assert.assertTrue(record.has("/request"));
    Assert.assertEquals("/apache_pb.gif", record.get("/request").getValueAsString());

    Assert.assertTrue(record.has("/protocol"));
    Assert.assertEquals("HTTP/1.0", record.get("/protocol").getValueAsString());

    Assert.assertTrue(record.has("/responseCode"));
    Assert.assertEquals("200", record.get("/responseCode").getValueAsString());

    Assert.assertTrue(record.has("/size"));
    Assert.assertEquals("2326", record.get("/size").getValueAsString());

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
    Assert.assertTrue(parser instanceof CommonLogFormatParser);

    Assert.assertEquals(0, parser.getOffset());
    Record record = parser.parse();
    Assert.assertFalse(record.has("/text"));

    Assert.assertTrue(record.has("/ipAddress"));
    Assert.assertEquals("127.0.0.1", record.get("/ipAddress").getValueAsString());

    Assert.assertTrue(record.has("/clientId"));
    Assert.assertEquals("ss", record.get("/clientId").getValueAsString());

    Assert.assertTrue(record.has("/userId"));
    Assert.assertEquals("h", record.get("/userId").getValueAsString());

    Assert.assertTrue(record.has("/dateTime"));
    Assert.assertEquals("10/Oct/2000:13:55:36 -0700", record.get("/dateTime").getValueAsString());

    Assert.assertTrue(record.has("/method"));
    Assert.assertEquals("GET", record.get("/method").getValueAsString());

    Assert.assertTrue(record.has("/request"));
    Assert.assertEquals("/apache_pb.gif", record.get("/request").getValueAsString());

    Assert.assertTrue(record.has("/protocol"));
    Assert.assertEquals("HTTP/1.0", record.get("/protocol").getValueAsString());

    Assert.assertTrue(record.has("/responseCode"));
    Assert.assertEquals("200", record.get("/responseCode").getValueAsString());

    Assert.assertTrue(record.has("/size"));
    Assert.assertEquals("2326", record.get("/size").getValueAsString());
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
    Assert.assertTrue(parser instanceof CommonLogFormatParser);

    Assert.assertEquals(0, parser.getOffset());
    Record record = parser.parse();
    Assert.assertTrue(record.has("/text"));

    Assert.assertTrue(record.has("/ipAddress"));
    Assert.assertEquals("127.0.0.1", record.get("/ipAddress").getValueAsString());

    Assert.assertTrue(record.has("/clientId"));
    Assert.assertEquals("ss", record.get("/clientId").getValueAsString());

    Assert.assertTrue(record.has("/userId"));
    Assert.assertEquals("h", record.get("/userId").getValueAsString());

    Assert.assertTrue(record.has("/dateTime"));
    Assert.assertEquals("10/Oct/2000:13:55:36 -0700", record.get("/dateTime").getValueAsString());

    Assert.assertTrue(record.has("/method"));
    Assert.assertEquals("GET", record.get("/method").getValueAsString());

    Assert.assertTrue(record.has("/request"));
    Assert.assertEquals("/apache_pb.gif", record.get("/request").getValueAsString());

    Assert.assertTrue(record.has("/protocol"));
    Assert.assertEquals("HTTP/1.0", record.get("/protocol").getValueAsString());

    Assert.assertTrue(record.has("/responseCode"));
    Assert.assertEquals("200", record.get("/responseCode").getValueAsString());

    Assert.assertTrue(record.has("/size"));
    Assert.assertEquals("2326", record.get("/size").getValueAsString());

    Assert.assertEquals(82, parser.getOffset()); //The log line is 82 characters treating '\"' as a single character
    parser.close();
  }

  @Test
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
    Assert.assertTrue(parser instanceof CommonLogFormatParser);

    Assert.assertEquals(0, parser.getOffset());
    Record record = parser.parse();
    Assert.assertTrue(record.has("/text"));
    //log line is truncated
    Assert.assertTrue(record.has("/truncated"));

    //truncated log line does not match the Common Log Format pattern
    Assert.assertFalse(record.has("/ipAddress"));
    Assert.assertFalse(record.has("/clientId"));
    Assert.assertFalse(record.has("/userId"));
    Assert.assertFalse(record.has("/dateTime"));
    Assert.assertFalse(record.has("/method"));
    Assert.assertFalse(record.has("/request"));
    Assert.assertFalse(record.has("/protocol"));
    Assert.assertFalse(record.has("/responseCode"));
    Assert.assertFalse(record.has("/size"));
    Assert.assertEquals(82, parser.getOffset()); //The log line is 82 characters treating '\"' as a single character
    parser.close();
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
    Assert.assertTrue(parser instanceof CommonLogFormatParser);

    Assert.assertEquals(6, parser.getOffset());

    Record record = parser.parse();

    Assert.assertFalse(record.has("/text")); //do not retain original line

    Assert.assertTrue(record.has("/ipAddress"));
    Assert.assertEquals("127.0.0.1", record.get("/ipAddress").getValueAsString());

    Assert.assertTrue(record.has("/clientId"));
    Assert.assertEquals("ss", record.get("/clientId").getValueAsString());

    Assert.assertTrue(record.has("/userId"));
    Assert.assertEquals("h", record.get("/userId").getValueAsString());

    Assert.assertTrue(record.has("/dateTime"));
    Assert.assertEquals("10/Oct/2000:13:55:36 -0700", record.get("/dateTime").getValueAsString());

    Assert.assertTrue(record.has("/method"));
    Assert.assertEquals("GET", record.get("/method").getValueAsString());

    Assert.assertTrue(record.has("/request"));
    Assert.assertEquals("/apache_pb.gif", record.get("/request").getValueAsString());

    Assert.assertTrue(record.has("/protocol"));
    Assert.assertEquals("HTTP/1.0", record.get("/protocol").getValueAsString());

    Assert.assertTrue(record.has("/responseCode"));
    Assert.assertEquals("200", record.get("/responseCode").getValueAsString());

    Assert.assertTrue(record.has("/size"));
    Assert.assertEquals("2326", record.get("/size").getValueAsString());

    Assert.assertEquals(88, parser.getOffset()); //starts from offset 6 and reads a line 82 characters long
    parser.close();
  }
}
