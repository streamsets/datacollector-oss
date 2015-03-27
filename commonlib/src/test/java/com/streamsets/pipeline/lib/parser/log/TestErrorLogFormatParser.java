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

public class TestErrorLogFormatParser {

  private static final String ERROR_LOG_LINE = "[Wed Oct 11 14:32:52 2000] [error] [client 127.0.0.1] client denied " +
    "by server configuration: /export/home/live/ap/htdocs/test";

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR,
      Collections.<String>emptyList());
  }

  @Test
  public void testParse() throws Exception {
    DataParser parser = getDataParser(ERROR_LOG_LINE, 1000, 0);
    Assert.assertEquals(0, parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);

    Assert.assertEquals("id::0", record.getHeader().getSourceId());

    Assert.assertEquals(ERROR_LOG_LINE, record.get().getValueAsMap().get("originalLine").getValueAsString());

    Assert.assertFalse(record.has("/truncated"));

    Assert.assertEquals(125, parser.getOffset());

    Assert.assertTrue(record.has("/" + Constants.TIMESTAMP));
    Assert.assertEquals("Wed Oct 11 14:32:52 2000", record.get("/" + Constants.TIMESTAMP).getValueAsString());

    Assert.assertTrue(record.has("/" + Constants.LOGLEVEL));
    Assert.assertEquals("error", record.get("/" + Constants.LOGLEVEL).getValueAsString());

    Assert.assertTrue(record.has("/" + Constants.CLIENTIP));
    Assert.assertEquals("127.0.0.1", record.get("/" + Constants.CLIENTIP).getValueAsString());

    Assert.assertTrue(record.has("/" + Constants.MESSAGE));
    Assert.assertEquals("client denied by server configuration: /export/home/live/ap/htdocs/test",
      record.get("/" + Constants.MESSAGE).getValueAsString());

    parser.close();
  }

  @Test
  public void testParseWithOffset() throws Exception {
    DataParser parser = getDataParser(
      "Hello\n" + ERROR_LOG_LINE, 1000, 6);
    Assert.assertEquals(6, parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);

    Assert.assertEquals("id::6", record.getHeader().getSourceId());

    Assert.assertEquals(ERROR_LOG_LINE, record.get().getValueAsMap().get("originalLine").getValueAsString());

    Assert.assertFalse(record.has("/truncated"));

    Assert.assertEquals(131, parser.getOffset());

    Assert.assertTrue(record.has("/" + Constants.TIMESTAMP));
    Assert.assertEquals("Wed Oct 11 14:32:52 2000", record.get("/" + Constants.TIMESTAMP).getValueAsString());

    Assert.assertTrue(record.has("/" + Constants.LOGLEVEL));
    Assert.assertEquals("error", record.get("/" + Constants.LOGLEVEL).getValueAsString());

    Assert.assertTrue(record.has("/" + Constants.CLIENTIP));
    Assert.assertEquals("127.0.0.1", record.get("/" + Constants.CLIENTIP).getValueAsString());

    Assert.assertTrue(record.has("/" + Constants.MESSAGE));
    Assert.assertEquals("client denied by server configuration: /export/home/live/ap/htdocs/test",
      record.get("/" + Constants.MESSAGE).getValueAsString());

    record = parser.parse();
    Assert.assertNull(record);

    Assert.assertEquals(-1, parser.getOffset());
    parser.close();
  }

  @Test(expected = IOException.class)
  public void testClose() throws Exception {
    DataParser parser = getDataParser("Hello\nByte", 1000, 0);
    parser.close();
    parser.parse();
  }

  @Test(expected = DataParserException.class)
  public void testTruncate() throws Exception {
    DataParser parser = getDataParser(ERROR_LOG_LINE, 25, 0);
    Assert.assertEquals(0, parser.getOffset());
    try {
      parser.parse();
    } finally {
      parser.close();
    }
  }

  @Test(expected = DataParserException.class)
  public void testParseNonLogLine() throws Exception {
    DataParser parser = getDataParser(
      "127.0.0.1 ss h [10/Oct/2000:13:55:36 -0700] This is a log line that does not confirm to common log format",
      1000, 0);
    Assert.assertEquals(0, parser.getOffset());
    try {
      parser.parse();
    } finally {
      parser.close();
    }
  }

  private DataParser getDataParser(String logLine, int maxObjectLength, int readerOffset) throws DataParserException {
    OverrunReader reader = new OverrunReader(new StringReader(logLine), 1000, true);
    Map<String, Object> configs = LogCharDataParserFactory.registerConfigs(new HashMap<String, Object>());
    configs.put(LogCharDataParserFactory.RETAIN_ORIGINAL_TEXT_KEY, true);
    CharDataParserFactory factory = new LogCharDataParserFactory(getContext(), maxObjectLength,
      LogMode.APACHE_ERROR_LOG_FORMAT,
      configs);
    return factory.getParser("id", reader, readerOffset);
  }
}
