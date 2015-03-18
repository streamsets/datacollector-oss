/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.log;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.io.OverrunReader;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;

public class TestErrorLogFormatParser {

  private static final String ERROR_LOG_LINE = "[Wed Oct 11 14:32:52 2000] [error] [client 127.0.0.1] client denied " +
    "by server configuration: /export/home/live/ap/htdocs/test";

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR,
      Collections.<String>emptyList());
  }

  @Test
  public void testParse() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader(ERROR_LOG_LINE), 1000, true);
    DataParser parser = new ApacheErrorLogParserV22(getContext(), "id", reader, 0, 1000, true);
    Assert.assertEquals(0, parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);

    Assert.assertEquals("id::0", record.getHeader().getSourceId());

    Assert.assertEquals(ERROR_LOG_LINE, record.get().getValueAsMap().get("originalLine").getValueAsString());

    Assert.assertFalse(record.has("/truncated"));

    Assert.assertEquals(125, parser.getOffset());

    Assert.assertTrue(record.has("/" + ApacheErrorLogParserV22.DATE_TIME));
    Assert.assertEquals("Wed Oct 11 14:32:52 2000", record.get("/" + ApacheErrorLogParserV22.DATE_TIME).getValueAsString());

    Assert.assertTrue(record.has("/" + ApacheErrorLogParserV22.SEVERITY));
    Assert.assertEquals("error", record.get("/" + ApacheErrorLogParserV22.SEVERITY).getValueAsString());

    Assert.assertTrue(record.has("/" + ApacheErrorLogParserV22.CLIENT_IP_ADDRESS));
    Assert.assertEquals("127.0.0.1", record.get("/" + ApacheErrorLogParserV22.CLIENT_IP_ADDRESS).getValueAsString());

    Assert.assertTrue(record.has("/" + ApacheErrorLogParserV22.MESSAGE));
    Assert.assertEquals("client denied by server configuration: /export/home/live/ap/htdocs/test",
      record.get("/" + ApacheErrorLogParserV22.MESSAGE).getValueAsString());

    parser.close();
  }

  @Test
  public void testParseWithOffset() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader(
      "Hello\n" + ERROR_LOG_LINE), 1000, true);
    DataParser parser = new ApacheErrorLogParserV22(getContext(), "id", reader, 6, 1000, true);
    Assert.assertEquals(6, parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);

    Assert.assertEquals("id::6", record.getHeader().getSourceId());

    Assert.assertEquals(ERROR_LOG_LINE, record.get().getValueAsMap().get("originalLine").getValueAsString());

    Assert.assertFalse(record.has("/truncated"));

    Assert.assertEquals(131, parser.getOffset());

    Assert.assertTrue(record.has("/" + ApacheErrorLogParserV22.DATE_TIME));
    Assert.assertEquals("Wed Oct 11 14:32:52 2000", record.get("/" + ApacheErrorLogParserV22.DATE_TIME).getValueAsString());

    Assert.assertTrue(record.has("/" + ApacheErrorLogParserV22.SEVERITY));
    Assert.assertEquals("error", record.get("/" + ApacheErrorLogParserV22.SEVERITY).getValueAsString());

    Assert.assertTrue(record.has("/" + ApacheErrorLogParserV22.CLIENT_IP_ADDRESS));
    Assert.assertEquals("127.0.0.1", record.get("/" + ApacheErrorLogParserV22.CLIENT_IP_ADDRESS).getValueAsString());

    Assert.assertTrue(record.has("/" + ApacheErrorLogParserV22.MESSAGE));
    Assert.assertEquals("client denied by server configuration: /export/home/live/ap/htdocs/test",
      record.get("/" + ApacheErrorLogParserV22.MESSAGE).getValueAsString());

    record = parser.parse();
    Assert.assertNull(record);

    Assert.assertEquals(-1, parser.getOffset());
    parser.close();
  }

  @Test(expected = IOException.class)
  public void testClose() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("Hello\nByte"), 1000, true);
    DataParser parser = new ApacheErrorLogParserV22(getContext(), "id", reader, 0, 1000, false);
    parser.close();
    parser.parse();
  }

  @Test(expected = DataParserException.class)
  public void testTruncate() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader(ERROR_LOG_LINE), 1000, true);
    DataParser parser = new ApacheErrorLogParserV22(getContext(), "id", reader, 0, 25, true); //cut short to 25
    Assert.assertEquals(0, parser.getOffset());
    try {
      parser.parse();
    } finally {
      parser.close();
    }
  }

  @Test(expected = DataParserException.class)
  public void testParseNonLogLine() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader(
      "127.0.0.1 ss h [10/Oct/2000:13:55:36 -0700] This is a log line that does not confirm to common log format"),
      1000, true);
    DataParser parser = new ApacheErrorLogParserV22(getContext(), "id", reader, 0, 1000, true);
    Assert.assertEquals(0, parser.getOffset());
    try {
      parser.parse();
    } finally {
      parser.close();
    }
  }
}
