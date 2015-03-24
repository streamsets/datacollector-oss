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

public class TestGrokParser {

  private static final String LOG_LINE = "[3223] 26 Feb 23:59:01 Background append only file rewriting started by pid " +
    "19383 [19383] 26 Feb 23:59:01 SYNC append only file rewrite performed ";

  private static final String REGEX_DEFINITION =
    "REDISTIMESTAMP %{MONTHDAY} %{MONTH} %{TIME}\n" +
    "REDISLOG \\[%{POSINT:pid}\\] %{REDISTIMESTAMP:timestamp} .*";

  private static final String REGEX = "%{REDISLOG}";

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR,
      Collections.<String>emptyList());
  }

  @Test
  public void testParse() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader(LOG_LINE), 1000, true);
    DataParser parser = new GrokParser(getContext(), "id", reader, 0, 1000, true, REGEX_DEFINITION, REGEX,
      Collections.<String> emptyList());
    Assert.assertEquals(0, parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);

    Assert.assertEquals("id::0", record.getHeader().getSourceId());

    Assert.assertEquals(LOG_LINE, record.get().getValueAsMap().get("originalLine").getValueAsString());

    Assert.assertFalse(record.has("/truncated"));

    Assert.assertEquals(146, parser.getOffset());

    Assert.assertTrue(record.has("/timestamp"));
    Assert.assertEquals("26 Feb 23:59:01", record.get("/timestamp").getValueAsString());

    Assert.assertTrue(record.has("/pid"));
    Assert.assertEquals("3223", record.get("/pid").getValueAsString());

    parser.close();
  }

  @Test
  public void testParseWithOffset() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader(
      "Hello\n" + LOG_LINE), 1000, true);
    DataParser parser = new GrokParser(getContext(), "id", reader, 6, 1000, true, REGEX_DEFINITION, REGEX,
      Collections.<String> emptyList());
    Assert.assertEquals(6, parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);

    Assert.assertEquals("id::6", record.getHeader().getSourceId());

    Assert.assertEquals(LOG_LINE, record.get().getValueAsMap().get("originalLine").getValueAsString());

    Assert.assertFalse(record.has("/truncated"));

    Assert.assertEquals(152, parser.getOffset());

    Assert.assertTrue(record.has("/timestamp"));
    Assert.assertEquals("26 Feb 23:59:01", record.get("/timestamp").getValueAsString());

    Assert.assertTrue(record.has("/pid"));
    Assert.assertEquals("3223", record.get("/pid").getValueAsString());

    record = parser.parse();
    Assert.assertNull(record);

    Assert.assertEquals(-1, parser.getOffset());
    parser.close();
  }

  @Test(expected = IOException.class)
  public void testClose() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("Hello\nByte"), 1000, true);
    DataParser parser = new GrokParser(getContext(), "id", reader, 0, 1000, false, REGEX_DEFINITION, REGEX,
      Collections.<String> emptyList());
    parser.close();
    parser.parse();
  }

  @Test(expected = DataParserException.class)
  public void testTruncate() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader(
      LOG_LINE), 1000, true);
    DataParser parser = new GrokParser(getContext(), "id", reader, 0, 7, true, REGEX_DEFINITION, REGEX,
      Collections.<String> emptyList()); //cut short to 7
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
    DataParser parser = new GrokParser(getContext(), "id", reader, 0, 1000, true, REGEX_DEFINITION, REGEX,
      Collections.<String> emptyList());
    Assert.assertEquals(0, parser.getOffset());
    try {
      parser.parse();
    } finally {
      parser.close();
    }
  }
}
