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
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;

public class TestCommonLogFormatParser {
  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR,
      Collections.<String>emptyList());
  }

  @Test
  public void testParse() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader(
      "127.0.0.1 ss h [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326 Hello"), 1000, true);
    DataParser parser = new CommonLogFormatParser(getContext(), "id", reader, 0, 1000, true);
    Assert.assertEquals(0, parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);

    Assert.assertEquals("id::0", record.getHeader().getSourceId());

    Assert.assertEquals("127.0.0.1 ss h [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326 Hello",
      record.get().getValueAsMap().get("text").getValueAsString());

    Assert.assertFalse(record.has("/truncated"));

    Assert.assertEquals(88, parser.getOffset());

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

    parser.close();
  }

  @Test
  public void testParseWithOffset() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader(
      "Hello\n127.0.0.1 ss h [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326"), 1000, true);
    DataParser parser = new CommonLogFormatParser(getContext(), "id", reader, 6, 1000, true);
    Assert.assertEquals(6, parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);

    Assert.assertEquals("id::6", record.getHeader().getSourceId());

    Assert.assertEquals("127.0.0.1 ss h [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326",
      record.get().getValueAsMap().get("text").getValueAsString());

    Assert.assertFalse(record.has("/truncated"));

    Assert.assertEquals(88, parser.getOffset());

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


    record = parser.parse();
    Assert.assertNull(record);

    Assert.assertEquals(-1, parser.getOffset());
    parser.close();
  }

  @Test(expected = IOException.class)
  public void testClose() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("Hello\nByte"), 1000, true);
    DataParser parser = new CommonLogFormatParser(getContext(), "id", reader, 0, 1000, false);
    parser.close();
    parser.parse();
  }

  @Test
  public void testTruncate() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader(
      "127.0.0.1 ss h [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326"), 1000, true);
    DataParser parser = new CommonLogFormatParser(getContext(), "id", reader, 0, 25, true); //cut short to 25
    Assert.assertEquals(0, parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::0", record.getHeader().getSourceId());

    Assert.assertTrue(record.has("/text"));
    Assert.assertTrue(record.has("/truncated"));

    Assert.assertFalse(record.has("/ipAddress"));
    Assert.assertFalse(record.has("/clientId"));
    Assert.assertFalse(record.has("/userId"));
    Assert.assertFalse(record.has("/dateTime"));
    Assert.assertFalse(record.has("/method"));
    Assert.assertFalse(record.has("/request"));
    Assert.assertFalse(record.has("/protocol"));
    Assert.assertFalse(record.has("/responseCode"));
    Assert.assertFalse(record.has("/size"));
    Assert.assertEquals(82, parser.getOffset());

    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals(-1, parser.getOffset());
    parser.close();
  }

  @Test
  public void testParseNonLogLine() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader(
      "127.0.0.1 ss h [10/Oct/2000:13:55:36 -0700] This is a log line that does not confirm to common log format"),
      1000, true);
    DataParser parser = new CommonLogFormatParser(getContext(), "id", reader, 0, 1000, true);
    Assert.assertEquals(0, parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);

    Assert.assertEquals("id::0", record.getHeader().getSourceId());

    Assert.assertEquals(
      "127.0.0.1 ss h [10/Oct/2000:13:55:36 -0700] This is a log line that does not confirm to common log format",
      record.get().getValueAsMap().get("text").getValueAsString());

    Assert.assertFalse(record.has("/truncated"));

    Assert.assertEquals(105, parser.getOffset());

    Assert.assertFalse(record.has("/ipAddress"));
    Assert.assertFalse(record.has("/clientId"));
    Assert.assertFalse(record.has("/userId"));
    Assert.assertFalse(record.has("/dateTime"));
    Assert.assertFalse(record.has("/method"));
    Assert.assertFalse(record.has("/request"));
    Assert.assertFalse(record.has("/protocol"));
    Assert.assertFalse(record.has("/responseCode"));
    Assert.assertFalse(record.has("/size"));

    parser.close();
  }
}
