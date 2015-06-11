/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.text;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.io.OverrunException;
import com.streamsets.pipeline.lib.io.OverrunReader;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;

public class TestTextCharDataParser {

  @SuppressWarnings("unchecked")
  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR, Collections.EMPTY_LIST);
  }

  @Test
  public void testParse() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("Hello\nBye"), 1000, true, false);
    DataParser parser = new TextCharDataParser(getContext(), "id", reader, 0, 1000, "text", "truncated");
    Assert.assertEquals(0, parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::0", record.getHeader().getSourceId());
    Assert.assertEquals("Hello", record.get().getValueAsMap().get("text").getValueAsString());
    Assert.assertFalse(record.has("/truncated"));
    Assert.assertEquals(6, parser.getOffset());
    record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::6", record.getHeader().getSourceId());
    Assert.assertEquals("Bye", record.get().getValueAsMap().get("text").getValueAsString());
    Assert.assertFalse(record.has("/truncated"));
    Assert.assertEquals(9, parser.getOffset());
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals(-1, parser.getOffset());
    parser.close();
  }

  @Test
  public void testParseWithOffset() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("Hello\nBye"), 1000, true, false);
    DataParser parser = new TextCharDataParser(getContext(), "id", reader, 6, 1000, "text", "truncated");
    Assert.assertEquals(6, parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::6", record.getHeader().getSourceId());
    Assert.assertEquals("Bye", record.get().getValueAsMap().get("text").getValueAsString());
    Assert.assertFalse(record.has("/truncated"));
    Assert.assertEquals(9, parser.getOffset());
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals(-1, parser.getOffset());
    parser.close();
  }

  @Test(expected = IOException.class)
  public void testClose() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("Hello\nByte"), 1000, true, false);
    DataParser parser = new TextCharDataParser(getContext(), "id", reader, 0, 1000, "text", "truncated");
    parser.close();
    parser.parse();
  }

  @Test
  public void testTruncate() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("Hello\nBye"), 1000, true, false);
    DataParser parser = new TextCharDataParser(getContext(), "id", reader, 0, 3, "text", "truncated");
    Assert.assertEquals(0, parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::0", record.getHeader().getSourceId());
    Assert.assertEquals("Hel", record.get().getValueAsMap().get("text").getValueAsString());
    Assert.assertTrue(record.has("/truncated"));
    Assert.assertEquals(6, parser.getOffset());
    record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::6", record.getHeader().getSourceId());
    Assert.assertEquals("Bye", record.get().getValueAsMap().get("text").getValueAsString());
    Assert.assertFalse(record.has("/truncated"));
    Assert.assertEquals(9, parser.getOffset());
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals(-1, parser.getOffset());
    parser.close();
  }

  private String createTextLines(int underLimitLength, int underLimitLines, int overLimitLength) {
    StringBuilder sb = new StringBuilder(underLimitLength * underLimitLength + overLimitLength + underLimitLines + 1);
    for (int line = 0; line < underLimitLines; line++) {
      for (int len = 0; len < underLimitLength; len++) {
        sb.append((char) (len % 28 + 65));
      }
      sb.append('\n');
    }
    for (int len = 0; len < overLimitLength; len++) {
      sb.append((char) (len % 28 + 65));
    }
    sb.append('\n');
    return sb.toString();
  }

  @Test(expected = OverrunException.class)
  public void testOverrun() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader(createTextLines(1000, 20, 5000)), 2 * 1000, true, false);
    int lines = 0;
    try (DataParser parser = new TextCharDataParser(getContext(), "id", reader, 0, 3, "text", "truncated")) {
      // we read 20 lines under the limit then one over the limit
      while (parser.parse() != null) {
        lines++;
      }
    } finally {
      Assert.assertEquals(20, lines);
    }
  }

}
