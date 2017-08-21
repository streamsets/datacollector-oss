/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.lib.parser.text;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ext.io.OverrunException;
import com.streamsets.pipeline.api.ext.io.OverrunReader;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.StringBuilderPoolFactory;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
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
    DataParser parser = new TextCharDataParser(
        getContext(),
        "id",
        false,
        false,
        "",
        false,
        reader,
        0,
        1000,
        "text",
        "truncated",
        getStringBuilderPool()
    );
    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::0", record.getHeader().getSourceId());
    Assert.assertEquals("Hello", record.get().getValueAsMap().get("text").getValueAsString());
    Assert.assertFalse(record.has("/truncated"));
    Assert.assertEquals(6, Long.parseLong(parser.getOffset()));
    record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::6", record.getHeader().getSourceId());
    Assert.assertEquals("Bye", record.get().getValueAsMap().get("text").getValueAsString());
    Assert.assertFalse(record.has("/truncated"));
    Assert.assertEquals(9, Long.parseLong(parser.getOffset()));
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals(-1, Long.parseLong(parser.getOffset()));
    parser.close();
  }

  @Test
  public void testParseWithOffset() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("Hello\nBye"), 1000, true, false);
    DataParser parser = new TextCharDataParser(
        getContext(),
        "id",
        false,
        false,
        "",
        false,
        reader,
        6,
        1000,
        "text",
        "truncated",
        getStringBuilderPool()
    );
    Assert.assertEquals(6, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::6", record.getHeader().getSourceId());
    Assert.assertEquals("Bye", record.get().getValueAsMap().get("text").getValueAsString());
    Assert.assertFalse(record.has("/truncated"));
    Assert.assertEquals(9, Long.parseLong(parser.getOffset()));
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals(-1, Long.parseLong(parser.getOffset()));
    parser.close();
  }

  @Test(expected = IOException.class)
  public void testClose() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("Hello\nByte"), 1000, true, false);
    DataParser parser = new TextCharDataParser(
        getContext(),
        "id",
        false,
        false,
        "",
        false,
        reader,
        0,
        1000,
        "text",
        "truncated",
        getStringBuilderPool()
    );
    parser.close();
    parser.parse();
  }

  @Test
  public void testTruncate() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("Hello\nBye"), 1000, true, false);
    DataParser parser = new TextCharDataParser(
        getContext(),
        "id",
        false,
        false,
        "",
        false,
        reader,
        0,
        3,
        "text",
        "truncated",
        getStringBuilderPool()
    );
    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::0", record.getHeader().getSourceId());
    Assert.assertEquals("Hel", record.get().getValueAsMap().get("text").getValueAsString());
    Assert.assertTrue(record.has("/truncated"));
    Assert.assertEquals(6, Long.parseLong(parser.getOffset()));
    record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::6", record.getHeader().getSourceId());
    Assert.assertEquals("Bye", record.get().getValueAsMap().get("text").getValueAsString());
    Assert.assertFalse(record.has("/truncated"));
    Assert.assertEquals(9, Long.parseLong(parser.getOffset()));
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals(-1, Long.parseLong(parser.getOffset()));
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
    try (DataParser parser = new TextCharDataParser(
        getContext(),
        "id",
        false,
        false,
        "",
        false,
        reader,
        0,
        3,
        "text",
        "truncated",
        getStringBuilderPool()
    )) {
      // we read 20 lines under the limit then one over the limit
      while (parser.parse() != null) {
        lines++;
      }
    } finally {
      Assert.assertEquals(20, lines);
    }
  }

  @Test
  public void testCollapseAllDefault() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("Hello\nBye"), 1000, true, false);
    DataParser parser = new TextCharDataParser(
        getContext(),
        "id",
        true,
        false,
        "",
        false,
        reader,
        0,
        100,
        "text",
        "truncated",
        getStringBuilderPool()
    );
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("Hello\nBye\n", record.get().getValueAsMap().get("text").getValueAsString());
    record = parser.parse();
    Assert.assertNull(record);
    parser.close();
  }


  @Test
  public void testCollapseAllWithCustomDelimiter() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("Hello\nBye"), 1000, true, false);
    DataParser parser = new TextCharDataParser(
        getContext(),
        "id",
        true,
        true,
        "\r\n",
        false,
        reader,
        0,
        100,
        "text",
        "truncated",
        getStringBuilderPool()
    );
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("Hello\nBye\n", record.get().getValueAsMap().get("text").getValueAsString());
    record = parser.parse();
    Assert.assertNull(record);
    parser.close();
  }

  @Test
  public void testCustomDelimiterForJson() throws Exception {
    String record1 = "{\"menu\": {\n" +
        "  \"id\": \"file\",\n" +
        "  \"value\": \"Record1\",\n" +
        "  \"popup\": {\n" +
        "    \"menuitem\": [\n" +
        "      {\"value\": \"New\", \"onclick\": \"CreateNewDoc()\"},\n" +
        "      {\"value\": \"Open\", \"onclick\": \"OpenDoc()\"},\n" +
        "      {\"value\": \"Close\", \"onclick\": \"CloseDoc()\"}]}}}";
    String record2 = "{\"menu\": {\n" +
        "  \"id\": \"file\",\n" +
        "  \"value\": \"Record2\",\n" +
        "  \"popup\": {\n" +
        "    \"menuitem\": [\n" +
        "      {\"value\": \"New\", \"onclick\": \"CreateNewDoc()\"},\n" +
        "      {\"value\": \"Open\", \"onclick\": \"OpenDoc()\"},\n" +
        "      {\"value\": \"Close\", \"onclick\": \"CloseDoc()\"}]}}}";
    OverrunReader reader = new OverrunReader(new StringReader(record1 + "\n" +record2 + "\n"), 1000, true, false);
    DataParser parser = new TextCharDataParser(
        getContext(),
        "id",
        false,
        true,
        "}]}}}\n",
        true,
        reader, 0, 10000, "text", "truncated", getStringBuilderPool());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals(record1 + "\n", record.get().getValueAsMap().get("text").getValueAsString());
    record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals(record2 + "\n", record.get().getValueAsMap().get("text").getValueAsString());
    record = parser.parse();
    Assert.assertNull(record);
    parser.close();
  }

  private GenericObjectPool<StringBuilder> getStringBuilderPool() {
    GenericObjectPoolConfig stringBuilderPoolConfig = new GenericObjectPoolConfig();
    stringBuilderPoolConfig.setMaxTotal(1);
    stringBuilderPoolConfig.setMinIdle(1);
    stringBuilderPoolConfig.setMaxIdle(1);
    stringBuilderPoolConfig.setBlockWhenExhausted(false);
    return new GenericObjectPool<>(new StringBuilderPoolFactory(1024), stringBuilderPoolConfig);
  }

}
