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
package com.streamsets.pipeline.lib.parser.log;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.config.LogMode;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.DataParserFormat;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;

public class TestCommonLogFormatParser {
  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR,
      Collections.<String>emptyList());
  }

  @Test
  public void testParse() throws Exception {
    DataParser parser = getDataParser(
      "127.0.0.1 ss h [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326 Hello", 1000, 0);

    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertNotNull(record);

    Assert.assertEquals("id::0", record.getHeader().getSourceId());

    Assert.assertEquals("127.0.0.1 ss h [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326 Hello",
      record.get().getValueAsMap().get("originalLine").getValueAsString());

    Assert.assertFalse(record.has("/truncated"));

    //offset is -1 as the parser attempted a read ahead and met the end
    Assert.assertEquals(88, Long.parseLong(parser.getOffset()));

    Assert.assertTrue(record.has("/" + Constants.CLIENTIP));
    Assert.assertEquals("127.0.0.1", record.get("/" + Constants.CLIENTIP).getValueAsString());

    Assert.assertTrue(record.has("/" + Constants.USER_IDENT));
    Assert.assertEquals("ss", record.get("/" + Constants.USER_IDENT).getValueAsString());

    Assert.assertTrue(record.has("/" + Constants.USER_AUTH));
    Assert.assertEquals("h", record.get("/" + Constants.USER_AUTH).getValueAsString());

    Assert.assertTrue(record.has("/" + Constants.TIMESTAMP));
    Assert.assertEquals("10/Oct/2000:13:55:36 -0700", record.get("/" + Constants.TIMESTAMP).getValueAsString());

    Assert.assertTrue(record.has("/" + Constants.VERB));
    Assert.assertEquals("GET", record.get("/" + Constants.VERB).getValueAsString());

    Assert.assertTrue(record.has("/" + Constants.REQUEST));
    Assert.assertEquals("/apache_pb.gif", record.get("/" + Constants.REQUEST).getValueAsString());

    Assert.assertTrue(record.has("/" + Constants.HTTPVERSION));
    Assert.assertEquals("1.0", record.get("/" + Constants.HTTPVERSION).getValueAsString());

    Assert.assertTrue(record.has("/" + Constants.RESPONSE));
    Assert.assertEquals("200", record.get("/" + Constants.RESPONSE).getValueAsString());

    Assert.assertTrue(record.has("/" + Constants.BYTES));
    Assert.assertEquals("2326", record.get("/" + Constants.BYTES).getValueAsString());

    parser.close();
  }

  @Test
  public void testParseWithOffset() throws Exception {
    DataParser parser = getDataParser(
      "Hello\n127.0.0.1 ss h [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326", 1000, 6);
    Assert.assertEquals(6, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertNotNull(record);

    Assert.assertEquals("id::6", record.getHeader().getSourceId());

    Assert.assertEquals("127.0.0.1 ss h [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326",
      record.get().getValueAsMap().get("originalLine").getValueAsString());

    Assert.assertFalse(record.has("/truncated"));

    //offset is -1 as the parser attempted a read ahead and met the end
    Assert.assertEquals(88, Long.parseLong(parser.getOffset()));

    Assert.assertTrue(record.has("/" + Constants.CLIENTIP));
    Assert.assertEquals("127.0.0.1", record.get("/" + Constants.CLIENTIP).getValueAsString());

    Assert.assertTrue(record.has("/" + Constants.USER_IDENT));
    Assert.assertEquals("ss", record.get("/" + Constants.USER_IDENT).getValueAsString());

    Assert.assertTrue(record.has("/" + Constants.USER_AUTH));
    Assert.assertEquals("h", record.get("/" + Constants.USER_AUTH).getValueAsString());

    Assert.assertTrue(record.has("/" + Constants.TIMESTAMP));
    Assert.assertEquals("10/Oct/2000:13:55:36 -0700", record.get("/" + Constants.TIMESTAMP).getValueAsString());

    Assert.assertTrue(record.has("/" + Constants.VERB));
    Assert.assertEquals("GET", record.get("/" + Constants.VERB).getValueAsString());

    Assert.assertTrue(record.has("/" + Constants.REQUEST));
    Assert.assertEquals("/apache_pb.gif", record.get("/" + Constants.REQUEST).getValueAsString());

    Assert.assertTrue(record.has("/" + Constants.HTTPVERSION));
    Assert.assertEquals("1.0", record.get("/" + Constants.HTTPVERSION).getValueAsString());

    Assert.assertTrue(record.has("/" + Constants.RESPONSE));
    Assert.assertEquals("200", record.get("/" + Constants.RESPONSE).getValueAsString());

    Assert.assertTrue(record.has("/" + Constants.BYTES));
    Assert.assertEquals("2326", record.get("/" + Constants.BYTES).getValueAsString());


    record = parser.parse();
    Assert.assertNull(record);

    Assert.assertEquals(-1, Long.parseLong(parser.getOffset()));
    parser.close();
  }

  @Test(expected = IOException.class)
  public void testClose() throws Exception {
    DataParser parser = getDataParser("Hello\nByte", 1000, 6);
    parser.close();
    parser.parse();
  }

  @Test(expected = DataParserException.class)
  public void testTruncate() throws Exception {
    DataParser parser = getDataParser(
      "127.0.0.1 ss h [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326", 25, 0);
    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
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
    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
    try {
      parser.parse();
    } finally {
      parser.close();
    }
  }

  private DataParser getDataParser(String logLine, int maxObjectLength, int readerOffset) throws DataParserException {
    InputStream is = new ByteArrayInputStream(logLine.getBytes());
    DataParserFactoryBuilder dataParserFactoryBuilder = new DataParserFactoryBuilder(getContext(), DataParserFormat.LOG);
    DataParserFactory factory = dataParserFactoryBuilder
      .setMaxDataLen(maxObjectLength)
      .setMode(LogMode.COMMON_LOG_FORMAT)
      .setConfig(LogDataParserFactory.RETAIN_ORIGINAL_TEXT_KEY, true)
      .build();
    return factory.getParser("id", is, String.valueOf(readerOffset));
  }
}
