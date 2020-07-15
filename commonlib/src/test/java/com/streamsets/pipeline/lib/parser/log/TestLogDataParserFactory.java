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
import com.streamsets.pipeline.common.DataFormatConstants;
import com.streamsets.pipeline.config.LogMode;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.DataParserFormat;
import com.streamsets.pipeline.lib.parser.WrapperDataParserFactory;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

public class TestLogDataParserFactory {

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR, Collections.<String>emptyList());
  }

  @Test
  public void testGetParserStringWithRetainOriginalText() throws Exception {
    DataParserFactoryBuilder dataParserFactoryBuilder =
        new DataParserFactoryBuilder(getContext(), DataParserFormat.LOG);
    DataParserFactory factory = dataParserFactoryBuilder
        .setMaxDataLen(100)
        .setMode(LogMode.COMMON_LOG_FORMAT)
        .setConfig(LogDataParserFactory.RETAIN_ORIGINAL_TEXT_KEY, true)
        .build();

    DataParser parser = factory.getParser("id",
                                          "127.0.0.1 ss h [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326"
                                              .getBytes());

    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertTrue(record.has("/originalLine"));

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

    Assert.assertEquals(82, Long.parseLong(parser.getOffset()));
    parser.close();
  }

  @Test
  public void testGetParserStringWithOutRetainOriginalText() throws Exception {
    DataParserFactoryBuilder dataParserFactoryBuilder =
        new DataParserFactoryBuilder(getContext(), DataParserFormat.LOG);
    DataParserFactory factory = dataParserFactoryBuilder
        .setMaxDataLen(100)
        .setMode(LogMode.COMMON_LOG_FORMAT)
        .setConfig(LogDataParserFactory.RETAIN_ORIGINAL_TEXT_KEY, false)
        .build();


    DataParser parser = factory.getParser("id",
                                          "127.0.0.1 ss h [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326"
                                              .getBytes());

    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertFalse(record.has("/originalLine"));

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

    Assert.assertEquals(82, Long.parseLong(parser.getOffset()));

    parser.close();
  }

  @Test
  public void testGetParserReader() throws Exception {
    DataParserFactoryBuilder dataParserFactoryBuilder =
        new DataParserFactoryBuilder(getContext(), DataParserFormat.LOG);
    DataParserFactory factory = dataParserFactoryBuilder
        .setMaxDataLen(100)
        .setMode(LogMode.COMMON_LOG_FORMAT)
        .setConfig(LogDataParserFactory.RETAIN_ORIGINAL_TEXT_KEY, true)
        .build();

    InputStream is = new ByteArrayInputStream(
        "127.0.0.1 ss h [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326".getBytes());

    DataParser parser = factory.getParser("id", is, "0");

    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertTrue(record.has("/originalLine"));

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

    Assert.assertEquals(82, Long.parseLong(parser.getOffset()));

    parser.close();
  }

  @Test(expected = DataParserException.class)
  public void testGetParserReaderLogLineCutShort() throws Exception {
    DataParserFactoryBuilder dataParserFactoryBuilder =
        new DataParserFactoryBuilder(getContext(), DataParserFormat.LOG);
    DataParserFactory factory = dataParserFactoryBuilder
        .setMaxDataLen(25) //cut short the capacity of the reader
        .setMode(LogMode.COMMON_LOG_FORMAT)
        .setConfig(LogDataParserFactory.RETAIN_ORIGINAL_TEXT_KEY, true)
        .build();


    InputStream is = new ByteArrayInputStream(
        "127.0.0.1 ss h [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326".getBytes());
    DataParser parser = factory.getParser("id", is, "0");

    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
    try {
      parser.parse();
    } finally {
      parser.close();
    }
  }

  @Test
  public void testGetParserReaderWithOffset() throws Exception {
    DataParserFactoryBuilder dataParserFactoryBuilder =
        new DataParserFactoryBuilder(getContext(), DataParserFormat.LOG);
    DataParserFactory factory = dataParserFactoryBuilder
        .setMaxDataLen(150)
        .setMode(LogMode.COMMON_LOG_FORMAT)
        .setConfig(LogDataParserFactory.RETAIN_ORIGINAL_TEXT_KEY, false)
        .build();

    InputStream is = new ByteArrayInputStream(
        "Hello\n127.0.0.1 ss h [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326".getBytes());

    DataParser parser = factory.getParser("id", is, "6");

    Assert.assertEquals(6, Long.parseLong(parser.getOffset()));

    Record record = parser.parse();

    Assert.assertFalse(record.has("/originalLine")); //do not retain original line

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

    Assert.assertEquals(88, Long.parseLong(parser.getOffset()));

    parser.close();
  }

  @Test
  public void testFactoryCombinedLogFormatParser() throws DataParserException, IOException {
    DataParserFactoryBuilder dataParserFactoryBuilder =
        new DataParserFactoryBuilder(getContext(), DataParserFormat.LOG);
    DataParserFactory factory = dataParserFactoryBuilder
        .setMaxDataLen(1000)
        .setMode(LogMode.COMBINED_LOG_FORMAT)
        .setConfig(LogDataParserFactory.RETAIN_ORIGINAL_TEXT_KEY, true)
        .build();

    DataParser parser = factory.getParser("id",
                                          "127.0.0.1 ss h [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326 \"http:www.example.com/start.html\" \"Mozilla/4.08 [en] (Win98; I ;Nav)\""
                                              .getBytes());

    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertNotNull(record);

    Assert.assertEquals("id::0", record.getHeader().getSourceId());

    Assert.assertEquals("127.0.0.1 ss h [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326 " +
                        "\"http:www.example.com/start.html\" \"Mozilla/4.08 [en] (Win98; I ;Nav)\"",
                        record.get().getValueAsMap().get("originalLine").getValueAsString());

    Assert.assertFalse(record.has("/truncated"));

    Assert.assertEquals(152, Long.parseLong(parser.getOffset()));

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


    Assert.assertTrue(record.has("/" + Constants.REFERRER));
    Assert.assertEquals("\"http:www.example.com/start.html\"", record.get("/" + Constants.REFERRER).getValueAsString());

    Assert.assertTrue(record.has("/" + Constants.AGENT));
    Assert.assertEquals("\"Mozilla/4.08 [en] (Win98; I ;Nav)\"", record.get("/" + Constants.AGENT).getValueAsString());

    parser.close();
  }

  @Test
  public void testFactoryApacheErrorLogFormatParser() throws DataParserException, IOException {
    DataParserFactoryBuilder dataParserFactoryBuilder =
        new DataParserFactoryBuilder(getContext(), DataParserFormat.LOG);
    DataParserFactory factory = dataParserFactoryBuilder
        .setMaxDataLen(1000)
        .setMode(LogMode.APACHE_ERROR_LOG_FORMAT)
        .setConfig(LogDataParserFactory.RETAIN_ORIGINAL_TEXT_KEY, true)
        .build();

    DataParser parser = factory.getParser("id",
                                          "[Wed Oct 11 14:32:52 2000] [error] [client 127.0.0.1] client denied by server configuration: /export/home/live/ap/htdocs/test"
                                              .getBytes());

    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertNotNull(record);

    Assert.assertEquals("id::0", record.getHeader().getSourceId());

    Assert.assertEquals("[Wed Oct 11 14:32:52 2000] [error] [client 127.0.0.1] client denied " +
                        "by server configuration: /export/home/live/ap/htdocs/test",
                        record.get().getValueAsMap().get("originalLine").getValueAsString());

    Assert.assertFalse(record.has("/truncated"));

    Assert.assertEquals(125, Long.parseLong(parser.getOffset()));

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
  public void testFactoryApacheCustomFormatParser() throws DataParserException, IOException {

    String logLine = "127.0.0.1 ss h [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326 " +
                     "\"http:www.example.com/start.html\" \"Mozilla/4.08 [en] (Win98; I ;Nav)\"";

    DataParserFactoryBuilder dataParserFactoryBuilder =
        new DataParserFactoryBuilder(getContext(), DataParserFormat.LOG);
    DataParserFactory factory = dataParserFactoryBuilder
        .setMaxDataLen(1000)
        .setMode(LogMode.APACHE_CUSTOM_LOG_FORMAT)
        .setConfig(LogDataParserFactory.RETAIN_ORIGINAL_TEXT_KEY, true)
        .setConfig(LogDataParserFactory.APACHE_CUSTOMLOG_FORMAT_KEY,
                   "%h %l %u [%t] \"%m %U %H\" %>s %b \"%{Referer}i\" \"%{User-agent}i\"")
        .build();

    DataParser parser = factory.getParser("id", logLine.getBytes());

    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertNotNull(record);

    Assert.assertEquals("id::0", record.getHeader().getSourceId());

    Assert.assertEquals(logLine, record.get().getValueAsMap().get("originalLine").getValueAsString());

    Assert.assertFalse(record.has("/truncated"));

    Assert.assertEquals(152, Long.parseLong(parser.getOffset()));

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

    Assert.assertTrue(record.has("/httpversion"));
    Assert.assertEquals("1.0", record.get("/httpversion").getValueAsString());

    Assert.assertTrue(record.has("/status"));
    Assert.assertEquals("200", record.get("/status").getValueAsString());

    Assert.assertTrue(record.has("/bytesSent"));
    Assert.assertEquals("2326", record.get("/bytesSent").getValueAsString());

    Assert.assertTrue(record.has("/referer"));
    Assert.assertEquals("http:www.example.com/start.html", record.get("/referer").getValueAsString());

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

    DataParserFactoryBuilder dataParserFactoryBuilder =
        new DataParserFactoryBuilder(getContext(), DataParserFormat.LOG);
    DataParserFactory factory = dataParserFactoryBuilder
        .setMaxDataLen(1000)
        .setMode(LogMode.REGEX)
        .setConfig(LogDataParserFactory.RETAIN_ORIGINAL_TEXT_KEY, true)
        .setConfig(LogDataParserFactory.REGEX_KEY, regex)
        .setConfig(LogDataParserFactory.REGEX_FIELD_PATH_TO_GROUP_KEY, fieldToGroupMap)
        .build();

    DataParser parser = factory.getParser("id", logLine.getBytes());


    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertNotNull(record);

    Assert.assertEquals("id::0", record.getHeader().getSourceId());

    Assert.assertEquals(logLine, record.get().getValueAsMap().get("originalLine").getValueAsString());

    Assert.assertFalse(record.has("/truncated"));

    Assert.assertEquals(88, Long.parseLong(parser.getOffset()));

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

  @Test
  public void testCharacterBaseParserMethod() throws Exception {
    DataParserFactoryBuilder dataParserFactoryBuilder =
        new DataParserFactoryBuilder(getContext(), DataParserFormat.LOG);
    DataParserFactory factory = dataParserFactoryBuilder
        .setMaxDataLen(100)
        .setMode(LogMode.COMMON_LOG_FORMAT)
        .setConfig(LogDataParserFactory.RETAIN_ORIGINAL_TEXT_KEY, true)
        .build();

    DataParser parser =
        factory.getParser("id", "127.0.0.1 ss h [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326");

    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertTrue(record.has("/originalLine"));

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

    Assert.assertEquals(82, Long.parseLong(parser.getOffset()));
    parser.close();
  }

  @Test
  public void testParserFactoryCurrentLineStringBuilderPool() throws Exception {

    DataParserFactoryBuilder dataParserFactoryBuilder = new DataParserFactoryBuilder(getContext(), DataParserFormat.LOG);
    WrapperDataParserFactory factory = (WrapperDataParserFactory) dataParserFactoryBuilder
      .setMaxDataLen(1000)
      .setMaxDataLen(100)
      .setMode(LogMode.COMMON_LOG_FORMAT)
      .build();

    LogDataParserFactory logDataParserFactory = (LogDataParserFactory) factory.getFactory();
    GenericObjectPool<StringBuilder> stringBuilderPool = logDataParserFactory.getCurrentLineBuilderPool();

    testStringBuilderPool(
        stringBuilderPool,
        logDataParserFactory,
        DataFormatConstants.DEFAULT_STRING_BUILDER_POOL_SIZE
    );
  }

  @Test
  public void testParserFactoryPreviousLineStringBuilderPool() throws Exception {

    DataParserFactoryBuilder dataParserFactoryBuilder = new DataParserFactoryBuilder(getContext(), DataParserFormat.LOG);
    WrapperDataParserFactory factory = (WrapperDataParserFactory) dataParserFactoryBuilder
      .setMaxDataLen(1000)
      .setMaxDataLen(100)
      .setMode(LogMode.COMMON_LOG_FORMAT)
      .build();

    LogDataParserFactory logDataParserFactory = (LogDataParserFactory) factory.getFactory();
    GenericObjectPool<StringBuilder> stringBuilderPool = logDataParserFactory.getPreviousLineBuilderPool();
    testStringBuilderPool(
        stringBuilderPool,
        logDataParserFactory,
        DataFormatConstants.DEFAULT_STRING_BUILDER_POOL_SIZE
    );
  }

  @Test
  public void testStringBuilderPoolException() throws Exception {

    DataParserFactoryBuilder dataParserFactoryBuilder = new DataParserFactoryBuilder(getContext(), DataParserFormat.LOG);
    WrapperDataParserFactory factory = (WrapperDataParserFactory) dataParserFactoryBuilder
      .setMaxDataLen(1000)
      .setMaxDataLen(100)
      .setMode(LogMode.COMMON_LOG_FORMAT)
      .build();

    LogDataParserFactory logDataParserFactory = (LogDataParserFactory) factory.getFactory();
    GenericObjectPool<StringBuilder> stringBuilderPool = logDataParserFactory.getPreviousLineBuilderPool();
    Assert.assertNotNull(stringBuilderPool);
    Assert.assertEquals(DataFormatConstants.DEFAULT_STRING_BUILDER_POOL_SIZE, stringBuilderPool.getMaxIdle());
    Assert.assertEquals(1, stringBuilderPool.getMinIdle());
    Assert.assertEquals(DataFormatConstants.DEFAULT_STRING_BUILDER_POOL_SIZE, stringBuilderPool.getMaxTotal());
    Assert.assertEquals(0, stringBuilderPool.getNumIdle());
    Assert.assertEquals(0, stringBuilderPool.getNumActive());

    for (int i = 0; i < DataFormatConstants.DEFAULT_STRING_BUILDER_POOL_SIZE; ++i) {
      factory.getParser("id", "Hello\\r\\nBye");
    }

    Assert.assertEquals(0, stringBuilderPool.getNumIdle());
    Assert.assertEquals(DataFormatConstants.DEFAULT_STRING_BUILDER_POOL_SIZE, stringBuilderPool.getNumActive());

    try {
      factory.getParser("id", "Hello\\r\\nBye");
      Assert.fail("Expected IOException which wraps NoSuchElementException since pool is empty");
    } catch (DataParserException e) {
      Assert.assertTrue(e.getCause() instanceof IOException);
      Assert.assertTrue(e.getCause().getCause() instanceof NoSuchElementException);
    }

  }

  private void testStringBuilderPool(
      GenericObjectPool<StringBuilder> stringBuilderPool,
      LogDataParserFactory factory,
      int poolSize
  ) throws Exception {
    Assert.assertNotNull(stringBuilderPool);
    Assert.assertEquals(poolSize, stringBuilderPool.getMaxIdle());
    Assert.assertEquals(1, stringBuilderPool.getMinIdle());
    Assert.assertEquals(poolSize, stringBuilderPool.getMaxTotal());
    Assert.assertEquals(0, stringBuilderPool.getNumIdle());
    Assert.assertEquals(0, stringBuilderPool.getNumActive());

    DataParser parser = factory.getParser("id", "Hello\\r\\nBye");

    Assert.assertEquals(0, stringBuilderPool.getNumIdle());
    Assert.assertEquals(1, stringBuilderPool.getNumActive());

    parser.close();

    Assert.assertEquals(1, stringBuilderPool.getNumIdle());
    Assert.assertEquals(0, stringBuilderPool.getNumActive());
  }

}
