/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.lib.parser.xml;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.io.OverrunReader;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.Map;

public class TestXmlCharDataParser {

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR, Collections.EMPTY_LIST);
  }

  @Before
  public void setUp() {
    System.setProperty(XmlCharDataParser.INCLUDE_XPATH_MAP, "true");
  }

  @After
  public void tearDown() {
    System.getProperties().remove(XmlCharDataParser.INCLUDE_XPATH_MAP);
  }

  @Test
  public void testParse() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("<r><e>Hello</e><e>Bye</e></r>"), 1000, true, false);
    DataParser parser = new XmlCharDataParser(getContext(), "id", reader, 0, "e", 100);
    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::0", record.getHeader().getSourceId());
    Assert.assertEquals("Hello", record.get().getValueAsMap().get("value").getValueAsString());
    Assert.assertEquals(18, Long.parseLong(parser.getOffset()));
    record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::18", record.getHeader().getSourceId());
    Assert.assertEquals("Bye", record.get().getValueAsMap().get("value").getValueAsString());
    Assert.assertEquals(29, Long.parseLong(parser.getOffset()));
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals(-1, Long.parseLong(parser.getOffset()));
    parser.close();
  }

  @Test
  public void testXpathWithoutDelimiterElement() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader(
        "<bookstore>\n" +
        "  <book>\n" +
        "    <title lang=\"en\">Harry Potter</title>\n" +
        "    <price>29.99</price>\n" +
        "  </book>\n" +
        "  <book>\n" +
        "    <title lang=\"en\">Learning XML</title>\n" +
        "    <price>39.95</price>\n" +
        "  </book>\n" +
        "</bookstore>"
    ), 1000, true, false);
    DataParser parser = new XmlCharDataParser(getContext(), "id", reader, 0, "", null, 1000);
    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Map<String, Field> xpathMap = record.get().getValueAsMap().get("xpath").getValueAsMap();
    Assert.assertEquals("Harry Potter", xpathMap.get("/bookstore/book[0]/title").getValueAsString());
    Assert.assertEquals("en", xpathMap.get("/bookstore/book[0]/title@lang").getValueAsString());
    Assert.assertEquals("29.99", xpathMap.get("/bookstore/book[0]/price").getValueAsString());
    Assert.assertEquals("Learning XML", xpathMap.get("/bookstore/book[1]/title").getValueAsString());
    Assert.assertEquals("en", xpathMap.get("/bookstore/book[1]/title@lang").getValueAsString());
    Assert.assertEquals("39.95", xpathMap.get("/bookstore/book[1]/price").getValueAsString());
    parser.close();
  }

  @Test
  public void testXpathWithDelimiterElement() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader(
        "<bookstore>\n" +
        "  <book>\n" +
        "    <title lang=\"en\">Harry Potter</title>\n" +
        "    <price>29.99</price>\n" +
        "  </book>\n" +
        "  <book>\n" +
        "    <title lang=\"en\">Learning XML</title>\n" +
        "    <price>39.95</price>\n" +
        "  </book>\n" +
        "</bookstore>"
    ), 1000, true, false);
    DataParser parser = new XmlCharDataParser(getContext(), "id", reader, 0, "book", null, 1000);
    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Map<String, Field> xpathMap = record.get().getValueAsMap().get("xpath").getValueAsMap();
    Assert.assertEquals("Harry Potter", xpathMap.get("/bookstore/book/title").getValueAsString());
    Assert.assertEquals("en", xpathMap.get("/bookstore/book/title@lang").getValueAsString());
    Assert.assertEquals("29.99", xpathMap.get("/bookstore/book/price").getValueAsString());
    record = parser.parse();
    Assert.assertNotNull(record);
    xpathMap = record.get().getValueAsMap().get("xpath").getValueAsMap();
    Assert.assertEquals("Learning XML", xpathMap.get("/bookstore/book/title").getValueAsString());
    Assert.assertEquals("en", xpathMap.get("/bookstore/book/title@lang").getValueAsString());
    Assert.assertEquals("39.95", xpathMap.get("/bookstore/book/price").getValueAsString());
    parser.close();
  }

  @Test
  public void testParseWithOffset() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("<r><e>Hello</e><e>Bye</e></r>"), 1000, true, false);
    DataParser parser = new XmlCharDataParser(getContext(), "id", reader, 18, "e", 100);
    Assert.assertEquals(18, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::18", record.getHeader().getSourceId());
    Assert.assertEquals("Bye", record.get().getValueAsMap().get("value").getValueAsString());
    Assert.assertEquals(29, Long.parseLong(parser.getOffset()));
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals(-1, Long.parseLong(parser.getOffset()));
    parser.close();
  }

  @Test(expected = IOException.class)
  public void testClose() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("<r><e>Hello</e><e>Bye</e></r>"), 1000, true, false);
    DataParser parser = new XmlCharDataParser(getContext(), "id", reader, 0, "e", 100);
    parser.close();
    parser.parse();
  }

}
