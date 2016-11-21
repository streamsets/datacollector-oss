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
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.DataParserFormat;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;

public class TestXmlDataParserFactory {

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
  public void testGetParserString() throws Exception {

    DataParserFactoryBuilder dataParserFactoryBuilder =
        new DataParserFactoryBuilder(getContext(), DataParserFormat.XML);
    DataParserFactory factory = dataParserFactoryBuilder
        .setMaxDataLen(20)
        .setConfig(XmlDataParserFactory.RECORD_ELEMENT_KEY, "e")
        .build();

    DataParser parser = factory.getParser("id", "<r><e>Hello</e><e>Bye</e></r>".getBytes());
    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("Hello", record.get("/value").getValueAsString());
    Assert.assertEquals(18, Long.parseLong(parser.getOffset()));
    parser.close();
  }

  @Test
  public void testGetParserNoRecordElement() throws Exception {
    DataParserFactoryBuilder dataParserFactoryBuilder =
        new DataParserFactoryBuilder(getContext(), DataParserFormat.XML);
    DataParserFactory factory = dataParserFactoryBuilder
        .setMaxDataLen(40)
        .build();


    DataParser parser = factory.getParser("id", "<r><e>Hello</e><e>Bye</e></r>".getBytes());
    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("Hello", record.get("/e[0]/value").getValueAsString());
    Assert.assertEquals("Bye", record.get("/e[1]/value").getValueAsString());
    Assert.assertEquals(-1, Long.parseLong(parser.getOffset()));
    parser.close();
  }

  @Test
  public void testGetParserReader() throws Exception {
    DataParserFactoryBuilder dataParserFactoryBuilder =
        new DataParserFactoryBuilder(getContext(), DataParserFormat.XML);
    DataParserFactory factory = dataParserFactoryBuilder
        .setMaxDataLen(20)
        .setConfig(XmlDataParserFactory.RECORD_ELEMENT_KEY, "e")
        .build();

    InputStream is = new ByteArrayInputStream("<r><e>Hello</e><e>Bye</e></r>".getBytes());
    DataParser parser = factory.getParser("id", is, "0");
    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertTrue(record.has(""));
    Assert.assertEquals(18, Long.parseLong(parser.getOffset()));
    parser.close();
  }

  @Test
  public void testGetParserReaderWithOffset() throws Exception {
    DataParserFactoryBuilder dataParserFactoryBuilder =
        new DataParserFactoryBuilder(getContext(), DataParserFormat.XML);
    DataParserFactory factory = dataParserFactoryBuilder
        .setMaxDataLen(20)
        .setConfig(XmlDataParserFactory.RECORD_ELEMENT_KEY, "e")
        .build();

    InputStream is = new ByteArrayInputStream("<r><e>Hello</e><e>Bye</e></r>".getBytes());
    DataParser parser = factory.getParser("id", is, "18");
    Assert.assertEquals(18, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertTrue(record.has(""));
    Assert.assertEquals(29, Long.parseLong(parser.getOffset()));
    parser.close();
  }

  @Test
  public void testCharacterBaseParserMethod() throws Exception {
    DataParserFactoryBuilder dataParserFactoryBuilder =
        new DataParserFactoryBuilder(getContext(), DataParserFormat.XML);
    DataParserFactory factory = dataParserFactoryBuilder
        .setMaxDataLen(20)
        .setConfig(XmlDataParserFactory.RECORD_ELEMENT_KEY, "e")
        .build();

    DataParser parser = factory.getParser("id", "<r><e>Hello</e><e>Bye</e></r>");
    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertTrue(record.has(""));
    Assert.assertEquals(18, Long.parseLong(parser.getOffset()));
    parser.close();

  }

  @Test
  public void testXpath() throws Exception {
    String xml =
        "<root>" +
        "  <entry>" +
        "    <observation classCode=\"OBS\" moodCode=\"EVN\" negationInd=\"false\">" +
        "      <value type=\"CD\" code=\"419199007\" codeSystem=\"2.16.840.1.113883.6.96\"/>" +
        "      <value type=\"CD\" code=\"520200118\" codeSystem=\"3.27.951.2.224994.7.07\"/>" +
        "    </observation>" +
        "  </entry>" +
        "</root>";
    DataParserFactoryBuilder dataParserFactoryBuilder =
        new DataParserFactoryBuilder(getContext(), DataParserFormat.XML);
    DataParserFactory factory = dataParserFactoryBuilder
        .setMaxDataLen(1000)
        .setConfig(XmlDataParserFactory.RECORD_ELEMENT_KEY, "")
        .build();

    InputStream is = new ByteArrayInputStream(xml.getBytes());
    DataParser parser = factory.getParser("id", is, "0");
    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Map<String, Field> xpathMap = record.get().getValueAsMap().get("xpath").getValueAsMap();
    Assert.assertEquals("OBS", xpathMap.get("/root/entry/observation@classCode").getValueAsString());
    Assert.assertEquals("EVN", xpathMap.get("/root/entry/observation@moodCode").getValueAsString());
    Assert.assertEquals("false", xpathMap.get("/root/entry/observation@negationInd").getValueAsString());
    Assert.assertEquals("CD", xpathMap.get("/root/entry/observation/value[0]@type").getValueAsString());
    Assert.assertEquals("419199007", xpathMap.get("/root/entry/observation/value[0]@code").getValueAsString());
    Assert.assertEquals("2.16.840.1.113883.6.96", xpathMap.get("/root/entry/observation/value[0]@codeSystem").getValueAsString());
    Assert.assertEquals("CD", xpathMap.get("/root/entry/observation/value[1]@type").getValueAsString());
    Assert.assertEquals("520200118", xpathMap.get("/root/entry/observation/value[1]@code").getValueAsString());
    Assert.assertEquals("3.27.951.2.224994.7.07", xpathMap.get("/root/entry/observation/value[1]@codeSystem").getValueAsString());
    parser.close();
  }
}
