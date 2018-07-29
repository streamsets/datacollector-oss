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
package com.streamsets.pipeline.lib.parser.xml;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.DataParserFormat;
import com.streamsets.pipeline.lib.xml.StreamingXmlParser;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.testing.ApiUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TestXmlDataParserFactory {

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR, Collections.EMPTY_LIST);
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
  public void testGetParserBOM() throws Exception {
    DataParserFactoryBuilder dataParserFactoryBuilder =
        new DataParserFactoryBuilder(getContext(), DataParserFormat.XML);

    // Charset defaults to UTF-8, so no need to set it here
    DataParserFactory factory = dataParserFactoryBuilder
        .setMaxDataLen(20)
        .setConfig(XmlDataParserFactory.RECORD_ELEMENT_KEY, "e")
        .build();

    byte[] bom = {(byte)0xef, (byte)0xbb, (byte)0xbf};
    byte[] stringBytes = "<r><e>Hello</e><e>Bye</e></r>".getBytes();
    byte[] bomPlusString = new byte[bom.length + stringBytes.length];

    System.arraycopy(bom, 0, bomPlusString, 0, bom.length);
    System.arraycopy(stringBytes, 0, bomPlusString, bom.length, stringBytes.length);

    DataParser parser = factory.getParser("id", bomPlusString);
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
        .setConfig(XmlDataParserFactory.INCLUDE_FIELD_XPATH_ATTRIBUTES_KEY, true)
        .setConfig(XmlDataParserFactory.USE_FIELD_ATTRIBUTES, false)
        .build();

    InputStream is = new ByteArrayInputStream(xml.getBytes());
    DataParser parser = factory.getParser("id", is, "0");
    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertNotNull(record);
    //Map<String, Field> xpathMap = record.get().getValueAsMap().get("xpath").getValueAsMap();

    Map<String, Field> entryMap = ApiUtils.firstItemAsMap(record.get().getValueAsMap().get("entry"));

    Map<String, Field> observationMap = ApiUtils.firstItemAsMap(entryMap.get("observation"));

    Field classCodeAttr = observationMap.get(StreamingXmlParser.ATTR_PREFIX_KEY+"classCode");
    Assert.assertEquals("OBS", classCodeAttr.getValueAsString());
    Assert.assertNotNull(classCodeAttr.getAttributes());
    Assert.assertEquals(1, classCodeAttr.getAttributes().size());
    Assert.assertEquals("/root/entry/observation/@classCode", classCodeAttr.getAttribute(StreamingXmlParser.XPATH_KEY));

    Field moodCodeAttr = observationMap.get(StreamingXmlParser.ATTR_PREFIX_KEY+"moodCode");
    Assert.assertEquals("EVN", moodCodeAttr.getValueAsString());
    Assert.assertNotNull(moodCodeAttr.getAttributes());
    Assert.assertEquals(1, moodCodeAttr.getAttributes().size());
    Assert.assertEquals("/root/entry/observation/@moodCode", moodCodeAttr.getAttribute(StreamingXmlParser.XPATH_KEY));

    Field negationIndAttr = observationMap.get(StreamingXmlParser.ATTR_PREFIX_KEY+"negationInd");
    Assert.assertEquals("false", negationIndAttr.getValueAsString());
    Assert.assertNotNull(negationIndAttr.getAttributes());
    Assert.assertEquals(1, negationIndAttr.getAttributes().size());
    Assert.assertEquals("/root/entry/observation/@negationInd",
        negationIndAttr.getAttribute(StreamingXmlParser.XPATH_KEY));

    List<Field> valueList = observationMap.get("value").getValueAsList();
    Assert.assertEquals(2, valueList.size());

    Map<String, Field> value1 = valueList.get(0).getValueAsMap();

    Field typeAttr = value1.get(StreamingXmlParser.ATTR_PREFIX_KEY+"type");
    Assert.assertEquals("CD", typeAttr.getValueAsString());
    Assert.assertNotNull(typeAttr.getAttributes());
    Assert.assertEquals(1, typeAttr.getAttributes().size());
    Assert.assertEquals("/root/entry/observation/value[0]/@type", typeAttr.getAttribute(StreamingXmlParser.XPATH_KEY));

    Field codeAttr = value1.get(StreamingXmlParser.ATTR_PREFIX_KEY+"code");
    Assert.assertEquals("419199007", codeAttr.getValueAsString());
    Assert.assertNotNull(codeAttr.getAttributes());
    Assert.assertEquals(1, codeAttr.getAttributes().size());
    Assert.assertEquals("/root/entry/observation/value[0]/@code", codeAttr.getAttribute(StreamingXmlParser.XPATH_KEY));

    Field codeSysAttr = value1.get(StreamingXmlParser.ATTR_PREFIX_KEY+"codeSystem");
    Assert.assertEquals("2.16.840.1.113883.6.96", codeSysAttr.getValueAsString());
    Assert.assertNotNull(codeSysAttr.getAttributes());
    Assert.assertEquals(1, codeSysAttr.getAttributes().size());
    Assert.assertEquals("/root/entry/observation/value[0]/@codeSystem",
        codeSysAttr.getAttribute(StreamingXmlParser.XPATH_KEY));

    Map<String, Field> value2 = valueList.get(1).getValueAsMap();

    typeAttr = value2.get(StreamingXmlParser.ATTR_PREFIX_KEY+"type");
    Assert.assertEquals("CD", typeAttr.getValueAsString());
    Assert.assertNotNull(typeAttr.getAttributes());
    Assert.assertEquals(1, typeAttr.getAttributes().size());
    Assert.assertEquals("/root/entry/observation/value[1]/@type", typeAttr.getAttribute(StreamingXmlParser.XPATH_KEY));

    codeAttr = value2.get(StreamingXmlParser.ATTR_PREFIX_KEY+"code");
    Assert.assertEquals("520200118", codeAttr.getValueAsString());
    Assert.assertNotNull(codeAttr.getAttributes());
    Assert.assertEquals(1, codeAttr.getAttributes().size());
    Assert.assertEquals("/root/entry/observation/value[1]/@code", codeAttr.getAttribute(StreamingXmlParser.XPATH_KEY));

    codeSysAttr = value2.get(StreamingXmlParser.ATTR_PREFIX_KEY+"codeSystem");
    Assert.assertEquals("3.27.951.2.224994.7.07", codeSysAttr.getValueAsString());
    Assert.assertNotNull(codeSysAttr.getAttributes());
    Assert.assertEquals(1, codeSysAttr.getAttributes().size());
    Assert.assertEquals("/root/entry/observation/value[1]/@codeSystem",
        codeSysAttr.getAttribute(StreamingXmlParser.XPATH_KEY));

    parser.close();
  }
}
