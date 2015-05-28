/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.xml;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.data.DataFactory;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.DataParserFormat;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Collections;

public class TestXmlDataParserFactory {

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR, Collections.EMPTY_LIST);
  }

  @Test
  public void testGetParserString() throws Exception {

    DataParserFactoryBuilder dataParserFactoryBuilder =
        new DataParserFactoryBuilder(getContext(), DataParserFormat.XML);
    DataFactory dataFactory = dataParserFactoryBuilder
        .setMaxDataLen(20)
        .setConfig(XmlDataParserFactory.RECORD_ELEMENT_KEY, "e")
        .build();
    Assert.assertTrue(dataFactory instanceof XmlDataParserFactory);
    XmlDataParserFactory factory = (XmlDataParserFactory) dataFactory;

    DataParser parser = factory.getParser("id", "<r><e>Hello</e><e>Bye</e></r>".getBytes());
    Assert.assertEquals(0, parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("Hello", record.get("/value").getValueAsString());
    Assert.assertEquals(18, parser.getOffset());
    parser.close();
  }

  @Test
  public void testGetParserNoRecordElement() throws Exception {
    DataParserFactoryBuilder dataParserFactoryBuilder =
        new DataParserFactoryBuilder(getContext(), DataParserFormat.XML);
    DataFactory dataFactory = dataParserFactoryBuilder
        .setMaxDataLen(40)
        .build();
    Assert.assertTrue(dataFactory instanceof XmlDataParserFactory);
    XmlDataParserFactory factory = (XmlDataParserFactory) dataFactory;


    DataParser parser = factory.getParser("id", "<r><e>Hello</e><e>Bye</e></r>".getBytes());
    Assert.assertEquals(0, parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("Hello", record.get("/e[0]/value").getValueAsString());
    Assert.assertEquals("Bye", record.get("/e[1]/value").getValueAsString());
    Assert.assertEquals(-1, parser.getOffset());
    parser.close();
  }

  @Test
  public void testGetParserReader() throws Exception {
    DataParserFactoryBuilder dataParserFactoryBuilder =
        new DataParserFactoryBuilder(getContext(), DataParserFormat.XML);
    DataFactory dataFactory = dataParserFactoryBuilder
        .setMaxDataLen(20)
        .setConfig(XmlDataParserFactory.RECORD_ELEMENT_KEY, "e")
        .build();
    Assert.assertTrue(dataFactory instanceof XmlDataParserFactory);
    XmlDataParserFactory factory = (XmlDataParserFactory) dataFactory;

    InputStream is = new ByteArrayInputStream("<r><e>Hello</e><e>Bye</e></r>".getBytes());
    DataParser parser = factory.getParser("id", is, 0);
    Assert.assertEquals(0, parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertTrue(record.has(""));
    Assert.assertEquals(18, parser.getOffset());
    parser.close();
  }

  @Test
  public void testGetParserReaderWithOffset() throws Exception {
    DataParserFactoryBuilder dataParserFactoryBuilder =
        new DataParserFactoryBuilder(getContext(), DataParserFormat.XML);
    DataFactory dataFactory = dataParserFactoryBuilder
        .setMaxDataLen(20)
        .setConfig(XmlDataParserFactory.RECORD_ELEMENT_KEY, "e")
        .build();
    Assert.assertTrue(dataFactory instanceof XmlDataParserFactory);
    XmlDataParserFactory factory = (XmlDataParserFactory) dataFactory;

    InputStream is = new ByteArrayInputStream("<r><e>Hello</e><e>Bye</e></r>".getBytes());
    DataParser parser = factory.getParser("id", is, 18);
    Assert.assertEquals(18, parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertTrue(record.has(""));
    Assert.assertEquals(29, parser.getOffset());
    parser.close();
  }

  @Test
  public void testCharacterBaseParserMethod() throws Exception {
    DataParserFactoryBuilder dataParserFactoryBuilder =
        new DataParserFactoryBuilder(getContext(), DataParserFormat.XML);
    DataFactory dataFactory = dataParserFactoryBuilder
        .setMaxDataLen(20)
        .setConfig(XmlDataParserFactory.RECORD_ELEMENT_KEY, "e")
        .build();
    Assert.assertTrue(dataFactory instanceof XmlDataParserFactory);
    XmlDataParserFactory factory = (XmlDataParserFactory) dataFactory;

    DataParser parser = factory.getParser("id", "<r><e>Hello</e><e>Bye</e></r>");
    Assert.assertEquals(0, parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertTrue(record.has(""));
    Assert.assertEquals(18, parser.getOffset());
    parser.close();

  }
}
