/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.xml;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.io.OverrunReader;
import com.streamsets.pipeline.lib.parser.CharDataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.junit.Assert;
import org.junit.Test;

import java.io.StringReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TestXmlCharDataParserFactory {

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR, Collections.EMPTY_LIST);
  }

  @Test
  public void testGetParserString() throws Exception {
    Map<String, Object> configs = XmlCharDataParserFactory.registerConfigs(new HashMap<String, Object>());
    configs.put(XmlCharDataParserFactory.RECORD_ELEMENT_KEY, "e");
    CharDataParserFactory factory = new XmlCharDataParserFactory(getContext(), 20, configs);
    DataParser parser = factory.getParser("id", "<r><e>Hello</e><e>Bye</e></r>");
    Assert.assertEquals(0, parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("Hello", record.get("/value").getValueAsString());
    Assert.assertEquals(18, parser.getOffset());
    parser.close();
  }

  @Test
  public void testGetParserNoRecordElement() throws Exception {
    Map<String, Object> configs = XmlCharDataParserFactory.registerConfigs(new HashMap<String, Object>());
    CharDataParserFactory factory = new XmlCharDataParserFactory(getContext(), 40, configs);
    DataParser parser = factory.getParser("id", "<r><e>Hello</e><e>Bye</e></r>");
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
    Map<String, Object> configs = XmlCharDataParserFactory.registerConfigs(new HashMap<String, Object>());
    configs.put(XmlCharDataParserFactory.RECORD_ELEMENT_KEY, "e");
    CharDataParserFactory factory = new XmlCharDataParserFactory(getContext(), 20, configs);
    OverrunReader reader = new OverrunReader(new StringReader("<r><e>Hello</e><e>Bye</e></r>"), 1000, true);
    DataParser parser = factory.getParser("id", reader, 0);
    Assert.assertEquals(0, parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertTrue(record.has(""));
    Assert.assertEquals(18, parser.getOffset());
    parser.close();
  }

  @Test
  public void testGetParserReaderWithOffset() throws Exception {
    Map<String, Object> configs = XmlCharDataParserFactory.registerConfigs(new HashMap<String, Object>());
    configs.put(XmlCharDataParserFactory.RECORD_ELEMENT_KEY, "e");
    CharDataParserFactory factory = new XmlCharDataParserFactory(getContext(), 20, configs);
    OverrunReader reader = new OverrunReader(new StringReader("<r><e>Hello</e><e>Bye</e></r>"), 1000, true);
    DataParser parser = factory.getParser("id", reader, 18);
    Assert.assertEquals(18, parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertTrue(record.has(""));
    Assert.assertEquals(29, parser.getOffset());
    parser.close();
  }
}
