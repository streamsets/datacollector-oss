/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.text;

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

public class TestTextCharDataParserFactory {

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR, Collections.EMPTY_LIST);
  }

  @Test
  public void testGetParserString() throws Exception {
    Map<String, Object> configs = TextCharDataParserFactory.registerConfigs(new HashMap<String, Object>());
    CharDataParserFactory factory = new TextCharDataParserFactory(getContext(), 3, configs);
    DataParser parser = factory.getParser("id", "Hello\n");
    Assert.assertEquals(0, parser.getOffset());
    Record record = parser.parse();
    Assert.assertTrue(record.has("/text"));
    Assert.assertTrue(record.has("/truncated"));
    Assert.assertEquals(6, parser.getOffset());
    parser.close();
  }

  @Test
  public void testGetParserReader() throws Exception {
    Map<String, Object> configs = TextCharDataParserFactory.registerConfigs(new HashMap<String, Object>());
    CharDataParserFactory factory = new TextCharDataParserFactory(getContext(), 3, configs);
    OverrunReader reader = new OverrunReader(new StringReader("Hello\nBye"), 1000, true);
    DataParser parser = factory.getParser("id", reader, 0);
    Assert.assertEquals(0, parser.getOffset());
    Record record = parser.parse();
    Assert.assertTrue(record.has("/text"));
    Assert.assertTrue(record.has("/truncated"));
    Assert.assertEquals(6, parser.getOffset());
    parser.close();
  }

  @Test
  public void testGetParserReaderWithOffset() throws Exception {
    Map<String, Object> configs = TextCharDataParserFactory.registerConfigs(new HashMap<String, Object>());
    CharDataParserFactory factory = new TextCharDataParserFactory(getContext(), 3, configs);
    OverrunReader reader = new OverrunReader(new StringReader("Hello\nBye"), 1000, true);
    DataParser parser = factory.getParser("id", reader, 6);
    Assert.assertEquals(6, parser.getOffset());
    Record record = parser.parse();
    Assert.assertTrue(record.has("/text"));
    Assert.assertEquals(9, parser.getOffset());
    parser.close();
  }
}
