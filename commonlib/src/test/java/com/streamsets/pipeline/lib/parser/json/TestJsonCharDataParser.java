/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.json;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.io.OverrunReader;
import com.streamsets.pipeline.lib.json.OverrunStreamingJsonParser;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;

public class TestJsonCharDataParser {

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR, Collections.EMPTY_LIST);
  }

  @Test
  public void testParseMultipleObjects() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("[\"Hello\"]\n[\"Bye\"]"), 1000, true);
    DataParser parser = new JsonCharDataParser(getContext(), "id", reader, 0,
                                           OverrunStreamingJsonParser.Mode.MULTIPLE_OBJECTS, 100);
    Assert.assertEquals(0, parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::0", record.getHeader().getSourceId());
    Assert.assertEquals("Hello", record.get().getValueAsList().get(0).getValueAsString());
    Assert.assertEquals(10, parser.getOffset());
    record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::10", record.getHeader().getSourceId());
    Assert.assertEquals("Bye", record.get().getValueAsList().get(0).getValueAsString());
    Assert.assertEquals(17, parser.getOffset());
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals(-1, parser.getOffset());
    parser.close();
  }

  @Test
  public void testParseMultipleObjectsWithOffset() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("[\"Hello\"]\n[\"Bye\"]"), 1000, true);
    DataParser parser = new JsonCharDataParser(getContext(), "id", reader, 10,
                                           OverrunStreamingJsonParser.Mode.MULTIPLE_OBJECTS, 100);
    Assert.assertEquals(10, parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::10", record.getHeader().getSourceId());
    Assert.assertEquals("Bye", record.get().getValueAsList().get(0).getValueAsString());
    Assert.assertEquals(17, parser.getOffset());
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals(-1, parser.getOffset());
    parser.close();
  }

  /*
   TODO: investigate SDC-265
  @Test
  public void testParseMultipleStringObjects() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("\"Hello\"\"Bye\""), 1000, true);
    DataParser parser = new JsonDataParser(getContext(), "id", reader, 0, OverrunStreamingJsonParser.Mode.MULTIPLE_OBJECTS, 100);
    Assert.assertEquals(0, parser.getOffset()); // "HELLO""
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::0", record.getHeader().getSourceId());
    Assert.assertEquals("Hello", record.get().getValueAsString());
    Assert.assertEquals(7, parser.getOffset());
    record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::7", record.getHeader().getSourceId());
    Assert.assertEquals("Bye", record.get().getValueAsString());
    Assert.assertEquals(12, parser.getOffset());
    record = parser.parse();
    Assert.assertEquals(12, parser.getOffset());
    Assert.assertNull(record);
    Assert.assertEquals(-1, parser.getOffset());
    parser.close();
  }
   */

  @Test(expected = IOException.class)
  public void testClose() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("\"Hello\"\"Bye\""), 1000, true);
    DataParser parser = new JsonCharDataParser(getContext(), "id", reader, 0,
                                           OverrunStreamingJsonParser.Mode.MULTIPLE_OBJECTS, 10);
    parser.close();
    parser.parse();
  }

  @Test
  public void testParseArray() throws Exception {
    //TODO: investigate SDC-265, if the elements of the array are Strings the same issue shows up
    OverrunReader reader = new OverrunReader(new StringReader("[[\"Hello\"],[\"Bye\"]]"), 1000, true);
    DataParser parser = new JsonCharDataParser(getContext(), "id", reader, 0,
                                           OverrunStreamingJsonParser.Mode.ARRAY_OBJECTS, 10);
    Assert.assertEquals(0, parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::0", record.getHeader().getSourceId());
    Assert.assertEquals("Hello", record.get().getValueAsList().get(0).getValueAsString());
    Assert.assertEquals(9, parser.getOffset());
    record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::9", record.getHeader().getSourceId());
    Assert.assertEquals("Bye", record.get().getValueAsList().get(0).getValueAsString());
    Assert.assertEquals(17, parser.getOffset());
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals(-1, parser.getOffset());
    parser.close();
  }

  @Test
  public void testParseArrayWithOffset() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("[[\"Hello\"],[\"Bye\"]]"), 1000, true);
    DataParser parser = new JsonCharDataParser(getContext(), "id", reader, 9,
                                           OverrunStreamingJsonParser.Mode.ARRAY_OBJECTS, 10);
    Assert.assertEquals(9, parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::9", record.getHeader().getSourceId());
    Assert.assertEquals("Bye", record.get().getValueAsList().get(0).getValueAsString());
    Assert.assertEquals(17, parser.getOffset());
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals(-1, parser.getOffset());
    parser.close();
  }

}
