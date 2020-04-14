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
package com.streamsets.pipeline.lib.parser.json;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ext.io.OverrunReader;
import com.streamsets.pipeline.api.ext.json.Mode;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;

public class TestJsonCharDataParser {

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR, Collections.emptyList());
  }

  @Test
  public void testParseMultipleObjects() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("[\"Hello\"]\n[\"Bye\"]"), 1000, true, false);
    DataParser parser = new JsonCharDataParser(getContext(), "id", reader, 0,
                                           Mode.MULTIPLE_OBJECTS, 100);
    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::0", record.getHeader().getSourceId());
    Assert.assertEquals("Hello", record.get().getValueAsList().get(0).getValueAsString());
    Assert.assertEquals(10, Long.parseLong(parser.getOffset()));
    record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::10", record.getHeader().getSourceId());
    Assert.assertEquals("Bye", record.get().getValueAsList().get(0).getValueAsString());
    Assert.assertEquals(17, Long.parseLong(parser.getOffset()));
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals(-1, Long.parseLong(parser.getOffset()));
    parser.close();
  }

  @Test
  public void testParseMultipleObjectsWithOffset() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("[\"Hello\"]\n[\"Bye\"]"), 1000, true, false);
    DataParser parser = new JsonCharDataParser(getContext(), "id", reader, 10,
                                           Mode.MULTIPLE_OBJECTS, 100);
    Assert.assertEquals(10, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::10", record.getHeader().getSourceId());
    Assert.assertEquals("Bye", record.get().getValueAsList().get(0).getValueAsString());
    Assert.assertEquals(17, Long.parseLong(parser.getOffset()));
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals(-1, Long.parseLong(parser.getOffset()));
    parser.close();
  }

  /*
   TODO: investigate SDC-265
  @Test
  public void testParseMultipleStringObjects() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("\"Hello\"\"Bye\""), 1000, true);
    DataParser parser = new JsonDataParser(getContext(), "id", reader, 0, Mode.MULTIPLE_OBJECTS, 100);
    Assert.assertEquals(0, Long.parseLong(parser.getOffset())); // "HELLO""
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::0", record.getHeader().getSourceId());
    Assert.assertEquals("Hello", record.get().getValueAsString());
    Assert.assertEquals(7, Long.parseLong(parser.getOffset()));
    record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::7", record.getHeader().getSourceId());
    Assert.assertEquals("Bye", record.get().getValueAsString());
    Assert.assertEquals(12, Long.parseLong(parser.getOffset()));
    record = parser.parse();
    Assert.assertEquals(12, Long.parseLong(parser.getOffset()));
    Assert.assertNull(record);
    Assert.assertEquals(-1, Long.parseLong(parser.getOffset()));
    parser.close();
  }
   */

  @Test(expected = IOException.class)
  public void testClose() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("\"Hello\"\"Bye\""), 1000, true, false);
    DataParser parser = new JsonCharDataParser(getContext(), "id", reader, 0,
                                           Mode.MULTIPLE_OBJECTS, 10);
    parser.close();
    parser.parse();
  }

  @Test
  public void testParseArray() throws Exception {
    //TODO: investigate SDC-265, if the elements of the array are Strings the same issue shows up
    OverrunReader reader = new OverrunReader(new StringReader("[[\"Hello\"],[\"Bye\"]]"), 1000, true, false);
    DataParser parser = new JsonCharDataParser(getContext(), "id", reader, 0,
                                           Mode.ARRAY_OBJECTS, 10);
    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::0", record.getHeader().getSourceId());
    Assert.assertEquals("Hello", record.get().getValueAsList().get(0).getValueAsString());
    Assert.assertEquals(9, Long.parseLong(parser.getOffset()));
    record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::9", record.getHeader().getSourceId());
    Assert.assertEquals("Bye", record.get().getValueAsList().get(0).getValueAsString());
    Assert.assertEquals(17, Long.parseLong(parser.getOffset()));
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals(-1, Long.parseLong(parser.getOffset()));
    parser.close();
  }

  @Test
  public void testParseArrayWithConstants() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("[\"Hi\",true,null]"), 1000, true, false);
    DataParser parser = new JsonCharDataParser(getContext(), "id", reader, 0, Mode.ARRAY_OBJECTS, 10);
    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("Hi", record.get().getValueAsString());
    record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals(true, record.get().getValueAsBoolean());
    record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals(Field.Type.STRING, record.get().getType());
    Assert.assertEquals(null, record.get().getValueAsString());
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals(-1, Long.parseLong(parser.getOffset()));
    parser.close();
  }

  @Test
  public void testParseArrayWithOffset() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("[[\"Hello\"],[\"Bye\"]]"), 1000, true, false);
    DataParser parser = new JsonCharDataParser(getContext(), "id", reader, 9,
                                           Mode.ARRAY_OBJECTS, 10);
    Assert.assertEquals(9, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::9", record.getHeader().getSourceId());
    Assert.assertEquals("Bye", record.get().getValueAsList().get(0).getValueAsString());
    Assert.assertEquals(17, Long.parseLong(parser.getOffset()));
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals(-1, Long.parseLong(parser.getOffset()));
    parser.close();
  }

  @Test
  public void testParseCtrlCharFilter() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("[[\"He\0llo\"],[\"Bye\"]]"), 1000, true, true);
    DataParser parser = new JsonCharDataParser(getContext(), "id", reader, 9,
                                               Mode.ARRAY_OBJECTS, 10);
    Assert.assertEquals("9", parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::9", record.getHeader().getSourceId());
    Assert.assertEquals("Bye", record.get().getValueAsList().get(0).getValueAsString());
    Assert.assertEquals("17", parser.getOffset());
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals("-1", parser.getOffset());
    parser.close();
  }

  @Test(expected = IOException.class)
  public void testParseCtrlCharFilterFail() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("[[\"He\0llo\"],[\"Bye\"]]"), 1000, true, false);
    DataParser parser = new JsonCharDataParser(getContext(), "id", reader, 9,
                                               Mode.ARRAY_OBJECTS, 10);
    try {
      parser.parse();
    } finally {
      parser.close();
    }
  }

  @Test
  public void testParseCtrlCharFilterMultipleCtrls() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("{\"a\" :\0 \"foo\2ba\3r\4\"}"), 1000, true, true);
    DataParser parser = new JsonCharDataParser(getContext(), "id", reader, 0,
                                               Mode.MULTIPLE_OBJECTS, 1000);
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::0", record.getHeader().getSourceId());
    Assert.assertEquals("foobar", record.get().getValueAsMap().get("a").getValueAsString());
    record = parser.parse();
    Assert.assertNull(record);
    parser.close();
  }

  @Test
  public void testParseBigInteger() throws Exception {
    OverrunReader reader = new OverrunReader(
        new StringReader("{\"test\": 100000000000100000000000100000000000100000000000100000000000100000000000}"),
        1000,
        true,
        true
    );
    DataParser parser = new JsonCharDataParser(
        getContext(),
        "id",
        reader,
        0,
        Mode.MULTIPLE_OBJECTS, 1000
    );
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::0", record.getHeader().getSourceId());
    Assert.assertEquals(Field.Type.DECIMAL, record.get().getValueAsMap().get("test").getType());
    Assert.assertEquals(
        "100000000000100000000000100000000000100000000000100000000000100000000000",
        record.get().getValueAsMap().get("test").getValueAsString()
    );
    record = parser.parse();
    Assert.assertNull(record);
    parser.close();
  }

  @Test
  public void testParseStringConstant() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("\"str\""), 1000, true, false);
    DataParser parser = new JsonCharDataParser(getContext(), "id", reader, 0, Mode.MULTIPLE_OBJECTS, 100);
    Record record = parser.parse();
    Assert.assertNotNull(record);

    Assert.assertEquals(Field.Type.STRING, record.get().getType());
    Assert.assertEquals("str", record.get().getValueAsString());

    parser.close();
  }

  @Test
  public void testParseFalseConstant() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("false"), 1000, true, false);
    DataParser parser = new JsonCharDataParser(getContext(), "id", reader, 0, Mode.MULTIPLE_OBJECTS, 100);
    Record record = parser.parse();
    Assert.assertNotNull(record);

    Assert.assertEquals(Field.Type.BOOLEAN, record.get().getType());
    Assert.assertEquals(false, record.get().getValueAsBoolean());

    parser.close();
  }

  @Test
  public void testParseNullConstant() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("null"), 1000, true, false);
    DataParser parser = new JsonCharDataParser(getContext(), "id", reader, 0, Mode.MULTIPLE_OBJECTS, 100);
    Record record = parser.parse();
    Assert.assertNotNull(record);

    Assert.assertEquals(Field.Type.STRING, record.get().getType());
    Assert.assertEquals(null, record.get().getValueAsString());

    parser.close();
  }
}
