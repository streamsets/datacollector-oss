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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ext.io.OverrunException;
import com.streamsets.pipeline.lib.io.OverrunInputStream;
import com.streamsets.pipeline.api.ext.json.Mode;
import com.streamsets.pipeline.lib.json.StreamingJsonParser;
import com.streamsets.pipeline.lib.json.StreamingJsonParserImpl;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TestStreamingJsonParser {
  private static final Logger LOG = LoggerFactory.getLogger(TestStreamingJsonParser.class);

  private Reader getJsonReader(String name) throws Exception {
    return new InputStreamReader(Resources.getResource(name).openStream());
  }

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR,
        Collections.<String>emptyList());
  }

  @Test
  public void testIncorrectJSONMode() throws Exception {
    Reader reader = new InputStreamReader(
        new OverrunInputStream(
            Resources.getResource("TestStreamingJsonParser-testIncorrectJSONMode.json").openStream(),
            128, true
        )
    );
    StreamingJsonParser parser = new StreamingJsonParserImpl(getContext(), reader, Mode.MULTIPLE_OBJECTS);
    try {
      parser.read();
      Assert.fail();
    } catch (OverrunException e) {
      Assert.assertEquals(164, e.getStreamOffset());
    } finally {
      parser.close();
    }
  }

  // Array of Maps

  @Test
  public void testArrayOfMaps() throws Exception {
    StreamingJsonParser parser = new StreamingJsonParserImpl(getContext(), getJsonReader("TestStreamingJsonParser-arrayOfMaps.json"),
                                                         Mode.ARRAY_OBJECTS);
    Map m1 = (Map) parser.read();
    Assert.assertNotNull(m1);
    Map m2 = (Map) parser.read();
    Assert.assertNotNull(m2);
    Assert.assertNull(parser.read());
    Assert.assertEquals(ImmutableMap.of("a", "A"), m1);
    Assert.assertEquals(ImmutableMap.of("b", "B"), m2);
  }

  // Array of arrays

  @Test
  public void testArrayOfArrays() throws Exception {
    StreamingJsonParser parser = new StreamingJsonParserImpl(getContext(), getJsonReader("TestStreamingJsonParser-arrayOfArrays.json"),
                                                         Mode.ARRAY_OBJECTS);
    List a1 = (List) parser.read();
    Assert.assertNotNull(a1);
    List a2 = (List) parser.read();
    Assert.assertNotNull(a2);
    Assert.assertNull(parser.read());
    Assert.assertEquals(ImmutableList.of("a", "A"), a1);
    Assert.assertEquals(ImmutableList.of("b", "B"), a2);
  }

  // Multiple Maps

  @Test
  public void testMultipleMaps() throws Exception {
    StreamingJsonParser parser = new StreamingJsonParserImpl(getContext(), getJsonReader("TestStreamingJsonParser-multipleMaps.json"),
                                                         Mode.MULTIPLE_OBJECTS);
    Map m1 = (Map) parser.read();
    Assert.assertNotNull(m1);
    Map m2 = (Map) parser.read();
    Assert.assertNotNull(m2);
    Assert.assertNull(parser.read());
    Assert.assertEquals(ImmutableMap.of("a", "A"), m1);
    Assert.assertEquals(ImmutableMap.of("b", "B"), m2);
  }

  // Multiple array

  @Test
  public void testMultipleArrays() throws Exception {
    StreamingJsonParser parser = new StreamingJsonParserImpl(getContext(), getJsonReader("TestStreamingJsonParser-multipleArrays.json"),
                                                         Mode.MULTIPLE_OBJECTS);
    List a1 = (List) parser.read();
    Assert.assertNotNull(a1);
    List a2 = (List) parser.read();
    Assert.assertNotNull(a2);
    Assert.assertNull(parser.read());
    Assert.assertEquals(ImmutableList.of("a", "A"), a1);
    Assert.assertEquals(ImmutableList.of("b", "B"), a2);
  }

  @Test
  public void testArrayPositionable() throws Exception {
    StreamingJsonParser parser = new StreamingJsonParserImpl(getContext(), getJsonReader("TestStreamingJsonParser-arrayOfMaps.json"),
                                                         Mode.ARRAY_OBJECTS);
    Map m1 = (Map) parser.read();
    long firstObjectPos = parser.getReaderPosition();
    Assert.assertNotNull(m1);
    Map m2 = (Map) parser.read();
    long secondObjectPos = parser.getReaderPosition();
    Assert.assertNotNull(m1);
    Assert.assertNotNull(m2);
    long lastObjectPos = parser.getReaderPosition();
    Assert.assertNotNull(m1);
    Assert.assertNull(parser.read());
    long endPos = parser.getReaderPosition();
    Assert.assertNotNull(m1);

    parser = new StreamingJsonParserImpl(getContext(), getJsonReader("TestStreamingJsonParser-arrayOfMaps.json"), firstObjectPos,
                                     Mode.ARRAY_OBJECTS);
    Assert.assertEquals(firstObjectPos, parser.getReaderPosition());
    Map m2a = (Map) parser.read();
    Assert.assertEquals(secondObjectPos, parser.getReaderPosition());
    Assert.assertEquals(m2, m2a);
    Assert.assertEquals(lastObjectPos, parser.getReaderPosition());
    Assert.assertNull(parser.read());
    Assert.assertEquals(endPos, parser.getReaderPosition());

    parser = new StreamingJsonParserImpl(getContext(), getJsonReader("TestStreamingJsonParser-arrayOfMaps.json"), secondObjectPos,
                                     Mode.ARRAY_OBJECTS);
    Assert.assertEquals(secondObjectPos, parser.getReaderPosition());
    Assert.assertEquals(m2, m2a);
    Assert.assertEquals(lastObjectPos, parser.getReaderPosition());
    Assert.assertNull(parser.read());
    Assert.assertEquals(endPos, parser.getReaderPosition());

    parser = new StreamingJsonParserImpl(getContext(), getJsonReader("TestStreamingJsonParser-arrayOfMaps.json"), lastObjectPos,
                                     Mode.ARRAY_OBJECTS);
    Assert.assertEquals(lastObjectPos, parser.getReaderPosition());
    Assert.assertNull(parser.read());
    Assert.assertEquals(endPos, parser.getReaderPosition());

    parser = new StreamingJsonParserImpl(getContext(), getJsonReader("TestStreamingJsonParser-arrayOfMaps.json"), endPos,
                                     Mode.ARRAY_OBJECTS);
    Assert.assertEquals(endPos, parser.getReaderPosition());
    Assert.assertNull(parser.read());
    Assert.assertEquals(endPos, parser.getReaderPosition());
  }

  @Test
  public void testMultipleObjectsPositionable() throws Exception {
    StreamingJsonParser parser = new StreamingJsonParserImpl(getContext(), getJsonReader("TestStreamingJsonParser-multipleMaps.json"),
                                                         Mode.MULTIPLE_OBJECTS);
    Map m1 = (Map) parser.read();
    long firstObjectPos = parser.getReaderPosition();
    Assert.assertNotNull(m1);
    Map m2 = (Map) parser.read();
    long secondObjectPos = parser.getReaderPosition();
    Assert.assertNotNull(m1);
    Assert.assertNotNull(m2);
    long lastObjectPos = parser.getReaderPosition();
    Assert.assertNotNull(m1);

    parser = new StreamingJsonParserImpl(getContext(), getJsonReader("TestStreamingJsonParser-multipleMaps.json"), firstObjectPos,
                                                         Mode.MULTIPLE_OBJECTS);
    Assert.assertEquals(firstObjectPos, parser.getReaderPosition());
    Map m2a = (Map) parser.read();
    Assert.assertEquals(secondObjectPos, parser.getReaderPosition());
    Assert.assertEquals(m2, m2a);
    Assert.assertEquals(lastObjectPos, parser.getReaderPosition());
    Assert.assertNull(parser.read());

    parser = new StreamingJsonParserImpl(getContext(), getJsonReader("TestStreamingJsonParser-multipleMaps.json"), secondObjectPos,
                                     Mode.MULTIPLE_OBJECTS);
    Assert.assertEquals(secondObjectPos, parser.getReaderPosition());
    Assert.assertEquals(m2, m2a);
    Assert.assertEquals(lastObjectPos, parser.getReaderPosition());
    Assert.assertNull(parser.read());

    parser = new StreamingJsonParserImpl(getContext(), getJsonReader("TestStreamingJsonParser-multipleMaps.json"), lastObjectPos,
                                     Mode.MULTIPLE_OBJECTS);
    Assert.assertEquals(lastObjectPos, parser.getReaderPosition());
    Assert.assertNull(parser.read());
  }

}
