/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.lib.json;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

public class TestStreamingJsonParser {

  private InputStream getJsonStream(String name) throws Exception {
    return Thread.currentThread().getContextClassLoader().getResourceAsStream(name);
  }

  // Array of Maps

  @Test
  public void testArrayOfMaps() throws Exception {
    StreamingJsonParser parser = new StreamingJsonParser(getJsonStream("TestStreamingJsonParser-arrayOfMaps.json"),
                                                         StreamingJsonParser.Mode.ARRAY_OBJECTS);
    Map m1 = parser.readMap();
    Assert.assertNotNull(m1);
    Map m2 = parser.readMap();
    Assert.assertNotNull(m2);
    Assert.assertNull(parser.readMap());
    Assert.assertEquals(ImmutableMap.of("a", "A"), m1);
    Assert.assertEquals(ImmutableMap.of("b", "B"), m2);
  }

  @Test(expected = IOException.class)
  public void testArrayOfMapsInvalid1() throws Exception {
    StreamingJsonParser parser = new StreamingJsonParser(getJsonStream("TestStreamingJsonParser-arrayOfArrays.json"),
                                                         StreamingJsonParser.Mode.ARRAY_OBJECTS);
    parser.readMap();
  }

  @Test(expected = IOException.class)
  public void testArrayOfMapsInvalid2() throws Exception {
    StreamingJsonParser parser = new StreamingJsonParser(getJsonStream("TestStreamingJsonParser-multipleArrays.json"),
                                                         StreamingJsonParser.Mode.ARRAY_OBJECTS);
    parser.readMap();
  }

  @Test(expected = IOException.class)
  public void testArrayOfMapsInvalid3() throws Exception {
    StreamingJsonParser parser = new StreamingJsonParser(getJsonStream("TestStreamingJsonParser-multipleMaps.json"),
                                                         StreamingJsonParser.Mode.ARRAY_OBJECTS);
    parser.readMap();
  }

  // Array of arrays

  @Test
  public void testArrayOfArrays() throws Exception {
    StreamingJsonParser parser = new StreamingJsonParser(getJsonStream("TestStreamingJsonParser-arrayOfArrays.json"),
                                                         StreamingJsonParser.Mode.ARRAY_OBJECTS);
    List a1 = parser.readList();
    Assert.assertNotNull(a1);
    List a2 = parser.readList();
    Assert.assertNotNull(a2);
    Assert.assertNull(parser.readList());
    Assert.assertEquals(ImmutableList.of("a", "A"), a1);
    Assert.assertEquals(ImmutableList.of("b", "B"), a2);
  }

  @Test(expected = IOException.class)
  public void testArrayOfArraysInvalid1() throws Exception {
    StreamingJsonParser parser = new StreamingJsonParser(getJsonStream("TestStreamingJsonParser-arrayOfMaps.json"),
                                                         StreamingJsonParser.Mode.ARRAY_OBJECTS);
    parser.readList();
  }

  @Test(expected = IOException.class)
  public void testArrayOfArraysInvalid2() throws Exception {
    StreamingJsonParser parser = new StreamingJsonParser(getJsonStream("TestStreamingJsonParser-multipleArrays.json"),
                                                         StreamingJsonParser.Mode.ARRAY_OBJECTS);
    parser.readList();
  }

  @Test(expected = IOException.class)
  public void testArrayOfArraysInvalid3() throws Exception {
    StreamingJsonParser parser = new StreamingJsonParser(getJsonStream("TestStreamingJsonParser-multipleMaps.json"),
                                                         StreamingJsonParser.Mode.ARRAY_OBJECTS);
    parser.readList();
  }

  // Multiple Maps

  @Test
  public void testMultipleMaps() throws Exception {
    StreamingJsonParser parser = new StreamingJsonParser(getJsonStream("TestStreamingJsonParser-multipleMaps.json"),
                                                         StreamingJsonParser.Mode.MULTIPLE_OBJECTS);
    Map m1 = parser.readMap();
    Assert.assertNotNull(m1);
    Map m2 = parser.readMap();
    Assert.assertNotNull(m2);
    Assert.assertNull(parser.readMap());
    Assert.assertEquals(ImmutableMap.of("a", "A"), m1);
    Assert.assertEquals(ImmutableMap.of("b", "B"), m2);
  }

  @Test(expected = IOException.class)
  public void testMultipleMapsInvalid1() throws Exception {
    StreamingJsonParser parser = new StreamingJsonParser(getJsonStream("TestStreamingJsonParser-arrayOfArrays.json"),
                                                         StreamingJsonParser.Mode.MULTIPLE_OBJECTS);
    parser.readMap();
  }

  @Test(expected = IOException.class)
  public void testMultipleMapsInvalid2() throws Exception {
    StreamingJsonParser parser = new StreamingJsonParser(getJsonStream("TestStreamingJsonParser-multipleArrays.json"),
                                                         StreamingJsonParser.Mode.MULTIPLE_OBJECTS);
    parser.readMap();
  }

  @Test(expected = IOException.class)
  public void testMultipleMapsInvalid3() throws Exception {
    StreamingJsonParser parser = new StreamingJsonParser(getJsonStream("TestStreamingJsonParser-arrayOfMaps.json"),
                                                         StreamingJsonParser.Mode.MULTIPLE_OBJECTS);
    parser.readMap();
  }


  // Multiple array

  @Test
  public void testMultipleArrays() throws Exception {
    StreamingJsonParser parser = new StreamingJsonParser(getJsonStream("TestStreamingJsonParser-multipleArrays.json"),
                                                         StreamingJsonParser.Mode.MULTIPLE_OBJECTS);
    List a1 = parser.readList();
    Assert.assertNotNull(a1);
    List a2 = parser.readList();
    Assert.assertNotNull(a2);
    Assert.assertNull(parser.readList());
    Assert.assertEquals(ImmutableList.of("a", "A"), a1);
    Assert.assertEquals(ImmutableList.of("b", "B"), a2);
  }

  @Test(expected = IOException.class)
  public void testMultipleArraysInvalid2() throws Exception {
    StreamingJsonParser parser = new StreamingJsonParser(getJsonStream("TestStreamingJsonParser-multipleMaps.json"),
                                                         StreamingJsonParser.Mode.MULTIPLE_OBJECTS);
    parser.readList();
  }

  // cannot test array of arrays negative test because it cannot be differentiated from a single array file
  // cannot test array of maps negative test because it cannot be differentiated from a single array file

}
