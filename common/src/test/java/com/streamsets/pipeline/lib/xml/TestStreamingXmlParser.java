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
package com.streamsets.pipeline.lib.xml;

import com.streamsets.pipeline.api.Field;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;
import java.util.Map;

public class TestStreamingXmlParser {

  private Reader getXml(String name) throws Exception {
    return new InputStreamReader(Thread.currentThread().getContextClassLoader().getResourceAsStream(name));
  }

  @Test
  public void testParser() throws Exception {
    StreamingXmlParser parser = new StreamingXmlParser(getXml("TestStreamingXmlParser-records.xml"), "record" );

    Field f = parser.read();
    Assert.assertNotNull(f);
    Assert.assertTrue(f.getValueAsMap().isEmpty());

    f = parser.read();
    Assert.assertNotNull(f);
    Assert.assertEquals(1, f.getValueAsMap().size());
    Assert.assertEquals("r1", f.getValueAsMap().get("value").getValue());

    f = parser.read();
    Assert.assertNotNull(f);
    Assert.assertEquals(1, f.getValueAsMap().size());
    Assert.assertEquals("r2", f.getValueAsMap().get("value").getValue());

    f = parser.read();
    Assert.assertNotNull(f);
    Assert.assertEquals(1, f.getValueAsMap().size());
    Assert.assertEquals("A", f.getValueAsMap().get("attr|a").getValue());

    f = parser.read();
    Assert.assertNotNull(f);
    Assert.assertEquals(2, f.getValueAsMap().size());
    Assert.assertEquals("y", f.getValueAsMap().get("ns|xmlns:x").getValue());
    Assert.assertEquals("r4", f.getValueAsMap().get("value").getValue());

    f = parser.read();
    Assert.assertNotNull(f);
    Assert.assertEquals(1, f.getValueAsMap().size());
    Assert.assertEquals(1, f.getValueAsMap().get("name").getValueAsList().size());
    Assert.assertEquals(1, f.getValueAsMap().get("name").getValueAsList().get(0).getValueAsMap().size());
    Assert.assertEquals("a", f.getValueAsMap().get("name").getValueAsList().get(0).getValueAsMap().get("value").getValue());

    f = parser.read();
    Assert.assertNotNull(f);
    Assert.assertEquals(2, f.getValueAsMap().size());
    Assert.assertEquals(1, f.getValueAsMap().get("name").getValueAsList().size());
    Assert.assertEquals(1, f.getValueAsMap().get("name").getValueAsList().get(0).getValueAsMap().size());
    Assert.assertEquals("b", f.getValueAsMap().get("name").getValueAsList().get(0).getValueAsMap().get("value").getValue());
    Assert.assertEquals(1, f.getValueAsMap().get("data").getValueAsList().size());
    Map<String, Field> data = f.getValueAsMap().get("data").getValueAsList().get(0).getValueAsMap();
    Assert.assertEquals(1, data.size());
    List<Field> values = data.get("value").getValueAsList();
    Assert.assertEquals(2, values.size());
    Assert.assertEquals(1, values.get(0).getValueAsMap().size());
    Assert.assertEquals("0", values.get(0).getValueAsMap().get("value").getValue());
    Assert.assertEquals(1, values.get(1).getValueAsMap().size());
    Assert.assertEquals("1", values.get(1).getValueAsMap().get("value").getValue());

    f = parser.read();
    Assert.assertNotNull(f);
    Assert.assertEquals(1, f.getValueAsMap().size());
    Assert.assertEquals("foobar", f.getValueAsMap().get("value").getValue());

    f = parser.read();
    Assert.assertNull(f);

    parser.close();
  }

  @Test
  public void testParserWithInitialPosition() throws Exception {
    StreamingXmlParser parser = new StreamingXmlParser(getXml("TestStreamingXmlParser-records.xml"), "record" );

    parser.read();
    parser.read();
    parser.read();
    long pos = parser.getReaderPosition();
    parser.close();

    parser = new StreamingXmlParser(getXml("TestStreamingXmlParser-records.xml"), "record", pos);
    Field f = parser.read();
    Assert.assertNotNull(f);
    Assert.assertEquals(1, f.getValueAsMap().size());
    Assert.assertEquals("A", f.getValueAsMap().get("attr|a").getValue());

    f = parser.read();
    Assert.assertNotNull(f);
    Assert.assertEquals(2, f.getValueAsMap().size());
    Assert.assertEquals("y", f.getValueAsMap().get("ns|xmlns:x").getValue());
    Assert.assertEquals("r4", f.getValueAsMap().get("value").getValue());

    f = parser.read();
    Assert.assertNotNull(f);
    Assert.assertEquals(1, f.getValueAsMap().size());
    Assert.assertEquals(1, f.getValueAsMap().get("name").getValueAsList().size());
    Assert.assertEquals(1, f.getValueAsMap().get("name").getValueAsList().get(0).getValueAsMap().size());
    Assert.assertEquals("a", f.getValueAsMap().get("name").getValueAsList().get(0).getValueAsMap().get("value").getValue());

    f = parser.read();
    Assert.assertNotNull(f);
    Assert.assertEquals(2, f.getValueAsMap().size());
    Assert.assertEquals(1, f.getValueAsMap().get("name").getValueAsList().size());
    Assert.assertEquals(1, f.getValueAsMap().get("name").getValueAsList().get(0).getValueAsMap().size());
    Assert.assertEquals("b", f.getValueAsMap().get("name").getValueAsList().get(0).getValueAsMap().get("value").getValue());
    Assert.assertEquals(1, f.getValueAsMap().get("data").getValueAsList().size());
    Map<String, Field> data = f.getValueAsMap().get("data").getValueAsList().get(0).getValueAsMap();
    Assert.assertEquals(1, data.size());
    List<Field> values = data.get("value").getValueAsList();
    Assert.assertEquals(2, values.size());
    Assert.assertEquals(1, values.get(0).getValueAsMap().size());
    Assert.assertEquals("0", values.get(0).getValueAsMap().get("value").getValue());
    Assert.assertEquals(1, values.get(1).getValueAsMap().size());
    Assert.assertEquals("1", values.get(1).getValueAsMap().get("value").getValue());

    f = parser.read();
    Assert.assertNotNull(f);
    Assert.assertEquals(1, f.getValueAsMap().size());
    Assert.assertEquals("foobar", f.getValueAsMap().get("value").getValue());

    f = parser.read();
    Assert.assertNull(f);
    parser.close();
  }

  @Test
  public void testParserFullDocumentAsRecord() throws Exception {
    StreamingXmlParser parser = new StreamingXmlParser(getXml("TestStreamingXmlParser-docAsRecord.xml"));
    Field f = parser.read();
    Assert.assertEquals(2, f.getValueAsMap().size());
    Assert.assertEquals(1, f.getValueAsMap().get("a").getValueAsList().size());
    Assert.assertEquals(1, f.getValueAsMap().get("b").getValueAsList().size());
    f = parser.read();
    Assert.assertNull(f);
    parser.close();
  }

}
