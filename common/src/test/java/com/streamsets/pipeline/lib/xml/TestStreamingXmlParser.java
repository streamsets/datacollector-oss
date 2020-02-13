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
package com.streamsets.pipeline.lib.xml;

import com.streamsets.pipeline.api.Field;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class TestStreamingXmlParser {

  private final String XML_RECORDS = "TestStreamingXmlParser-records.xml";
  private final String XML_COMPLEX_REC = "com/streamsets/pipeline/lib/xml/TestStreamingXmlParser-complex-records.xml";
  private final String XML_NAMESPACED = "com/streamsets/pipeline/lib/xml/TestStreamingXmlParser-namespaced-records.xml";
  private final String XML_WHITESPACES = "TestStreamingXmlParser-whitespaces.xml";
  private final String XML_DOC_AS_RECORD = "TestStreamingXmlParser-docAsRecord.xml";

  private final String XML_TEST_ELEMENT_ROOT = "root[1]/toplevel[3]/blargh[@theone='yes']/record";

  private Reader getXml(String name) throws Exception {
    return new InputStreamReader(Thread.currentThread().getContextClassLoader().getResourceAsStream(name));
  }

  @Test
  public void testParser() throws Exception {
    StreamingXmlParser parser = new StreamingXmlParserBuilder()
        .withReader(getXml(XML_RECORDS))
        .withRecordElement("record")
        .build();

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
    Assert.assertEquals(0, f.getValueAsMap().size());
    Assert.assertEquals("A", f.getAttribute(StreamingXmlParser.XMLATTR_ATTRIBUTE_PREFIX+"a"));

    f = parser.read();
    Assert.assertNotNull(f);
    Assert.assertEquals(1, f.getValueAsMap().size());
    Assert.assertEquals("y", f.getAttribute("xmlns:x"));
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
  public void testComplexInput() throws Exception {
    StreamingXmlParser parser = new StreamingXmlParserBuilder()
        .withReader(getXml(XML_COMPLEX_REC))
        .withRecordElement(XML_TEST_ELEMENT_ROOT)
        .build();

    Field f = parser.read();
    Assert.assertNotNull(f);
    Assert.assertTrue(f.getValueAsMap().isEmpty());

    f = parser.read();
    Assert.assertNotNull(f);
    Assert.assertEquals(1, f.getValueAsMap().size());
    Assert.assertEquals("r1", f.getValueAsMap().get("value").getValue());

    f = parser.read();
    Assert.assertNotNull(f);
    Assert.assertEquals(0, f.getValueAsMap().size());
    Assert.assertEquals("A", f.getAttribute(StreamingXmlParser.XMLATTR_ATTRIBUTE_PREFIX+"a"));

    f = parser.read();
    Assert.assertNotNull(f);
    Assert.assertEquals(1, f.getValueAsMap().size());
    Assert.assertEquals("y", f.getAttribute("xmlns:x"));
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
  public void testPositionPredicate() throws Exception {
    StreamingXmlParser parser = new StreamingXmlParserBuilder()
        .withReader(getXml(XML_COMPLEX_REC))
        .withRecordElement(XML_TEST_ELEMENT_ROOT + "[5]")
        .build();

    Field f = parser.read();
    Assert.assertNotNull(f);
    final Map<String, Field> values = f.getValueAsMap();
    Assert.assertFalse(values.isEmpty());
    Assert.assertEquals("a", values.get("name").getValueAsList().get(0).getValueAsMap().get("value")
        .getValueAsString());

    f = parser.read();
    Assert.assertNull(f);

    parser.close();
  }

  @Test
  public void testXPathWithNamespaces() throws Exception {
    Map<String, String> namespaces = new HashMap<>();
    namespaces.put("myns", "x");
    StreamingXmlParser parser = new StreamingXmlParserBuilder()
        .withReader(getXml(XML_NAMESPACED))
        .withRecordElement("myns:record")
        .withNamespaces(namespaces)
        .build();

    Field f = parser.read();
    Assert.assertNotNull(f);
    Assert.assertEquals("0", f.getValueAsMap().get("value").getValue());

    f = parser.read();
    Assert.assertNotNull(f);
    Assert.assertEquals("3", f.getValueAsMap().get("value").getValue());

    f = parser.read();
    Assert.assertNotNull(f);
    Assert.assertEquals("4", f.getValueAsMap().get("value").getValue());

    f = parser.read();
    Assert.assertNotNull(f);
    Assert.assertEquals("7", f.getValueAsMap().get("value").getValue());

    f = parser.read();
    Assert.assertNotNull(f);
    Assert.assertEquals("9", f.getValueAsMap().get("value").getValue());

    f = parser.read();
    Assert.assertNull(f);
    parser.close();
  }

  @Test
  public void testParserWithInitialPosition() throws Exception {
    StreamingXmlParserBuilder parserBuilder = new StreamingXmlParserBuilder()
        .withReader(getXml(XML_RECORDS))
        .withRecordElement("record");
    StreamingXmlParser parser = parserBuilder.build();

    parser.read();
    parser.read();
    parser.read();
    long pos = parser.getReaderPosition();
    parser.close();

    parser = new StreamingXmlParserBuilder()
        .withReader(getXml(XML_RECORDS))
        .withRecordElement("record")
        .withInitialPosition(pos)
        .withPreserveRootElement(false)
        .build();

    Field f = parser.read();
    Assert.assertNotNull(f);
    Assert.assertEquals(0, f.getValueAsMap().size());
    Assert.assertEquals("A", f.getAttribute(StreamingXmlParser.XMLATTR_ATTRIBUTE_PREFIX + "a"));

    f = parser.read();
    Assert.assertNotNull(f);
    Assert.assertEquals(1, f.getValueAsMap().size());
    Assert.assertEquals("y", f.getAttribute("xmlns:x"));
    Assert.assertEquals("r4", f.getValueAsMap().get(StreamingXmlParser.VALUE_KEY).getValue());

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
  public void testParserComplexInputAndInitialPosition() throws Exception {
    StreamingXmlParserBuilder parserBuilder = new StreamingXmlParserBuilder()
        .withReader(getXml(XML_COMPLEX_REC))
        .withRecordElement(XML_TEST_ELEMENT_ROOT);
    StreamingXmlParser parser = parserBuilder.build();

    parser.read();

    // Store the current stack
    LinkedList<String> elementNameStack = parser.elementNameStack;
    long pos = parser.getReaderPosition();

    // Instance new parser with initial position
    parser = new StreamingXmlParserBuilder()
        .withReader(getXml(XML_COMPLEX_REC))
        .withRecordElement(XML_TEST_ELEMENT_ROOT)
        .withInitialPosition(pos)
        .withPreserveRootElement(false)
        .build();

    Assert.assertEquals(elementNameStack, parser.elementNameStack);
  }

  @Test
  public void testParserFullDocumentAsRecord() throws Exception {
    StreamingXmlParser parser = new StreamingXmlParserBuilder()
        .withReader(getXml(XML_DOC_AS_RECORD))
        .build();

    Field f = parser.read();
    Assert.assertEquals(2, f.getValueAsMap().size());
    Assert.assertEquals(1, f.getValueAsMap().get("a").getValueAsList().size());
    Assert.assertEquals(1, f.getValueAsMap().get("b").getValueAsList().size());

    f = parser.read();
    Assert.assertNull(f);

    parser.close();
  }

  @Test
  public void testParserWithWhitespaces() throws Exception {
    StreamingXmlParser parser = new StreamingXmlParserBuilder()
        .withReader(getXml(XML_WHITESPACES))
        .build();

    Field f = parser.read();
    Assert.assertEquals(1, f.getValueAsMap().size());
    Map<String, Field> a = f.getValueAsMap().get("a").getValueAsList().get(0).getValueAsMap();
    Assert.assertEquals(2, a.size());

    Field b = a.get("b").getValueAsList().get(0);
    Assert.assertEquals("foo", b.getValueAsMap().get("value").getValueAsString());

    Field c = a.get("c").getValueAsList().get(0);
    Assert.assertNull(c.getValueAsMap().get("value"));
    f = parser.read();
    Assert.assertNull(f);

    parser.close();
  }

  @Test
  public void testParserPreserveRootElement() throws Exception {
    // No record element
    StreamingXmlParser parser = new StreamingXmlParserBuilder()
        .withReader(getXml(XML_COMPLEX_REC))
        .withPreserveRootElement(true)
        .build();

    Field field = parser.read();
    Assert.assertNotNull(field.getValueAsMap().get("root"));

    // With record element delimiter
    parser = new StreamingXmlParserBuilder()
        .withReader(getXml(XML_COMPLEX_REC))
        .withRecordElement(XML_TEST_ELEMENT_ROOT)
        .withPreserveRootElement(true)
        .build();

    field = parser.read();
    Assert.assertNotNull(field.getValueAsMap().get("record"));
  }
}
