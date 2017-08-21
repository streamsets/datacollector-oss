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
package com.streamsets.pipeline.lib.generator.xml;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.glassfish.jersey.internal.util.Base64;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.transform.OutputKeys;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TestXmlCharDataGenerator {

  private static final String SCHEMA = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                                       "<xs:schema xmlns:xs=\"http://www.w3.org/2001/XMLSchema\">\n" +
                                       "\t<xs:element name=\"root\" type=\"xs:string\"/>\n" +
                                       "</xs:schema>";

  @Test
  public void testConstructor() throws Exception {
    StringWriter writer = new StringWriter();
    XmlCharDataGenerator generator = new XmlCharDataGenerator(writer, false, ImmutableList.of(), true);
    Assert.assertFalse(generator.isSchemaValidation());
    Assert.assertNull(generator.getSchema());
    Assert.assertTrue(generator.isPrettyFormat());

    generator = new XmlCharDataGenerator(writer, true, ImmutableList.of(SCHEMA), false);
    Assert.assertTrue(generator.isSchemaValidation());
    Assert.assertNotNull(generator.getSchema());
    Assert.assertFalse(generator.isPrettyFormat());
  }

  @Test
  public void testSupportMethods() throws Exception {
    StringWriter writer = new StringWriter();
    XmlCharDataGenerator generator = new XmlCharDataGenerator(writer, true, ImmutableList.of(SCHEMA), true);

    Assert.assertNotNull(generator.createSchema(ImmutableList.of(SCHEMA)));
    Assert.assertNotNull(generator.getSchema());

    Assert.assertNotNull(generator.createDocumentBuilder());
    Assert.assertNotNull(generator.getDocumentBuilder());

    Assert.assertNotNull(generator.getTransformerFactory());
    Assert.assertNotNull(generator.createTransformer());
    Assert.assertEquals("yes", generator.createTransformer().getOutputProperty(OutputKeys.INDENT));
    Assert.assertEquals("2",
        generator.createTransformer().getOutputProperty("{http://xml.apache.org/xslt}indent-amount")
    );

    Assert.assertFalse(generator.isMap(null));
    Assert.assertFalse(generator.isMap(Field.create(0)));
    Assert.assertTrue(generator.isMap(Field.create(new HashMap<>())));
    Assert.assertTrue(generator.isMap(Field.createListMap(new LinkedHashMap<>())));

    Assert.assertFalse(generator.isList(null));
    Assert.assertFalse(generator.isList(Field.create(0)));
    Assert.assertTrue(generator.isList(Field.create(new ArrayList<>())));

  }

  @Test
  public void testValidateOK() throws Exception {
    StringWriter writer = new StringWriter();
    XmlCharDataGenerator generator = new XmlCharDataGenerator(writer, true, ImmutableList.of(SCHEMA), true);

    Document doc = generator.getDocumentBuilder().newDocument();
    Element e = doc.createElementNS("", "root");
    doc.appendChild(e);

    generator.validate(doc);
  }

  @Test
  public void testXmlWrite() throws Exception {
    StringWriter writer = new StringWriter();
    XmlCharDataGenerator generator = new XmlCharDataGenerator(writer, true, ImmutableList.of(SCHEMA), true);

    Document doc = generator.getDocumentBuilder().newDocument();
    Element e = doc.createElementNS("", "root");
    doc.appendChild(e);

    generator.xmlWrite(doc);
    generator.close();
    Assert.assertTrue(writer.toString().trim().startsWith("<?xml"));
    Assert.assertTrue(writer.toString().trim().endsWith("<root/>"));
  }

  @Test(expected = DataGeneratorException.class)
  public void testValidateFail() throws DataGeneratorException, IOException {
    StringWriter writer = new StringWriter();
    XmlCharDataGenerator generator = new XmlCharDataGenerator(writer, true, ImmutableList.of(SCHEMA), true);

    Document doc = generator.getDocumentBuilder().newDocument();
    Element e = doc.createElementNS("", "invalid");
    doc.appendChild(e);

    generator.validate(doc);
  }

  @Test
  public void testFlushClose() throws IOException {
    Writer writer = Mockito.mock(Writer.class);
    XmlCharDataGenerator generator = new XmlCharDataGenerator(writer, true, ImmutableList.of(SCHEMA), true);

    generator.flush();
    Mockito.verify(writer, Mockito.times(1)).flush();

    generator.close();
    Mockito.verify(writer, Mockito.times(1)).close();
  }

  @Test
  public void testWrite() throws Exception {
    Writer writer = Mockito.mock(Writer.class);
    XmlCharDataGenerator generator = new XmlCharDataGenerator(writer, true, ImmutableList.of(SCHEMA), true);
    generator = Mockito.spy(generator);

    Record record = Mockito.mock(Record.class);
    Document document = Mockito.mock(Document.class);
    Mockito.doReturn(document).when(generator).recordToXmlDocument(Mockito.eq(record));

    Mockito.doReturn(false).when(generator).isSchemaValidation();
    Mockito.doNothing().when(generator).xmlWrite(Mockito.eq(document));

    generator.write(record);

    Mockito.verify(generator, Mockito.never()).validate(Mockito.eq(document));
    Mockito.verify(generator, Mockito.times(1)).xmlWrite(Mockito.eq(document));
  }

  @Test
  public void testGetPrefixGetName() throws IOException {
    Writer writer = Mockito.mock(Writer.class);
    XmlCharDataGenerator generator = new XmlCharDataGenerator(writer, true, ImmutableList.of(SCHEMA), true);

    Assert.assertEquals("", generator.getPrefix("x"));
    Assert.assertEquals("p", generator.getPrefix("p:x"));

    Assert.assertEquals("x", generator.getName("x"));
    Assert.assertEquals("x", generator.getName("p:x"));
  }

  @Test
  public void testGetAttributes() throws IOException {
    Writer writer = Mockito.mock(Writer.class);
    XmlCharDataGenerator generator = new XmlCharDataGenerator(writer, true, ImmutableList.of(SCHEMA), true);

    Field field = Field.create(0);
    Assert.assertEquals(Collections.emptyMap(), generator.getAttributes(field));

    field.setAttribute("xmlns", "uri");
    field.setAttribute("xmlns:foo", "uri:foo");
    Assert.assertEquals(Collections.emptyMap(), generator.getAttributes(field));

    field.setAttribute("a", "A");
    Assert.assertEquals(ImmutableMap.of("a", "A"), generator.getAttributes(field));
  }

  @Test
  public void testGetNamespaces() throws IOException {
    Writer writer = Mockito.mock(Writer.class);
    XmlCharDataGenerator generator = new XmlCharDataGenerator(writer, true, ImmutableList.of(SCHEMA), true);

    Map<String, String> existingNamespaces = ImmutableMap.of("ns1", "NS1");

    Field field = Field.create(0);
    field.setAttribute("xmlns:ns1", "fNS1");
    field.setAttribute("xmlns:ns2", "NS2");
    field.setAttribute("a", "A");

    Map<String, String> expected = ImmutableMap.of("ns1", "fNS1", "ns2", "NS2");

    Map<String, String> got = generator.getNamespaces(existingNamespaces, field);

    Assert.assertEquals(expected, got);
  }

  @Test
  public void testCreateElement() throws DataGeneratorException, IOException {
    Writer writer = Mockito.mock(Writer.class);
    XmlCharDataGenerator generator = new XmlCharDataGenerator(writer, true, ImmutableList.of(SCHEMA), true);
    Document document = generator.getDocumentBuilder().newDocument();

    String name = "element";
    String namespace = "";
    Map<String, String> namespaces = ImmutableMap.of("", "", "ns", "NS");
    Field field = Field.create("data");

    Element element = generator.createElement(document, namespaces, name, namespace, field);
    Assert.assertEquals("element", element.getNodeName());
    Assert.assertEquals(null, element.getPrefix());
    Assert.assertEquals(null, element.getNamespaceURI());
    Assert.assertEquals(0, element.getChildNodes().getLength());
    Assert.assertEquals(0, element.getAttributes().getLength());

    name = "ns:element";
    namespace = "NS";
    element = generator.createElement(document, namespaces, name, namespace, field);
    Assert.assertEquals("ns:element", element.getNodeName());
    Assert.assertEquals("ns", element.getPrefix());
    Assert.assertEquals("NS", element.getNamespaceURI());
    Assert.assertEquals(0, element.getChildNodes().getLength());
    Assert.assertEquals(0, element.getAttributes().getLength());

    field.setAttribute("a", "A");
    field.setAttribute("ns:b", "B");
    field.setAttribute("xmlns", "NS1");
    field.setAttribute("xmlns:ns2", "NS2");
    element = generator.createElement(document, namespaces, name, namespace, field);
    Assert.assertEquals(2, element.getAttributes().getLength());
    Assert.assertEquals("a", element.getAttributes().item(0).getNodeName());
    Assert.assertEquals(null, element.getAttributes().item(0).getNamespaceURI());
    Assert.assertEquals("A", element.getAttributes().item(0).getNodeValue());
    Assert.assertEquals("ns:b", element.getAttributes().item(1).getNodeName());
    Assert.assertEquals("NS", element.getAttributes().item(1).getNamespaceURI());
    Assert.assertEquals("B", element.getAttributes().item(1).getNodeValue());
  }

  @Test
  public void testFieldToXmlElementsFieldNull() throws IOException, DataGeneratorException {
    Writer writer = Mockito.mock(Writer.class);
    XmlCharDataGenerator generator = new XmlCharDataGenerator(writer, true, ImmutableList.of(SCHEMA), true);
    Document document = generator.getDocumentBuilder().newDocument();

    String name = "element";
    Map<String, String> namespaces = ImmutableMap.of("", "", "ns", "NS");
    Field field = null;

    List<Element> elements = generator.fieldToXmlElements(document, namespaces, name, field);
    Assert.assertEquals(1, elements.size());
    Element element = elements.get(0);
    Assert.assertEquals("element", element.getNodeName());
    Assert.assertEquals(null, element.getNamespaceURI());
    Assert.assertEquals(0, element.getChildNodes().getLength());
    Assert.assertEquals(0, element.getAttributes().getLength());
  }

  private void testFieldToXmlElementsFieldMap(Field field) throws IOException, DataGeneratorException {
    Writer writer = Mockito.mock(Writer.class);
    XmlCharDataGenerator generator = new XmlCharDataGenerator(writer, true, ImmutableList.of(SCHEMA), true);
    Document document = generator.getDocumentBuilder().newDocument();

    String name = "map";
    Map<String, String> namespaces = ImmutableMap.of("", "", "ns", "NS");

    List<Element> elements = generator.fieldToXmlElements(document, namespaces, name, field);
    Assert.assertEquals(1, elements.size());
    Element element = elements.get(0);
    Assert.assertEquals("map", element.getNodeName());
    Assert.assertEquals(null, element.getNamespaceURI());
    Assert.assertEquals(1, element.getChildNodes().getLength());
    Assert.assertEquals("entry", element.getChildNodes().item(0).getNodeName());
    Assert.assertEquals("data", element.getChildNodes().item(0).getTextContent());
  }

  @Test
  public void testFieldToXmlElementsFieldMap() throws IOException, DataGeneratorException {
    Map<String, Field> map = ImmutableMap.of("entry", Field.create("data"));
    Field field = Field.create(map);
    testFieldToXmlElementsFieldMap(field);
  }

  @Test
  public void testFieldToXmlElementsFieldListMap() throws IOException, DataGeneratorException {
    Map<String, Field> map = ImmutableMap.of("entry", Field.create("data"));
    Field field = Field.createListMap(new LinkedHashMap<>(map));
    testFieldToXmlElementsFieldMap(field);
  }

  @Test
  public void testFieldToXmlElementsFieldList() throws IOException, DataGeneratorException {
    Writer writer = Mockito.mock(Writer.class);
    XmlCharDataGenerator generator = new XmlCharDataGenerator(writer, true, ImmutableList.of(SCHEMA), true);
    Document document = generator.getDocumentBuilder().newDocument();

    String name = "element";
    Map<String, String> namespaces = ImmutableMap.of("", "", "ns", "NS");
    List<Field> list = ImmutableList.of(Field.create("data1"), Field.create("data2"));
    Field field = Field.create(list);

    List<Element> elements = generator.fieldToXmlElements(document, namespaces, name, field);
    Assert.assertEquals(2, elements.size());
    Element element = elements.get(0);
    Assert.assertEquals("element", element.getNodeName());
    Assert.assertEquals(null, element.getNamespaceURI());
    Assert.assertEquals(1, element.getChildNodes().getLength());
    Assert.assertEquals("data1", element.getTextContent());
    element = elements.get(1);
    Assert.assertEquals("element", element.getNodeName());
    Assert.assertEquals(null, element.getNamespaceURI());
    Assert.assertEquals(1, element.getChildNodes().getLength());
    Assert.assertEquals("data2", element.getTextContent());
  }

  @Test
  public void testFieldToXmlElementsFieldBasic() throws IOException, DataGeneratorException {
    Writer writer = Mockito.mock(Writer.class);
    XmlCharDataGenerator generator = new XmlCharDataGenerator(writer, true, ImmutableList.of(SCHEMA), true);
    Document document = generator.getDocumentBuilder().newDocument();

    String name = "element";
    Map<String, String> namespaces = ImmutableMap.of("", "", "ns", "NS");
    Field field = Field.create("data");

    List<Element> elements = generator.fieldToXmlElements(document, namespaces, name, field);
    Assert.assertEquals(1, elements.size());
    Element element = elements.get(0);
    Assert.assertEquals("element", element.getNodeName());
    Assert.assertEquals(null, element.getNamespaceURI());
    Assert.assertEquals(1, element.getChildNodes().getLength());
    Assert.assertEquals("data", element.getTextContent());
  }

  @Test
  public void testFieldToXmlElementsFieldBasicBinary() throws IOException, DataGeneratorException {
    Writer writer = Mockito.mock(Writer.class);
    XmlCharDataGenerator generator = new XmlCharDataGenerator(writer, true, ImmutableList.of(SCHEMA), true);
    Document document = generator.getDocumentBuilder().newDocument();

    String name = "element";
    Map<String, String> namespaces = ImmutableMap.of("", "", "ns", "NS");
    Field field = Field.create(new byte[]{0, 1, 2});

    List<Element> elements = generator.fieldToXmlElements(document, namespaces, name, field);
    Assert.assertEquals(1, elements.size());
    Element element = elements.get(0);
    Assert.assertEquals("element", element.getNodeName());
    Assert.assertEquals(null, element.getNamespaceURI());
    Assert.assertEquals(1, element.getChildNodes().getLength());
    Assert.assertEquals(Base64.encodeAsString(new byte[]{0, 1, 2}), element.getTextContent());
  }

  @Test
  public void testFieldToXmlElementsFieldWithAdditionalNamespace() throws IOException, DataGeneratorException {
    Writer writer = Mockito.mock(Writer.class);
    XmlCharDataGenerator generator = new XmlCharDataGenerator(writer, true, ImmutableList.of(SCHEMA), true);
    Document document = generator.getDocumentBuilder().newDocument();

    String name = "nsa:element";
    Map<String, String> namespaces = ImmutableMap.of("", "", "ns", "NS");
    Field field = Field.create("data");
    field.setAttribute("xmlns:nsa", "NSA");
    List<Element> elements = generator.fieldToXmlElements(document, namespaces, name, field);
    Assert.assertEquals(1, elements.size());
    Element element = elements.get(0);
    Assert.assertEquals("nsa:element", element.getNodeName());
    Assert.assertEquals("NSA", element.getNamespaceURI());
    Assert.assertEquals(1, element.getChildNodes().getLength());
    Assert.assertEquals("data", element.getTextContent());
  }

  @Test
  public void testRecordToXmlDocument() throws IOException, DataGeneratorException {
    Writer writer = Mockito.mock(Writer.class);
    XmlCharDataGenerator generator = new XmlCharDataGenerator(writer, true, ImmutableList.of(SCHEMA), true);

    Record record = RecordCreator.create();
    record.set(Field.create(ImmutableMap.of("root", Field.create("data"))));

    Document document = generator.recordToXmlDocument(record);

    Assert.assertNotNull(document);
    Element element = document.getDocumentElement();
    Assert.assertEquals("root", element.getNodeName());
    Assert.assertEquals(null, element.getNamespaceURI());
    Assert.assertEquals(1, element.getChildNodes().getLength());
    Assert.assertEquals("data", element.getTextContent());
  }

  @Test
  public void testRecordToXmlDocumentWithNS() throws IOException, DataGeneratorException {
    Writer writer = Mockito.mock(Writer.class);
    XmlCharDataGenerator generator = new XmlCharDataGenerator(writer, true, ImmutableList.of(SCHEMA), true);

    Record record = RecordCreator.create();
    Field root = Field.create(ImmutableMap.of("root", Field.create("data")));
    root.setAttribute("xmlns", "NS");
    record.set(root);

    Document document = generator.recordToXmlDocument(record);

    Assert.assertNotNull(document);
    Element element = document.getDocumentElement();
    Assert.assertEquals("root", element.getNodeName());
    Assert.assertEquals("NS", element.getNamespaceURI());
    Assert.assertEquals(1, element.getChildNodes().getLength());
    Assert.assertEquals("data", element.getTextContent());
  }

  @Test(expected = DataGeneratorException.class)
  public void testRecordToXmlDocumentNoField() throws IOException, DataGeneratorException {
    Writer writer = Mockito.mock(Writer.class);
    XmlCharDataGenerator generator = new XmlCharDataGenerator(writer, true, ImmutableList.of(SCHEMA), true);

    Record record = RecordCreator.create();

    generator.recordToXmlDocument(record);
  }


  @Test(expected = DataGeneratorException.class)
  public void testRecordToXmlDocumentNoRootMapField() throws IOException, DataGeneratorException {
    Writer writer = Mockito.mock(Writer.class);
    XmlCharDataGenerator generator = new XmlCharDataGenerator(writer, true, ImmutableList.of(SCHEMA), true);

    Record record = RecordCreator.create();
    record.set(Field.create("data"));

    generator.recordToXmlDocument(record);
  }

  @Test(expected = DataGeneratorException.class)
  public void testRecordToXmlDocumentRootMapEmptyField() throws IOException, DataGeneratorException {
    Writer writer = Mockito.mock(Writer.class);
    XmlCharDataGenerator generator = new XmlCharDataGenerator(writer, true, ImmutableList.of(SCHEMA), true);

    Record record = RecordCreator.create();
    Field root = Field.create(ImmutableMap.of());
    record.set(root);

    generator.recordToXmlDocument(record);
  }

  @Test(expected = DataGeneratorException.class)
  public void testRecordToXmlDocumentRootMapMultiEntriesField() throws IOException, DataGeneratorException {
    Writer writer = Mockito.mock(Writer.class);
    XmlCharDataGenerator generator = new XmlCharDataGenerator(writer, true, ImmutableList.of(SCHEMA), true);

    Record record = RecordCreator.create();
    Field root = Field.create(ImmutableMap.of("root1", Field.create("data1"), "root2", Field.create("data2")));
    record.set(root);

    generator.recordToXmlDocument(record);
  }

}
