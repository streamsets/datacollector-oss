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

import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import java.io.IOException;
import java.io.StringReader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Data generator that converts a Record into an XML document.
 * <p/>
 * It supports namespaces. It supports schema validation.
 * <p/>
 * The Record root field must be a single element MAP, which becomes the XML document root.
 * <p/>
 * XML attributes are supported via Field attributes.
 * <p/>
 * Namespaces are supported via Field attributes.
 * <p/>
 * To preserve order of Maps, LIST_MAP field should be used (this is important in case of schema validation).
 * <p/>
 * All basic types are converted to String via their <code>toString()</code> method. If any special
 * formatting is desired, it must be done via a processor in a String field value.
 * <p/>
 * For binary fields, the data is converted to BASE64.
 */
public class XmlCharDataGenerator implements DataGenerator {
  private final Writer writer;
  private final boolean schemaValidation;
  private final Schema schema;
  private final DocumentBuilder documentBuilder;
  private final TransformerFactory transformerFactory;
  private final boolean prettyFormat;
  private boolean closed;

  public XmlCharDataGenerator(Writer writer, boolean schemaValidation, List<String> schemas, boolean prettyFormat)
      throws IOException {
    this.writer = writer;
    documentBuilder = createDocumentBuilder();
    this.transformerFactory = TransformerFactory.newInstance();
    this.schemaValidation = schemaValidation;
    this.schema = (schemaValidation) ? createSchema(schemas) : null;
    this.prettyFormat = prettyFormat;
  }

  boolean isSchemaValidation() {
    return schemaValidation;
  }

  boolean isPrettyFormat() {
    return prettyFormat;
  }

  protected Schema createSchema(List<String> schemas) throws IOException {
    try {
      SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
      Source[] sources = new Source[schemas.size()];
      int idx = 0;
      for (String schema : schemas) {
        sources[idx++] = new StreamSource(new StringReader(schema));
      }
      return factory.newSchema(sources);
    } catch (SAXException ex) {
      throw new IOException(Utils.format("Could not load schema(s): {}", ex), ex);
    }
  }

  protected Schema getSchema() {
    return schema;
  }

  protected DocumentBuilder createDocumentBuilder() throws IOException {
    try {
      return DocumentBuilderFactory.newInstance().newDocumentBuilder();
    } catch (Exception ex) {
      throw new IOException(ex);
    }
  }

  protected DocumentBuilder getDocumentBuilder() {
    return documentBuilder;
  }

  protected TransformerFactory getTransformerFactory() {
    return transformerFactory;
  }

  protected Transformer createTransformer() throws IOException {
    try {
      Transformer transformer = getTransformerFactory().newTransformer();
      if (isPrettyFormat()) {
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
      }
      return transformer;
    } catch (Exception ex) {
      throw new IOException(ex);
    }
  }

  @Override
  public void write(Record record) throws IOException, DataGeneratorException {
    if (closed) {
      throw new IOException("Generator has been closed");
    }
    Document doc = recordToXmlDocument(record);
    if (isSchemaValidation()) {
      validate(doc);
    }
    xmlWrite(doc);
  }

  protected void validate(Document doc) throws DataGeneratorException {
    try {
      Validator validator = getSchema().newValidator();
      validator.validate(new DOMSource(doc));
    } catch (Exception ex) {
      throw new DataGeneratorException(Errors.XML_GENERATOR_02, ex);
    }
  }

  protected void xmlWrite(Document doc) throws IOException {
    try {
      Transformer transformer = createTransformer();
      DOMSource source = new DOMSource(doc);
      StreamResult result = new StreamResult(writer);
      transformer.transform(source, result);
    } catch (Exception ex) {
      throw new IOException(ex);
    }
  }

  @Override
  public void flush() throws IOException {
    if (closed) {
      throw new IOException("Generator has been closed");
    }
    writer.flush();
  }

  @Override
  public void close() throws IOException {
    closed = true;
    writer.close();
  }

  protected boolean isMap(Field field) {
    return field != null && (field.getType() == Field.Type.LIST_MAP || field.getType() == Field.Type.MAP);
  }

  protected boolean isList(Field field) {
    return field != null && (field.getType() == Field.Type.LIST);
  }

  protected Document recordToXmlDocument(Record record) throws DataGeneratorException {
    Preconditions.checkNotNull(record, "record cannot be NULL");
    Field field = record.get();
    if (field == null) {
      throw new DataGeneratorException(Errors.XML_GENERATOR_03);
    }
    if (!isMap(field)) {
      throw new DataGeneratorException(Errors.XML_GENERATOR_00, field.getType());
    }
    Map<String, Field> map = field.getValueAsMap();
    if (map.size() != 1) {
      throw new DataGeneratorException(Errors.XML_GENERATOR_04);
    }
    Map.Entry<String, Field> entry = map.entrySet().iterator().next();
    String rootName = entry.getKey();
    Field rootField = entry.getValue();
    if (isList(rootField)) {
      throw new DataGeneratorException(Errors.XML_GENERATOR_05);
    }

    Map<String, String> namespaces = new HashMap<>();
    namespaces.put("", "");

    namespaces = getNamespaces(namespaces, field);

    Document doc = getDocumentBuilder().newDocument();
    List<Element> elements = fieldToXmlElements(doc, namespaces, rootName, rootField);
    if (elements.isEmpty()) {
      throw new DataGeneratorException(Errors.XML_GENERATOR_06);
    }
    doc.appendChild(elements.get(0));

    return doc;
  }

  protected Map<String, String> getNamespaces(Map<String, String> existingNamespaces, Field field) {
    Map<String, String> namespaces = existingNamespaces;
    if (field.getAttributes() != null) {
      boolean hasNewNamespaces = false;
      for (Map.Entry<String, String> mapEntry : field.getAttributes().entrySet()) {
        String key = mapEntry.getKey();
        if (key.equals("xmlns")) {
          if (!hasNewNamespaces) {
            namespaces = new HashMap<>(existingNamespaces);
            hasNewNamespaces = true;
          }
          String newNamespace = mapEntry.getValue();
          namespaces.put("", newNamespace);
        } else if (key.startsWith("xmlns:")) {
          if (!hasNewNamespaces) {
            namespaces = new HashMap<>(existingNamespaces);
            hasNewNamespaces = true;
          }
          String prefix = key.substring("xmlns:".length());
          String newNamespace = mapEntry.getValue();
          namespaces.put(prefix, newNamespace);
        }
      }
    }
    return namespaces;
  }

  protected Map<String, String> getAttributes(Field field) {
    Map<String, String> attributes = Collections.emptyMap();
    if (field.getAttributes() != null) {
      attributes = new HashMap<>();
      for (Map.Entry<String, String> mapEntry : field.getAttributes().entrySet()) {
        String key = mapEntry.getKey();
        if (!key.equals("xmlns") && !key.startsWith("xmlns:")) {
          attributes.put(key, mapEntry.getValue());
        }
      }
    }
    return attributes;
  }

  protected String getPrefix(String prefixName) {
    String prefix = "";
    if (prefixName.contains(":")) {
      prefix = prefixName.substring(0, prefixName.indexOf(":"));
    }
    return prefix;
  }

  protected String getName(String prefixName) {
    String name = prefixName;
    if (prefixName.contains(":")) {
      name = prefixName.substring(prefixName.indexOf(":") + 1);
    }
    return name;
  }

  protected Element createElement(
      Document doc, Map<String, String> namespaces, String name, String namespace, Field field
  ) throws DataGeneratorException {
    Element element;
    element = doc.createElementNS(namespace, name);
    if (field != null) {
      Map<String, String> attributes = getAttributes(field);
      if (!attributes.isEmpty()) {
        for (Map.Entry<String, String> attributeEntry : attributes.entrySet()) {
          String attribute = attributeEntry.getKey();
          String attributePrefix = getPrefix(attribute);
          String attributeName = getName(attribute);
          if (attributePrefix.isEmpty()) {
            element.setAttribute(attributeName, attributeEntry.getValue());
          } else {
            String attributeNamespace = namespaces.get(attributePrefix);
            if (attributeNamespace == null) {
              throw new DataGeneratorException(Errors.XML_GENERATOR_07, attribute);
            }
            element.setAttributeNS(attributeNamespace, attribute, attributeEntry.getValue());
          }
        }
      }
    }
    return element;
  }

  protected List<Element> fieldToXmlElements(Document doc, Map<String, String> namespaces, String name, Field field)
      throws DataGeneratorException {
    Preconditions.checkNotNull(namespaces, "namespaces cannot be NULL");
    Preconditions.checkNotNull(name, "name cannot be NULL");

    List<Element> elements = new ArrayList<>();

    if (field != null) {
      namespaces = getNamespaces(namespaces, field);
    }

    String elementPrefix = getPrefix(name);
    String elementNamespace = namespaces.get(elementPrefix);
    if (elementNamespace == null) {
      throw new DataGeneratorException(Errors.XML_GENERATOR_08, name);
    }

    if (field == null) {
      Element element = createElement(doc, namespaces, name, elementNamespace, field);
      elements.add(element);
    } else if (isMap(field)) {
      Element element = createElement(doc, namespaces, name, elementNamespace, field);
      Map<String, Field> map = field.getValueAsMap();
      for (Map.Entry<String, Field> mapEntry : map.entrySet()) {
        for (Element entryElement : fieldToXmlElements(doc, namespaces, mapEntry.getKey(), mapEntry.getValue())) {
          element.appendChild(entryElement);
        }
      }
      elements.add(element);
    } else if (isList(field)) {
      for (Field fieldListElement : field.getValueAsList()) {
        elements.addAll(fieldToXmlElements(doc, namespaces, name, fieldListElement));
      }
    } else {
      Element element = createElement(doc, namespaces, name, elementNamespace, field);
      if (field.getType() == Field.Type.BYTE_ARRAY) {
        element.appendChild(doc.createTextNode(Base64.getEncoder().encodeToString(field.getValueAsByteArray())));
      } else {
        element.appendChild(doc.createTextNode(field.getValueAsString()));
      }
      elements.add(element);
    }
    return elements;
  }

}
