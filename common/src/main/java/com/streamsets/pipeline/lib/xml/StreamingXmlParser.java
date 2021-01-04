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

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.ext.io.ObjectLengthException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.xml.xpath.MatchStatus;
import com.streamsets.pipeline.lib.xml.xpath.XPathMatchingEventReader;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.Namespace;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class StreamingXmlParser {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingXmlParser.class);

  public static final String USE_JAVA_INTERNAL_XML_INPUT_FACTORY = "com.streamsets.pipeline.lib.xml.StreamingXmlParser.useJvmInternalInputFactoryImpl";
  private static final String JAVA_INTERNAL_XML_INPUT_FACTORY = "com.sun.xml.internal.stream.XMLInputFactoryImpl";

  public static final String VALUE_KEY = "value";
  public static final String ATTR_PREFIX_KEY = "attr|";
  private static final String NS_PREFIX_KEY = "ns|";
  public static final String GENERATED_NAMESPACE_PREFIX = "ns";
  public static final String XPATH_KEY = "xpath";
  public static final String XMLATTR_ATTRIBUTE_PREFIX = "xmlAttr:";

  private final Reader reader;
  private final XPathMatchingEventReader xmlEventReader;
  private final boolean useFieldAttributesInsteadOfFields;
  private final boolean preserveRootElement;
  private String recordElement;
  private boolean closed;

  private String lastParsedFieldXpathPrefix;
  final LinkedList<String> elementNameStack = new LinkedList<>();

  private int generatedNsPrefixCount = 1;
  private final Map<String, String> namespaceUriToPrefix = new HashMap<>();

  public StreamingXmlParser(
      final Reader reader,
      final String recordElement,
      final Map<String, String> namespaces,
      final long initialPosition,
      final boolean useFieldAttributesInsteadOfFields,
      final boolean preserveRootElement
  ) throws XMLStreamException {

    this.reader = reader;
    this.useFieldAttributesInsteadOfFields = useFieldAttributesInsteadOfFields;
    this.preserveRootElement = preserveRootElement;
    if (Strings.isNullOrEmpty(recordElement)) {
      this.recordElement = Constants.ROOT_ELEMENT_PATH;
    } else {
      this.recordElement = recordElement;
    }

    // The XMLInputFactory.newFactory() uses internally ServiceLoader which is fine under most of the circumstances.
    // The notable exception is cluster mode where Hadoop jars might "confuse" ServiceLoader since some of the
    // JVM-wide pieces of functionality aren't as "clear" as in standalone data collector.
    XMLInputFactory factory = null;

    if(Boolean.getBoolean(USE_JAVA_INTERNAL_XML_INPUT_FACTORY)) {
      try {
        LOG.debug("Using Java internal XmlInputFactoryImpl");
        Class factoryClass = Thread.currentThread().getContextClassLoader().loadClass(JAVA_INTERNAL_XML_INPUT_FACTORY);
        factory = (XMLInputFactory)factoryClass.newInstance();
      } catch (ClassNotFoundException|IllegalAccessException|InstantiationException e) {
        LOG.debug("Can't load Java Internal Factory: {}", JAVA_INTERNAL_XML_INPUT_FACTORY, e);
      }
    }

    if(factory == null) {
      factory = XMLInputFactory.newFactory();
    }
    LOG.debug("Loaded XMLInputFactory: {}", factory.getClass());

    factory.setProperty("javax.xml.stream.isCoalescing", true);
    factory.setProperty("javax.xml.stream.isSupportingExternalEntities", false);
    factory.setProperty("javax.xml.stream.supportDTD", false);
    this.xmlEventReader = new XPathMatchingEventReader(factory.createXMLEventReader(reader), this.recordElement, namespaces);
    while (hasNext(xmlEventReader) && !peek(xmlEventReader).isEndDocument() && !peek(xmlEventReader).isStartElement()) {
      read(xmlEventReader);
    }
    if (recordElement == null || recordElement.isEmpty()) {
      StartElement startE = (StartElement) peek(xmlEventReader);
      this.recordElement = startE.getName().getLocalPart();
    } else {
      //consuming root
      StartElement startE = (StartElement) read(xmlEventReader);
      elementNameStack.addFirst(getNameAndTrackNs(startE.getName()));
    }
    if (initialPosition > 0) {
      //fastforward to initial position
      while (hasNext(xmlEventReader) && peek(xmlEventReader).getLocation().getCharacterOffset() < initialPosition) {
        processNextEvent();
        fastForwardLeaseReader();
      }
      xmlEventReader.clearLastMatch();
    }
  }

  public Reader getReader() {
    return reader;
  }

  public String getLastParsedFieldXpathPrefix() {
    return lastParsedFieldXpathPrefix;
  }

  public Map<String, String> getNamespaceUriToPrefixMappings() {
    return Collections.unmodifiableMap(namespaceUriToPrefix);
  }

  public void close() {
    closed = true;
    try {
      xmlEventReader.close();
    } catch (Exception ex) {
      // NOP
    }
    elementNameStack.clear();
    generatedNsPrefixCount = 1;
    namespaceUriToPrefix.clear();
  }

  private String getNameAndTrackNs(QName name) {
    final String uri = name.getNamespaceURI();
    if (!Strings.isNullOrEmpty(uri)) {
      String prefix;
      if (!namespaceUriToPrefix.containsKey(uri)) {
        prefix = name.getPrefix();
        if (Strings.isNullOrEmpty(prefix)) {
          //generate a new namespace prefix for it
          prefix = GENERATED_NAMESPACE_PREFIX + generatedNsPrefixCount++;
        } //else the element already came with a prefix, so just use that
        namespaceUriToPrefix.put(uri, prefix);
      } else {
        prefix = namespaceUriToPrefix.get(uri);
      }
      return prefix + ":" + name.getLocalPart();
    } else {
      // element is in no namespace
      return name.getLocalPart();
    }
  }

  public Field read() throws IOException, XMLStreamException {
    if (closed) {
      throw new IOException("The parser has been closed");
    }
    Field field = null;
    if (hasNext(xmlEventReader)) {
      int depth = 0;

      // we need to skip first level elements that are not the record delimiter and we have to ignore record delimiter
      // elements deeper than first level
      while (hasNext(xmlEventReader) && !isStartOfRecord()) {
        depth += processNextEvent();
      }
      if (hasNext(xmlEventReader)) {
        StartElement startE = (StartElement) xmlEventReader.getLastMatchingEvent();
        field = parse(xmlEventReader, startE);

        if (preserveRootElement) {
          field = Field.create(Collections.singletonMap(getElementName(startE), field));
        }

        // the while loop consumes the start element for a record, and the parse method above consumes the end
        // so remove it from the stack
        elementNameStack.removeFirst();
      }
      // if advancing, don't evaluate XPath matches
      xmlEventReader.clearLastMatch();
    }
    return field;
  }

  protected void fastForwardLeaseReader() {
  }

  public long getReaderPosition() throws XMLStreamException {
    return (hasNext(xmlEventReader)) ? peek(xmlEventReader).getLocation().getCharacterOffset() : -1;
  }

  public String getXpathPrefix() {
    return "/" + StringUtils.join(Lists.reverse(elementNameStack), "/");
  }

  private boolean isStartOfRecord() {
    return xmlEventReader.getLastElementMatchResult() == MatchStatus.ELEMENT_MATCH;
  }

  boolean isIgnorable(XMLEvent event) {
    return event.getEventType() == XMLEvent.PROCESSING_INSTRUCTION || event.getEventType() == XMLEvent.COMMENT;
  }

  void skipIgnorable(XMLEventReader reader) throws XMLStreamException {
    while (reader.hasNext() && isIgnorable(reader.peek())) {
      reader.nextEvent();
    }
  }

  boolean hasNext(XMLEventReader reader) throws XMLStreamException {
    skipIgnorable(reader);
    return reader.hasNext();
  }

  XMLEvent peek(XMLEventReader reader) throws XMLStreamException {
    skipIgnorable(reader);
    return reader.peek();
  }

  XMLEvent read(XMLEventReader reader) throws XMLStreamException  {
    skipIgnorable(reader);
    return reader.nextEvent();
  }

  String getName(String namePrefix, Attribute element) {
    return getName(element.getName(), namePrefix);
  }

  String getName(StartElement element) {
    return getName(element.getName(), null);
  }

  private String getName(QName name, String namePrefix) {
    StringBuilder sb = new StringBuilder();
    if (!Strings.isNullOrEmpty(namePrefix)) {
      sb.append(namePrefix);
    }
    sb.append(getNameAndTrackNs(name));
    return sb.toString();
  }

  Map<String, Field> toField(StartElement startE) {
    Map<String, Field> map = new LinkedHashMap<>();
    Iterator attrs = startE.getAttributes();
    while (attrs.hasNext()) {
      Attribute attr = (Attribute) attrs.next();
      map.put(getName(ATTR_PREFIX_KEY, attr), Field.create(attr.getValue()));
    }
    Iterator nss = startE.getNamespaces();
    while (nss.hasNext()) {
      Namespace ns = (Namespace) nss.next();
      map.put(getName(NS_PREFIX_KEY, ns), Field.create(ns.getNamespaceURI()));
    }
    return map;
  }

  protected boolean isOverMaxObjectLength() throws XMLStreamException {
    return false;
  }

  @SuppressWarnings("unchecked")
  private void addContent(Map<String, Object> contents, String name, Field field) throws
      XMLStreamException,
      ObjectLengthException {
    throwIfOverMaxObjectLength();
    List<Field> list = (List<Field>) contents.get(name);
    if (list == null) {
      list = new ArrayList<>();
      contents.put(name, list);
    }
    list.add(field);
  }

  @SuppressWarnings("unchecked")
  Field parse(XMLEventReader reader, StartElement startE) throws XMLStreamException, ObjectLengthException {
    Map<String, Field> map = this.useFieldAttributesInsteadOfFields ? new LinkedHashMap<>() : toField(startE);
    Map<String, Field> startEMap = map;
    Map<String, Object> contents = new LinkedHashMap<>();
    boolean maybeText = true;
    while (hasNext(reader) && !peek(reader).isEndElement()) {
      XMLEvent next = read(reader);
      if (next.isCharacters()) {
        // If this set of characters is all whitespace, ignore.
        if (next.asCharacters().isWhiteSpace()) {
          continue;
        } else if (peek(reader).isEndElement() && maybeText) {
          contents.put(VALUE_KEY, Field.create(((Characters)next).getData()));
        } else if (peek(reader).isStartElement()) {
          StartElement subStartE = (StartElement) read(reader);
          Field subField = parse(reader, subStartE);
          addContent(contents, getName(subStartE), subField);
          if (hasNext(reader) && peek(reader).isCharacters()) {
            read(reader);
          }
        } else if (maybeText) {
          throw new XMLStreamException(Utils.format(
              "Unexpected XMLEvent '{}', it should be START_ELEMENT or END_ELEMENT", next), next.getLocation());
        }
      } else if (next.isStartElement()) {
        String name = getName((StartElement) next);
        Field field = parse(reader, (StartElement) next);
        addContent(contents, name, field);
      } else {
        throw new XMLStreamException(Utils.format("Unexpected XMLEvent '{}', it should be START_ELEMENT or CHARACTERS",
                                                  next), next.getLocation());
      }
      maybeText = false;
    }
    if (hasNext(reader)) {
      EndElement endE = (EndElement) read(reader);
      if (!endE.getName().equals(startE.getName())) {
        throw new XMLStreamException(Utils.format("Unexpected EndElement '{}', it should be '{}'",
                                                  endE.getName().getLocalPart(), startE.getName().getLocalPart()),
                                     endE.getLocation());
      }
      for (Map.Entry<String, Object> entry : contents.entrySet()) {
        if (entry.getValue() instanceof Field) {
          startEMap.put(entry.getKey(), (Field) entry.getValue());
        } else {
          startEMap.put(entry.getKey(), Field.create((List<Field>)entry.getValue()));
        }
      }
    }
    final Field field = Field.create(startEMap);

    if (this.useFieldAttributesInsteadOfFields) {
      Iterator attrs = startE.getAttributes();
      while (attrs.hasNext()) {
        Attribute attr = (Attribute) attrs.next();
        field.setAttribute(getName(XMLATTR_ATTRIBUTE_PREFIX, attr), attr.getValue());
      }
      Iterator nss = startE.getNamespaces();
      while (nss.hasNext()) {
        Namespace ns = (Namespace) nss.next();
        field.setAttribute(getName(null, ns), ns.getNamespaceURI());
      }
    }

    lastParsedFieldXpathPrefix = getXpathPrefix();
    return field;
  }

  protected void throwIfOverMaxObjectLength() throws XMLStreamException, ObjectLengthException {
  }

  private int processNextEvent() throws XMLStreamException {
    XMLEvent event = read(xmlEventReader);
    int depthUpdate = 0;
    if (event.isStartElement()) {
      elementNameStack.addFirst(getNameAndTrackNs(event.asStartElement().getName()));
      depthUpdate = 1;
    } else if (event.getEventType() == XMLEvent.END_ELEMENT) {
      elementNameStack.removeFirst();
      depthUpdate = -1;
    }
    return depthUpdate;
  }

  private String getElementName(StartElement element) {
    QName elementName = element.getName();
    if (elementName.getPrefix().isEmpty()) {
      return elementName.getLocalPart();
    } else {
      return String.format("%s:%s", element.getName().getPrefix(), element.getName().getLocalPart());
    }
  }

  public boolean isPreserveRootElement() {
    return preserveRootElement;
  }

}
