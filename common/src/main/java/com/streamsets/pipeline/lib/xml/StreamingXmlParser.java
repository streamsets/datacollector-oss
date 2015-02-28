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
package com.streamsets.pipeline.lib.xml;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.impl.Utils;

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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class StreamingXmlParser {

  private static final String VALUE_KEY = "value";
  private static final String ATTR_PREFIX_KEY = "attr|";
  private static final String NS_PREFIX_KEY = "ns|";

  private final Reader reader;
  private final XMLEventReader xmlEventReader;
  private String recordElement;
  private boolean closed;

  // reads a full XML document as a single Field
  public StreamingXmlParser(Reader xmlEventReader) throws IOException, XMLStreamException {
    this(xmlEventReader, null, 0);
  }

  // reads an XML document producing a Field for each first level 'recordElement' element, other first level elements
  // are ignored
  public StreamingXmlParser(Reader xmlEventReader, String recordElement)
      throws IOException, XMLStreamException {
    this(xmlEventReader, recordElement, 0);
  }

  // reads an XML document producing a Field for each first level 'recordElement' element, other first level elements
  // are ignored
  public StreamingXmlParser(Reader reader, String recordElement, long initialPosition)
      throws IOException, XMLStreamException {
    this.reader = reader;
    this.recordElement = recordElement;
    XMLInputFactory factory = XMLInputFactory.newFactory();
    factory.setProperty("javax.xml.stream.isCoalescing", true);
    this.xmlEventReader = factory.createXMLEventReader(reader);
    while (hasNext(xmlEventReader) && !peek(xmlEventReader).isEndDocument() && !peek(xmlEventReader).isStartElement()) {
      read(xmlEventReader);
    }
    if (recordElement == null || recordElement.isEmpty()) {
      StartElement startE = (StartElement) peek(xmlEventReader);
      this.recordElement = startE.getName().getLocalPart();
    } else {
      //consuming root
      read(xmlEventReader);
    }
    //fastforward to initial position
    while (hasNext(xmlEventReader) && peek(xmlEventReader).getLocation().getCharacterOffset() < initialPosition) {
      read(xmlEventReader);
      fastForwardLeaseReader();
    }
  }

  public Reader getReader() {
    return reader;
  }

  public void close() {
    closed = true;
    try {
      xmlEventReader.close();
    } catch (Exception ex) {
      // NOP
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
      while (hasNext(xmlEventReader) && !isStartOfRecord(peek(xmlEventReader), depth)) {
        XMLEvent event  = read(xmlEventReader);
        switch (event.getEventType()) {
          case XMLEvent.START_ELEMENT:
            depth++;
            break;
          case XMLEvent.END_ELEMENT:
            depth--;
            break;
        }
      }
      if (hasNext(xmlEventReader)) {
        StartElement startE = (StartElement) read(xmlEventReader);
        field = parse(xmlEventReader, startE);
      }
    }
    return field;
  }

  protected void fastForwardLeaseReader() {
  }

  public long getReaderPosition() throws XMLStreamException {
    return (hasNext(xmlEventReader)) ? peek(xmlEventReader).getLocation().getCharacterOffset() : -1;
  }

  private boolean isStartOfRecord(XMLEvent event, int depth) {
    boolean is = false;
    if (depth == 0 && event.isStartElement()) {
      StartElement startE = (StartElement) event;
      if (startE.getName().getLocalPart().equals(recordElement)) {
        is = true;
      }
    }
    return is;
  }

  boolean isIgnorable(XMLEvent event) {
    boolean ignore = false;
    switch (event.getEventType()) {
      case XMLEvent.PROCESSING_INSTRUCTION:
      case XMLEvent.COMMENT:
        ignore = true;
        break;
    }
    return ignore;
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
    StringBuilder sb = new StringBuilder();
    sb.append(namePrefix);
    String prefix = element.getName().getPrefix();
    if (!prefix.isEmpty()) {
      sb.append(prefix).append(":");
    }
    sb.append(element.getName().getLocalPart());
    return sb.toString();
  }

  String getName(StartElement element) {
    StringBuilder sb = new StringBuilder();
    String prefix = element.getName().getPrefix();
    if (!prefix.isEmpty()) {
      sb.append(prefix).append(":");
    }
    sb.append(element.getName().getLocalPart());
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
  private void addContent(Map<String, Object> contents, String name, Field field) throws XMLStreamException {
    if (!isOverMaxObjectLength()) {
      List<Field> list = (List<Field>) contents.get(name);
      if (list == null) {
        list = new ArrayList<>();
        contents.put(name, list);
      }
      list.add(field);
    }
  }

  @SuppressWarnings("unchecked")
  Field parse(XMLEventReader reader, StartElement startE) throws XMLStreamException {
    Map<String, Field> startEMap = toField(startE);
    Map<String, Object> contents = new LinkedHashMap<>();
    boolean maybeText = true;
    while (hasNext(reader) && !peek(reader).isEndElement()) {
      XMLEvent next = read(reader);
      if (next.isCharacters()) {
        if (peek(reader).isEndElement() && maybeText) {
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
    return Field.create(startEMap);
  }

}
