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
package com.streamsets.pipeline.lib.parser.xml;

import com.google.common.base.Strings;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.ProtoConfigurableEntity;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.ext.io.OverrunException;
import com.streamsets.pipeline.api.ext.io.OverrunReader;
import com.streamsets.pipeline.lib.parser.AbstractDataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.xml.OverrunStreamingXmlParser;
import com.streamsets.pipeline.lib.xml.StreamingXmlParser;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class XmlCharDataParser extends AbstractDataParser {
  private static final Pattern INDEX_PATTERN = Pattern.compile("((.*)/\\S+)(\\[\\d+\\]).*");
  private static final Pattern VALUE_PATTERN = Pattern.compile("'?(\\S+\\[\\d+\\])?'?/value$");
  private static final Pattern ATTR_PATTERN = Pattern.compile("'?(\\S+\\[\\d+\\])?'?/'attr\\|(\\S+)'");
  public static final String REMOVE_FIELD_PATH_SINGLE_QUOTE_PATTERN = "'?([^']*)'?(\\[\\d+\\])?";

  public static final String RECORD_ATTRIBUTE_NAMESPACE_PREFIX = "xmlns:";

  private final ProtoConfigurableEntity.Context context;
  private final String readerId;
  private final int maxObjectLen;
  private final OverrunStreamingXmlParser parser;
  private final boolean includeXpath;
  private long readerOffset;

  public XmlCharDataParser(
      ProtoConfigurableEntity.Context context,
      String readerId,
      OverrunReader reader,
      long readerOffset,
      String recordElement,
      int maxObjectLen
  ) throws IOException {
    this(
        context,
        readerId,
        reader,
        readerOffset,
        recordElement,
        false,
        null,
        maxObjectLen,
        true
    );
  }

  public XmlCharDataParser(
      ProtoConfigurableEntity.Context context,
      String readerId,
      OverrunReader reader,
      long readerOffset,
      String recordElement,
      boolean includeXpath,
      int maxObjectLen
  ) throws IOException {
    this(
        context,
        readerId,
        reader,
        readerOffset,
        recordElement,
        includeXpath,
        null,
        maxObjectLen,
        true
    );
  }

  public XmlCharDataParser(
      ProtoConfigurableEntity.Context context,
      String readerId,
      OverrunReader reader,
      long readerOffset,
      String recordElement,
      boolean includeXpath,
      Map<String, String> namespaces,
      int maxObjectLen,
      boolean useFieldAttributesInsteadOfFields
  ) throws IOException {
    this.context = context;
    this.readerId = readerId;
    this.readerOffset = readerOffset;
    this.maxObjectLen = maxObjectLen;
    this.includeXpath = includeXpath;
    try {
      parser = new OverrunStreamingXmlParser(
          reader,
          recordElement,
          namespaces,
          readerOffset,
          maxObjectLen,
          useFieldAttributesInsteadOfFields
      );
    } catch (XMLStreamException ex) {
      throw new IOException(ex);
    }
  }

  @Override
  public Record parse() throws IOException, DataParserException {
    Record record = null;
    long offset = -1;
    try {
      offset = getOffsetAsLong();
      Field field = parser.read();
      readerOffset = -1;
      if (field != null) {
        record = createRecord(offset, field);
      }
    } catch (OverrunException ex) {
      throw new DataParserException(Errors.XML_PARSER_02, readerId, offset, maxObjectLen);
    } catch (XMLStreamException ex) {
      throw new DataParserException(Errors.XML_PARSER_03, ex);
    }
    return record;
  }

  protected Record createRecord(long offset, Field field) throws DataParserException {
    Record record = context.createRecord(readerId + "::" + offset);
    record.set(field);
    if (includeXpath) {
      setFieldXpathAttributes(record);
    }
    return record;
  }

  private void setFieldXpathAttributes(Record record) {
    for (String path : record.getEscapedFieldPaths()) {
      // Only interested in leaves of the path tree so pass any complex types.
      // This check is needed because an XML element may be named as "value".
      if (record.get(path).getType() == Field.Type.LIST ||
          record.get(path).getType() == Field.Type.LIST_MAP ||
          record.get(path).getType() == Field.Type.MAP) {
        continue;
      }
      Matcher matcher = VALUE_PATTERN.matcher(path);
      Field field = record.get(path);
      String xpath = null;
      if (matcher.matches()) {
        String fieldPath = removeSingleQuotesFromFieldPath(matcher.group(1));
        xpath = toXpath(fieldPath, record);
      } else {
        matcher = ATTR_PATTERN.matcher(path);
        if (matcher.matches()) {
          String fieldPath = removeSingleQuotesFromFieldPath(matcher.group(1));
          String attribute = matcher.group(2);
          xpath = toXpath(fieldPath, record) + "/@" + attribute;
        }
      }
      if (!Strings.isNullOrEmpty(xpath)) {
        field.setAttribute(StreamingXmlParser.XPATH_KEY, xpath);
      }
    }

    Record.Header header = record.getHeader();
    for (Map.Entry<String, String> nsEntry : parser.getNamespaceUriToPrefixMappings().entrySet()) {
      header.setAttribute(RECORD_ATTRIBUTE_NAMESPACE_PREFIX + nsEntry.getValue(), nsEntry.getKey());
    }
  }

  private static String removeSingleQuotesFromFieldPath(String fieldPath) {
    if (Strings.isNullOrEmpty(fieldPath)) {
      return fieldPath;
    } else {
      return fieldPath.replaceAll(REMOVE_FIELD_PATH_SINGLE_QUOTE_PATTERN, "$1$2");
    }
  }

  private String toXpath(String fieldPath, Record record) {
    if (fieldPath == null) {
      fieldPath = "";
    }
    String xpath = fieldPath;
    List<MatchResult> matchResults = new ArrayList<>();
    Matcher matcher = INDEX_PATTERN.matcher(fieldPath);
    while (matcher.matches()) {
      MatchResult matchResult = matcher.toMatchResult();
      matchResults.add(matchResult);
      String parentPath = matchResult.group(2);
      matcher = INDEX_PATTERN.matcher(parentPath);
    }
    for (MatchResult matchResult : matchResults) {
      String currentPath = matchResult.group(1);
      String fieldIndex = matchResult.group(3);
      // If the field is an array of a single value, flatten it out
      // to make it comply with the XPath syntax.
      if (record.get(currentPath).getValueAsList().size() == 1) {
        xpath = xpath.replace(currentPath + fieldIndex, currentPath);
      }
    }
    return parser.getLastParsedFieldXpathPrefix() + xpath;
  }

  @Override
  public String getOffset() throws DataParserException {
    return String.valueOf(getOffsetAsLong());

  }

  private long getOffsetAsLong() throws DataParserException {
    try {
      return (readerOffset > -1) ? readerOffset : parser.getReaderPosition();
    } catch (XMLStreamException ex) {
      throw new DataParserException(Errors.XML_PARSER_01, ex.toString(), ex);
    }
  }

  @Override
  public void close() throws IOException {
    parser.close();
  }

}
