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
package com.streamsets.pipeline.lib.parser.log;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.ProtoConfigurableEntity;
import com.streamsets.pipeline.api.ext.io.OverrunReader;
import com.streamsets.pipeline.lib.parser.DataParserException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPool;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class ExtendedFormatParser extends LogCharDataParser {
  private static final String HEADER_REGEX = "(?<!\\\\)\\|";
  private static final String EXT_REGEX = "(?<!\\\\)=";
  static final Pattern HEADER_PATTERN = Pattern.compile(HEADER_REGEX);
  static final Pattern EXT_PATTERN = Pattern.compile(EXT_REGEX);

  private final String formatName;
  private final ExtendedFormatType formatType;

  public ExtendedFormatParser(
      ProtoConfigurableEntity.Context context,
      String readerId,
      OverrunReader reader,
      long readerOffset,
      int maxObjectLen,
      boolean retainOriginalText,
      String formatName,
      ExtendedFormatType formatType,
      GenericObjectPool<StringBuilder> currentLineBuilderPool,
      GenericObjectPool<StringBuilder> previousLineBuilderPool
  ) throws IOException {
    super(context,
        readerId,
        reader,
        readerOffset,
        maxObjectLen,
        retainOriginalText,
        -1,
        currentLineBuilderPool,
        previousLineBuilderPool
    );
    this.formatName = formatName;
    this.formatType = formatType;
  }

  abstract Field getExtFormatVersion(String val);

  abstract int getNumHeaderFields(ExtendedFormatType formatType, Field formatVersion);

  abstract String getHeaderFieldName(int index);

  abstract char getExtensionAttrSeparator(Matcher m, int index, StringBuilder logLine);

  @Override
  public Map<String, Field> parseLogLine(StringBuilder logLine) throws DataParserException {
    Map<String, Field> map = new HashMap<>();

    // Parse headers
    Matcher m = HEADER_PATTERN.matcher(logLine);
    int counter = 0;
    int index = 0;
    int headerCount = 1;
    while (counter < headerCount && m.find()) {
      String val = logLine.substring(index, m.start());

      if (counter == 0) {
        Field formatVersion = getExtFormatVersion(val);
        map.put(formatType.label + "Version", formatVersion);
        headerCount = getNumHeaderFields(formatType, formatVersion);
      } else {
        map.put(getHeaderFieldName(counter), Field.create(val));
      }

      index = m.end();
      counter++;
    }

    if (counter < headerCount) {
      throw new DataParserException(Errors.LOG_PARSER_12, formatName, headerCount, counter);
    }

    // For LEEF 2.0, there is an optional field in the header, so we check for it, and
    // advance the index, if necessary, to get to the start of the extensions
    char attrSeparator = getExtensionAttrSeparator(m, index, logLine);
    if (!m.hitEnd()) {
      index = m.end();
    }

    // Calls to trim() will strip off whitespace, but if format is LEEF 2.0 and a custom
    // delimiter is being used, we need to offset the start index of extension keys
    int offset = 0;
    if (!Character.isWhitespace(attrSeparator)) {
      offset = 1;
    }

    // Process extensions
    Map<String, Field> extMap = new HashMap<>();
    Map<String, String> labelMap = new HashMap<>();
    String ext = logLine.substring(index);
    m = EXT_PATTERN.matcher(ext);
    index = 0;
    String key = null;
    String value;

    while (m.find()) {
      if (key == null) {
        key = ext.substring(index, m.start());
        index = m.end();
        if (!m.find()) {
          break;
        }
      }
      // Regex will search for unescaped '=' character to find the split between keys
      // and values. We'll need to figure out where the separator is to determine the
      // end of the value, and then go back for the next KV pair
      value = ext.substring(index, m.start());
      index = m.end();
      int lastSepIndex = value.lastIndexOf(attrSeparator);
      if (lastSepIndex > 0) {
        String temp = value.substring(0, lastSepIndex).trim();
        putLabelIntoAppropriateMap(labelMap, extMap, key, temp);
        key = value.substring(lastSepIndex + offset).trim();
      }
    }
    value = ext.substring(index);

    // Build a map of Label extensions to apply later
    putLabelIntoAppropriateMap(labelMap, extMap, key, value);

    // Apply the labels to custom fields
    for (Map.Entry<String, String> label : labelMap.entrySet()) {
      if (extMap.containsKey(label.getKey())) {
        Field field = extMap.remove(label.getKey());
        extMap.put(label.getValue(), field);
      }
    }

    map.put("extensions", Field.create(extMap));

    return map;
  }

  void putLabelIntoAppropriateMap(
      Map<String, String> labelMap,
      Map<String, Field> extMap,
      String key,
      String value
  ) {
    if (key.endsWith("Label")) {
      labelMap.put(StringUtils.substringBefore(key, "Label"), value);
    } else {
      extMap.put(key, Field.create(value));
    }
  }
}
