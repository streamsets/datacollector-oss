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

import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.ext.io.OverrunReader;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class XmlDataParserFactory extends DataParserFactory {
  static final String KEY_PREFIX = "xml.";
  public static final String RECORD_ELEMENT_KEY = KEY_PREFIX + "record.element";
  static final String RECORD_ELEMENT_DEFAULT = "";
  // Byte order mark (SDC-9668)
  // Note - Java Reader translates the 3 byte UTF-8 0xef 0xbb 0xbf prefix to a single character, \ufeff
  static final char BOM = '\ufeff';

  public static final String INCLUDE_FIELD_XPATH_ATTRIBUTES_KEY = KEY_PREFIX + "include.fieldxpaths";
  public static final boolean INCLUDE_FIELD_XPATH_ATTRIBUTES_DEFAULT = false;

  public static final String RECORD_ELEMENT_XPATH_NAMESPACES_KEY = KEY_PREFIX + "record.element.xPathNamespaces";
  static final Map<String, String> RECORD_ELEMENT_XPATH_NAMESPACES_DEFAULT = new HashMap<>();

  public static final String USE_FIELD_ATTRIBUTES = KEY_PREFIX + "useFieldAttributes";
  public static final boolean USE_FIELD_ATTRIBUTES_DEFAULT = false;

  public static final String PRESERVE_ROOT_ELEMENT_KEY = KEY_PREFIX + "preserveRootElement";
  public static final boolean PRESERVE_ROOT_ELEMENT_DEFAULT = false;

  public static final Map<String, Object> CONFIGS = ImmutableMap.of(
      RECORD_ELEMENT_KEY, RECORD_ELEMENT_DEFAULT,
      RECORD_ELEMENT_XPATH_NAMESPACES_KEY, RECORD_ELEMENT_XPATH_NAMESPACES_DEFAULT,
      INCLUDE_FIELD_XPATH_ATTRIBUTES_KEY, INCLUDE_FIELD_XPATH_ATTRIBUTES_DEFAULT,
      USE_FIELD_ATTRIBUTES, USE_FIELD_ATTRIBUTES_DEFAULT,
      PRESERVE_ROOT_ELEMENT_KEY, PRESERVE_ROOT_ELEMENT_DEFAULT
  );
  public static final Set<Class<? extends Enum>> MODES = Collections.emptySet();

  public XmlDataParserFactory(Settings settings) {
    super(settings);
  }

  @Override
  public DataParser getParser(String id, InputStream is, String offset) throws DataParserException {
    return createParser(id, createReader(is), Long.parseLong(offset));
  }

  @Override
  public DataParser getParser(String id, Reader reader, long offset) throws DataParserException {
    return createParser(id, createReader(reader), offset);
  }

  private DataParser createParser(String id, OverrunReader reader, long offset) throws DataParserException {
    Utils.checkState(reader.getPos() == 0, Utils.formatL("reader must be in position '0', it is at '{}'",
      reader.getPos()));
    try {
      if (getSettings().getCharset().name().equals("UTF-8")) {
        // Consume BOM if it's there
        reader.mark(1);
        if (BOM != (char) reader.read()) {
          reader.reset();
        }
      }

      return new XmlCharDataParser(getSettings().getContext(), id, reader, offset,
          getSettings().<String>getConfig(RECORD_ELEMENT_KEY),
          getSettings().<Boolean>getConfig(INCLUDE_FIELD_XPATH_ATTRIBUTES_KEY),
          getSettings().<Map<String,String>>getConfig(RECORD_ELEMENT_XPATH_NAMESPACES_KEY),
          getSettings().getMaxRecordLen(),
          getSettings().getConfig(USE_FIELD_ATTRIBUTES),
          getSettings().getConfig(PRESERVE_ROOT_ELEMENT_KEY)
      );
    } catch (IOException ex) {
      throw new DataParserException(Errors.XML_PARSER_00, id, offset, ex.toString(), ex);
    }
  }

}
