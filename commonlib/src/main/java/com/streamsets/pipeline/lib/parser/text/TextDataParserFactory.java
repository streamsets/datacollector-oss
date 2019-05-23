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
package com.streamsets.pipeline.lib.parser.text;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.ext.io.OverrunReader;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.pool2.impl.GenericObjectPool;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class TextDataParserFactory extends DataParserFactory {
  public static final String MULTI_LINE_KEY = "multiLines";
  public static final boolean MULTI_LINE_DEFAULT = false;
  public static final String USE_CUSTOM_DELIMITER_KEY = "useCustomDelimiter";
  public static final boolean USE_CUSTOM_DELIMITER_DEFAULT = false;
  public static final String CUSTOM_DELIMITER_KEY = "customDelimiter";
  public static final String CUSTOM_DELIMITER_DEFAULT = "\\r\\n";
  public static final String INCLUDE_CUSTOM_DELIMITER_IN_TEXT_KEY = "includeCustomDelimiterInText";
  public static final boolean INCLUDE_CUSTOM_DELIMITER_IN_TEXT_DEFAULT = false;


  public static final Map<String, Object> CONFIGS = new HashMap<>();

  static {
    CONFIGS.put(MULTI_LINE_KEY, MULTI_LINE_DEFAULT);
    CONFIGS.put(USE_CUSTOM_DELIMITER_KEY, USE_CUSTOM_DELIMITER_DEFAULT);
    CONFIGS.put(CUSTOM_DELIMITER_KEY, CUSTOM_DELIMITER_DEFAULT);
    CONFIGS.put(INCLUDE_CUSTOM_DELIMITER_IN_TEXT_KEY, INCLUDE_CUSTOM_DELIMITER_IN_TEXT_DEFAULT);
  }

  public static final Set<Class<? extends Enum>> MODES = Collections.emptySet();

  public static final String TEXT_FIELD_NAME = "text";
  static final String TRUNCATED_FIELD_NAME = "truncated";

  private final GenericObjectPool<StringBuilder> stringBuilderPool;

  public TextDataParserFactory(Settings settings) {
    super(settings);
    stringBuilderPool = getStringBuilderPool(settings);
  }

  @Override
  public DataParser getParser(String id, InputStream is, String offset) throws DataParserException {
    return createParser(id, createReader(is), Long.parseLong(offset));
  }

  @Override
  public DataParser getParser(String id, Reader reader, long offset) throws DataParserException {
    return createParser(id, createReader(reader), offset);
  }

  @Override
  public void destroy() {
    if (stringBuilderPool != null) {
      stringBuilderPool.clear();
      stringBuilderPool.close();
    }

    super.destroy();
  }

  private DataParser createParser(String id, OverrunReader reader, long offset) throws DataParserException {
    Utils.checkState(reader.getPos() == 0, Utils.formatL("reader must be in position '0', it is at '{}'",
      reader.getPos()));
    try {

      return new TextCharDataParser(
          getSettings().getContext(),
          id,
          getSettings().<Boolean>getConfig(MULTI_LINE_KEY),
          getSettings().<Boolean>getConfig(USE_CUSTOM_DELIMITER_KEY),
          StringEscapeUtils.unescapeJava(getSettings().<String>getConfig(CUSTOM_DELIMITER_KEY)),
          getSettings().<Boolean>getConfig(INCLUDE_CUSTOM_DELIMITER_IN_TEXT_KEY),
          reader,
          offset,
          getSettings().getMaxRecordLen(),
          TEXT_FIELD_NAME,
          TRUNCATED_FIELD_NAME,
          stringBuilderPool
      );
    } catch (IOException ex) {
      throw new DataParserException(Errors.TEXT_PARSER_00, id, offset, ex.toString(), ex);
    }
  }

  @VisibleForTesting
  GenericObjectPool<StringBuilder> getStringBuilderPool() {
    return stringBuilderPool;
  }
}
