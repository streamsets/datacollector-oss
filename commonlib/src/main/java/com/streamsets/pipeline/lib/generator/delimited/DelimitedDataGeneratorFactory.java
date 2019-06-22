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
package com.streamsets.pipeline.lib.generator.delimited;

import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.util.DelimitedDataConstants;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DelimitedDataGeneratorFactory extends DataGeneratorFactory {
  static final String KEY_PREFIX = "delimited.";
  public static final String HEADER_KEY = KEY_PREFIX + "header";
  static final String HEADER_DEFAULT = "header";
  public static final String VALUE_KEY = KEY_PREFIX + "value";
  static final String VALUE_DEFAULT = "value";
  /*
   * We can't store null inside our configurations because underneath our code uses ImmutableMap
   * from guava that have this limitation. Hence we're modeling the optional argument with non-null
   * default value and explicit boolean argument stating if the option is on/off.
   */
  public static final String REPLACE_NEWLINES_KEY = KEY_PREFIX + "replaceNewLines";
  static final boolean REPLACE_NEWLINES_DEFAULT = true;
  public static final String REPLACE_NEWLINES_STRING_KEY = KEY_PREFIX + "replaceNewLinesString";
  static final String REPLACE_NEWLINES_STRING_DEFAULT = " ";

  public static final Map<String, Object> CONFIGS;

  static {
    Map<String, Object> configs = new HashMap<>();
    configs.put(HEADER_KEY, HEADER_DEFAULT);
    configs.put(VALUE_KEY, VALUE_DEFAULT);
    configs.put(REPLACE_NEWLINES_KEY, REPLACE_NEWLINES_DEFAULT);
    configs.put(REPLACE_NEWLINES_STRING_KEY, REPLACE_NEWLINES_STRING_DEFAULT);
    configs.put(DelimitedDataConstants.DELIMITER_CONFIG, '|');
    configs.put(DelimitedDataConstants.ESCAPE_CONFIG, '\\');
    configs.put(DelimitedDataConstants.QUOTE_CONFIG, '"');
    configs.put(DelimitedDataConstants.QUOTE_MODE, QuoteMode.MINIMAL);

    CONFIGS = Collections.unmodifiableMap(configs);
  }

  @SuppressWarnings("unchecked")
  public static final Set<Class<? extends Enum>> MODES = (Set) ImmutableSet.of(CsvMode.class, CsvHeader.class);

  private final CsvHeader header;
  private final String headerKey;
  private final String valueKey;
  private final boolean replaceNewLines;
  private final String replaceNewLinesString;

  public DelimitedDataGeneratorFactory(Settings settings) {
    super(settings);
    this.header = settings.getMode(CsvHeader.class);
    headerKey = settings.getConfig(HEADER_KEY);
    valueKey = settings.getConfig(VALUE_KEY);
    replaceNewLines = settings.getConfig(REPLACE_NEWLINES_KEY);
    replaceNewLinesString = settings.getConfig(REPLACE_NEWLINES_STRING_KEY);
  }

  @Override
  public DataGenerator getGenerator(OutputStream os) throws IOException {
    final CsvMode mode = getSettings().getMode(CsvMode.class);
    CSVFormat csvFormat = mode.getFormat();
    switch (mode) {
      case CUSTOM:
        csvFormat = CSVFormat.DEFAULT
          .withDelimiter(getSettings().getConfig(DelimitedDataConstants.DELIMITER_CONFIG))
          .withEscape((char) getSettings().getConfig(DelimitedDataConstants.ESCAPE_CONFIG))
          .withQuote((char)getSettings().getConfig(DelimitedDataConstants.QUOTE_CONFIG))
          .withQuoteMode(getSettings().getConfig(DelimitedDataConstants.QUOTE_MODE))
        ;
        break;
      case MULTI_CHARACTER:
        // TODO: figure out cleaner way (ex: hiding in UI)?
        throw new UnsupportedOperationException("Multiple character delimited is not yet supported for generators");
      default:
        //nothing to do;
        break;
    }
    return new DelimitedCharDataGenerator(createWriter(os), csvFormat, header, headerKey, valueKey, replaceNewLines ? replaceNewLinesString : null);
  }

}
