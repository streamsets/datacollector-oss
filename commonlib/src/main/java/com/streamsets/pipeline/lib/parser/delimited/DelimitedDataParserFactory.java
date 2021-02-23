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
package com.streamsets.pipeline.lib.parser.delimited;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.ProtoConfigurableEntity;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ext.io.OverrunReader;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.CsvParser;
import com.streamsets.pipeline.config.CsvRecordType;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.util.DelimitedDataConstants;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;
import org.apache.commons.csv.CSVFormat;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DelimitedDataParserFactory extends DataParserFactory {

  public static final Map<String, Object> CONFIGS = ImmutableMap.<String, Object>builder()
      .put(DelimitedDataConstants.PARSER, CsvParser.UNIVOCITY.name())
      .put(DelimitedDataConstants.DELIMITER_CONFIG, '|')
      .put(DelimitedDataConstants.ESCAPE_CONFIG, '\\')
      .put(DelimitedDataConstants.QUOTE_CONFIG, '"')
      .put(DelimitedDataConstants.SKIP_START_LINES, 0)
      .put(DelimitedDataConstants.PARSE_NULL, false)
      .put(DelimitedDataConstants.NULL_CONSTANT, "\\\\N")
      .put(DelimitedDataConstants.COMMENT_ALLOWED_CONFIG, false)
      .put(DelimitedDataConstants.COMMENT_MARKER_CONFIG, '#')
      .put(DelimitedDataConstants.IGNORE_EMPTY_LINES_CONFIG, true)
      .put(DelimitedDataConstants.ALLOW_EXTRA_COLUMNS, false)
      .put(DelimitedDataConstants.EXTRA_COLUMN_PREFIX, DelimitedDataConstants.DEFAULT_EXTRA_COLUMN_PREFIX)
      .put(DelimitedDataConstants.UNIVOCITY_FIELD_SEPARATOR, ",")
      .put(DelimitedDataConstants.UNIVOCITY_QUOTE, '"')
      .put(DelimitedDataConstants.UNIVOCITY_ESCAPE, '\\')
      .put(DelimitedDataConstants.UNIVOCITY_MAX_COLUMNS, 1000)
      .put(DelimitedDataConstants.UNIVOCITY_MAX_CHARS_PER_COLUMN, 1000)
      .put(DelimitedDataConstants.UNIVOCITY_SKIP_EMPTY_LINES, true)
      .put(DelimitedDataConstants.UNIVOCITY_COMMENT_CHAR, '\0')
      .put(DelimitedDataConstants.UNIVOCITY_LINE_SEPARATOR, "\n")
      .put(
          DelimitedDataConstants.MULTI_CHARACTER_FIELD_DELIMITER_CONFIG,
          DelimitedDataConstants.DEFAULT_MULTI_CHARACTER_FIELD_DELIMITER
      )
      .put(
          DelimitedDataConstants.MULTI_CHARACTER_LINE_DELIMITER_CONFIG,
          DelimitedDataConstants.DEFAULT_MULTI_CHARACTER_LINE_DELIMITER
      )
      .build();

  public static final Set<Class<? extends Enum>> MODES =
      ImmutableSet.of((Class<? extends Enum>) CsvMode.class, CsvHeader.class, CsvRecordType.class);

  // Note - Java Reader translates the 3 byte UTF-8 0xef 0xbb 0xbf prefix to a single character, \ufeff
  static final char BOM = '\ufeff';

  public DelimitedDataParserFactory(Settings settings) {
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

    DelimitedDataParserSettings.Builder builder = DelimitedDataParserSettings.builder();

    CSVFormat csvFormat;
    switch (getSettings().getMode(CsvMode.class)) {
      case CUSTOM:
        csvFormat = CSVFormat.DEFAULT.withDelimiter(getSettings().getConfig(DelimitedDataConstants.DELIMITER_CONFIG))
            .withEscape((char) getSettings().getConfig(DelimitedDataConstants.ESCAPE_CONFIG))
            .withQuote((char) getSettings().getConfig(DelimitedDataConstants.QUOTE_CONFIG))
            .withIgnoreEmptyLines(getSettings().getConfig(DelimitedDataConstants.IGNORE_EMPTY_LINES_CONFIG));

        if ((boolean) getSettings().getConfig(DelimitedDataConstants.COMMENT_ALLOWED_CONFIG)) {
          csvFormat = csvFormat.withCommentMarker((char) getSettings().getConfig(
              DelimitedDataConstants.COMMENT_MARKER_CONFIG)
          );
        }
        break;
      case MULTI_CHARACTER:
        csvFormat = null;
        builder = builder.withMultiCharacterFieldDelimiter(
            getSettings().getConfig(DelimitedDataConstants.MULTI_CHARACTER_FIELD_DELIMITER_CONFIG)
        ).withMultiCharEscapeChar(
            getSettings().getConfig(DelimitedDataConstants.ESCAPE_CONFIG)
        ).withMultiCharQuoteChar(
            getSettings().getConfig(DelimitedDataConstants.QUOTE_CONFIG)
        ).withMultiCharacterLineDelimiter(
            getSettings().getConfig(DelimitedDataConstants.MULTI_CHARACTER_LINE_DELIMITER_CONFIG
        ));
        break;
      default:
        csvFormat = getSettings().getMode(CsvMode.class).getFormat();
        break;
    }

    try {
      DelimitedDataParserSettings settings = builder
          .withParser(CsvParser.valueOf(getSettings().getConfig(DelimitedDataConstants.PARSER)))
          .withSkipStartLines(getSettings().getConfig(DelimitedDataConstants.SKIP_START_LINES))
          .withFormat(csvFormat)
          .withHeader(getSettings().getMode(CsvHeader.class))
          .withMaxObjectLen(getSettings().getMaxRecordLen())
          .withRecordType(getSettings().getMode(CsvRecordType.class))
          .withParseNull(getSettings().getConfig(DelimitedDataConstants.PARSE_NULL))
          .withNullConstant(getSettings().getConfig(DelimitedDataConstants.NULL_CONSTANT))
          .withAllowExtraColumns(getSettings().getConfig(DelimitedDataConstants.ALLOW_EXTRA_COLUMNS))
          .withExtraColumnPrefix(getSettings().getConfig(DelimitedDataConstants.EXTRA_COLUMN_PREFIX))
          .withUnivocityFieldSeparator(getSettings().getConfig(DelimitedDataConstants.UNIVOCITY_FIELD_SEPARATOR))
          .withUnivocityQuote(getSettings().getConfig(DelimitedDataConstants.UNIVOCITY_QUOTE))
          .withUnivocityEscape(getSettings().getConfig(DelimitedDataConstants.UNIVOCITY_ESCAPE))
          .withUnivocityMaxColumns(getSettings().getConfig(DelimitedDataConstants.UNIVOCITY_MAX_COLUMNS))
          .withUnivocityMaxCharsPerColumn(getSettings().getConfig(DelimitedDataConstants.UNIVOCITY_MAX_CHARS_PER_COLUMN))
          .withUnivocitySkipEmptyLines(getSettings().getConfig(DelimitedDataConstants.UNIVOCITY_SKIP_EMPTY_LINES))
          .withUnivocityCommentChar(getSettings().getConfig(DelimitedDataConstants.UNIVOCITY_COMMENT_CHAR))
          .withUnivocityLineSeparator(getSettings().getConfig(DelimitedDataConstants.UNIVOCITY_LINE_SEPARATOR))
          .build();

      if (getSettings().getCharset().name().equals("UTF-8")) {
        // Consume BOM if it's there
        reader.mark(1);
        if (BOM != (char) reader.read()) {
          reader.reset();
        }
      }

      return new DelimitedCharDataParser(getSettings().getContext(), id, reader, offset, settings);
    } catch (IOException ex) {
      throw new DataParserException(Errors.DELIMITED_PARSER_00, id, offset, ex.toString(), ex);
    }
  }
}
