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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.ProtoConfigurableEntity;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.ext.io.OverrunReader;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvRecordType;
import com.streamsets.pipeline.lib.csv.CsvMultiCharDelimitedParser;
import com.streamsets.pipeline.lib.csv.CsvUnivocityDelimitedParser;
import com.streamsets.pipeline.lib.csv.DelimitedDataParser;
import com.streamsets.pipeline.lib.csv.OverrunCsvParser;
import com.streamsets.pipeline.lib.parser.AbstractDataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.ParserRuntimeException;
import com.streamsets.pipeline.lib.parser.RecoverableDataParserException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DelimitedCharDataParser extends AbstractDataParser {
  private final ProtoConfigurableEntity.Context context;
  private final String readerId;
  private final DelimitedDataParser parser;
  private final DelimitedDataParserSettings settings;
  // Instance wise counter of extra columns that will remember how many columns have been every added in case
  // that user checked 'Allow extra columns'
  private int extraColumnCounter = 1;

  private List<Field> headers;
  private boolean eof;

  /**
   * If the delimited parser is configured to work without header, we use numbers as indexes. Converting int to String
   * can however easily become very expensive operation when done on wide columns and thus we pre-compute String version
   * of first N indexes and store them statically in memory. Keeping few hundred additional strings is quite cheap for
   * the gains. We default to 1k indexes (e.g. 1 thousand wide CSV records).
   */
  private final static int PRECOMPUTED_INDEXES_SIZE = Integer.parseInt(System.getProperty("com.streamsets.pipeline.lib.parser.delimited.DelimitedCharDataParser.PRECOMPUTED_INDEXES_SIZE", "1000"));
  private final static String []PRECOMPUTED_INDEXES;
  static {
    PRECOMPUTED_INDEXES = new String[PRECOMPUTED_INDEXES_SIZE];
    for(int i = 0; i < PRECOMPUTED_INDEXES_SIZE; i++) {
      PRECOMPUTED_INDEXES[i] = String.valueOf(i);
    }
  }

  public DelimitedCharDataParser(
      ProtoConfigurableEntity.Context context,
      String readerId,
      OverrunReader reader,
      long readerOffset,
      DelimitedDataParserSettings settings
  ) throws IOException {
    this.context = context;
    this.readerId = readerId;
    this.settings = settings;
    final CsvHeader header = settings.getHeader();

    switch(settings.getParser()) {
      case UNIVOCITY:
        parser = new CsvUnivocityDelimitedParser(
            reader,
            settings.getUnivocityQuote(),
            settings.getUnivocityEscape(),
            settings.getUnivocityFieldSeparator(),
            settings.getUnivocityCommentChar(),
            settings.getUnivocityLineSeparator(),
            EnumSet.of(CsvHeader.WITH_HEADER, CsvHeader.IGNORE_HEADER).contains(header),
            settings.getSkipStartLines(),
            settings.getUnivocityMaxColumns(),
            settings.getUnivocityMaxCharsPerColumn(),
            settings.getUnivocitySkipEmptyLines(),
            readerOffset
        );
        break;
      case LEGACY_PARSER:
        parser = getLegacyParser(reader, readerOffset, settings, header);
        break;
      default:
        throw new IOException(Utils.format("Unknown parser {}", settings.getParser()));
    }

    String[] hs = parser.getHeaders();
    if (header != CsvHeader.IGNORE_HEADER && hs != null) {
      headers = new ArrayList<>();
      for (String h : hs) {
        headers.add(Field.create(h));
      }
    }
  }

  private DelimitedDataParser getLegacyParser(
      OverrunReader reader,
      long readerOffset,
      DelimitedDataParserSettings settings,
      CsvHeader header
  ) throws IOException {
    // Resolving the legacy "parser" isn't as easy as it's two different underlying parsers that are selected per
    // config combination...
    if (!Strings.isNullOrEmpty(settings.getMultiCharacterFieldDelimiter())) {
      // use multi-character delimiter parser
      return new CsvMultiCharDelimitedParser(reader,
          settings.getMultiCharacterQuoteChar(),
          settings.getMultiCharacterEscapeChar(),
          settings.getMultiCharacterFieldDelimiter(),
          // we will make maxInputBufferSize equal to max record size, since that seems sensible
          settings.getMaxObjectLen(),
          settings.getMaxObjectLen(),
          EnumSet.of(CsvHeader.WITH_HEADER, CsvHeader.IGNORE_HEADER).contains(header), readerOffset,
          settings.getSkipStartLines(),
          settings.getMultiCharacterLineDelimiter()
      );
    } else {
      switch (header) {
        case WITH_HEADER:
        case IGNORE_HEADER:
          settings.setFormat(settings.getFormat().withHeader((String[])null).withSkipHeaderRecord(true));
          break;
        case NO_HEADER:
          settings.setFormat(settings.getFormat().withHeader((String[])null).withSkipHeaderRecord(false));
          break;
        default:
          throw new ParserRuntimeException(Utils.format("Unknown header error: {}", header));
      }
      return new OverrunCsvParser(reader,
          settings.getFormat(), readerOffset,
          settings.getSkipStartLines(),
          settings.getMaxObjectLen()
      );
    }
  }

  @Override
  public Record parse() throws IOException, DataParserException {
    Record record = null;
    long offset = parser.getReaderPosition();
    String[] columns = parser.read();
    if (columns != null) {
      record = createRecord(offset, columns);
    } else {
      eof = true;
    }
    return record;
  }

  protected Record createRecord(long offset, String[] columns) throws DataParserException {
    Record record = context.createRecord(readerId + "::" + offset);

    if(headers != null && settings.allowExtraColumns()) {
      int numColumns = columns.length;
      int numHeaders = headers.size();
      while (numHeaders < numColumns) {
        headers.add(Field.create(String.format("%s%02d", settings.getExtraColumnPrefix(), extraColumnCounter++)));
        ++numHeaders;
      }
    }

    // In case that the number of columns does not equal the number of expected columns from header, report the
    // parsing error as recoverable issue - it's safe to continue reading the stream.
    if(headers != null && columns.length > headers.size()) {
      record.set(Field.create(Field.Type.MAP, ImmutableMap.builder()
        .put("columns", getListField(columns))
        .put("headers", Field.create(Field.Type.LIST, headers))
        .build()
      ));

      throw new RecoverableDataParserException(record, Errors.DELIMITED_PARSER_01, offset, columns.length, headers.size());
    }

    if(settings.getRecordType() == CsvRecordType.LIST) {
      List<Field> row = new ArrayList<>();
      for (int i = 0; i < columns.length; i++) {
        Map<String, Field> cell = new HashMap<>();
        Field header = (headers != null) ? headers.get(i) : null;
        if (header != null) {
          cell.put("header", header);
        }
        Field value = getField(columns[i]);
        cell.put("value", value);
        row.add(Field.create(cell));
      }
      record.set(Field.create(row));
    } else {
      LinkedHashMap<String, Field> listMap = new LinkedHashMap<>(columns.length);
      for (int i = 0; i < columns.length; i++) {
        String key;
        Field header = (headers != null) ? headers.get(i) : null;
        if(header != null) {
          key = header.getValueAsString();
        } else {
          if(i < PRECOMPUTED_INDEXES_SIZE) {
            key = PRECOMPUTED_INDEXES[i];
          } else {
            key = Integer.toString(i);
          }
        }
        listMap.put(key, getField(columns[i]));
      }
      record.set(Field.createListMap(listMap));
    }

    return record;
  }

  private Field getListField(String... values) {
    ImmutableList.Builder<Field> listBuilder = ImmutableList.builder();
    for(String value : values) {
      listBuilder.add(Field.create(Field.Type.STRING, value));
    }

    return Field.create(Field.Type.LIST, listBuilder.build());
  }

  private Field getField(String value) {
    if(settings.parseNull() && settings.getNullConstant() != null && settings.getNullConstant().equals(value)) {
      return Field.create(Field.Type.STRING, null);
    }

    return Field.create(Field.Type.STRING, value);
  }

  @Override
  public String getOffset() {
    return eof ? String.valueOf(-1) : String.valueOf(parser.getReaderPosition());
  }

  @Override
  public void close() throws IOException {
    parser.close();
  }

}
