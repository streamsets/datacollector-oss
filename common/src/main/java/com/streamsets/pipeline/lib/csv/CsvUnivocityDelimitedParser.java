/*
 * Copyright 2021 StreamSets Inc.
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
package com.streamsets.pipeline.lib.csv;

import com.streamsets.pipeline.api.impl.Utils;
import com.univocity.parsers.common.TextParsingException;
import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;

public class CsvUnivocityDelimitedParser implements DelimitedDataParser {
  private static final Logger LOG = LoggerFactory.getLogger(CsvUnivocityDelimitedParser.class);
  private final CsvParser parser;
  private final String[] headers;

  public CsvUnivocityDelimitedParser(
      Reader reader,
      char quoteChar,
      char escapeChar,
      String fieldSeparator,
      char commentChar,
      String lineSeparator,
      boolean headerRow,
      int skipStartLines,
      int maxColumns,
      int maxCharsPerColumn,
      boolean skipEmptyLines,
      long initialPosition
  ) throws IOException {
    LOG.debug("Using Univocity CSV Parser");

    // Passing configuration down to Univocity
    CsvFormat csvFormat = new CsvFormat();
    csvFormat.setDelimiter(fieldSeparator);
    csvFormat.setQuote(quoteChar);
    csvFormat.setQuoteEscape(escapeChar);
    csvFormat.setComment(commentChar);
    csvFormat.setLineSeparator(lineSeparator);

    CsvParserSettings settings = new CsvParserSettings();
    settings.setFormat(csvFormat);
    settings.setHeaderExtractionEnabled(headerRow);
    settings.setMaxColumns(maxColumns);
    settings.setMaxCharsPerColumn(maxCharsPerColumn);
    settings.setSkipEmptyLines(skipEmptyLines);

    if (skipStartLines > 0) {
      settings.setNumberOfRowsToSkip(skipStartLines);
    }

    this.parser = new CsvParser(settings);
    parser.beginParsing(reader);

    if(headerRow) {
      try {
        this.headers = parser.getContext().parsedHeaders();
      } catch (TextParsingException e) {
        throw new IOException(Utils.format("Failed to read headers: {}", e.toString()), e);
      }
    } else {
      this.headers = null;
    }

    // If we are restarting, let's seek to the right spot. Sadly univocity doesn't have ability to "seek" at offset,
    // only to read and count either lines or characters which we will do here. We will skip lines until we get to
    // the required offset
    if(initialPosition != 0) {
      // Univocity have unfortunate behavior - it calling parser.getContext().skipLines() will result in noop if the
      // parser reached end of file. And there doesn't seem to be a method that would check for EOF, so we do that
      // by ensuring that the position has moved on each iteration.
      long lastLocation = -1;
      while(getReaderPosition() < initialPosition && lastLocation != getReaderPosition()) {
        lastLocation = getReaderPosition();
        parser.getContext().skipLines(1);
      }

      if(initialPosition != getReaderPosition()) {
        throw new RuntimeException(Utils.format(
            "Initial position {} isn't valid offset of a new line, closest new records starts on location {}",
            initialPosition,
            getReaderPosition()
        ));
      }
    }
  }

  @Override
  public String[] getHeaders() throws IOException {
    return headers;
  }

  @Override
  public String[] read() throws IOException {
    try {
      return parser.parseNext();
    } catch (TextParsingException e) {
      throw new IOException(Utils.format("Failed to parse next record: {}", e.toString()), e);
    }
  }

  @Override
  public long getReaderPosition() {
    return parser.getContext().currentChar();
  }

  @Override
  public void close() throws IOException {
    if(parser != null) {
      parser.stopParsing();
    }
  }
}
