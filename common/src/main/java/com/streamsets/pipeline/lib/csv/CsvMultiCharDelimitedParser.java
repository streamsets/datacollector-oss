/*
 * Copyright 2018 StreamSets Inc.
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

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.io.AbstractOverrunDelimitedReader;
import com.streamsets.pipeline.lib.io.OverrunCustomDelimiterReader;
import com.streamsets.pipeline.lib.io.OverrunLineReader;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.jparsec.Parser;
import org.jparsec.Parsers;
import org.jparsec.error.ParserException;
import org.jparsec.pattern.CharPredicates;
import org.jparsec.pattern.Pattern;
import org.jparsec.pattern.Patterns;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.util.List;

/**
 * Adapted from https://github.com/koo-inc/fm-csv-mapper/blob/master/src/main/java/jp/co/freemind/csv/internal/CsvLineParser.java
 */
public class CsvMultiCharDelimitedParser implements DelimitedDataParser {
  private static final Logger LOG = LoggerFactory.getLogger(CsvMultiCharDelimitedParser.class);
  private final Reader inputReader;
  private final AbstractOverrunDelimitedReader wrappingReader;
  private final Parser<List<String>> parser;
  private final char quoteChar;
  private final char escapeChar;
  private final String fieldSeparator;
  private final String lineSeparator;
  private final StringBuilder inputBuffer = new StringBuilder();
  private final int maxInputBufferSize;
  private final int maxRecordSize;
  private final String[] headers;
  private final long readerPositionAfterHeaders;

  public static CsvMultiCharDelimitedParser createNonReaderParser(char quoteChar, char escapeChar, String fieldSeparator) {
    return new CsvMultiCharDelimitedParser(null, quoteChar, escapeChar, fieldSeparator, 4096, -1, false, 0l, 0, "\n");
  }

  public static CsvMultiCharDelimitedParser createNonReaderParser(
      char quoteChar,
      char escapeChar,
      String fieldSeparator,
      int maxInputBufferSize
  ) {
    return new CsvMultiCharDelimitedParser(
        null,
        quoteChar,
        escapeChar,
        fieldSeparator,
        maxInputBufferSize,
        -1,
        false,
        0l,
        0,
        "\n"
    );
  }

  public CsvMultiCharDelimitedParser(
      Reader reader,
      char quoteChar,
      char escapeChar,
      String fieldSeparator,
      int maxInputBufferSize,
      int maxRecordSize,
      boolean headerRow,
      long initialPosition,
      int skipStartLines,
      String lineSeparator
  ) {

    this.inputReader = reader;
    if (inputReader != null) {
      if (StringUtils.equals(lineSeparator, "\n") || StringUtils.equals(lineSeparator, "\r\n")) {
        // we can use the OverrunLineReader implementation, since it will handle such line endings
        this.wrappingReader = new OverrunLineReader(inputReader, maxRecordSize, quoteChar, escapeChar);
      } else {
        // we should use the OverrunCustomDelimiterReader to handle arbitrary line endings
        this.wrappingReader = new OverrunCustomDelimiterReader(inputReader, maxRecordSize, lineSeparator, false, quoteChar, escapeChar);
      }
    } else {
      this.wrappingReader = null;
    }

    this.quoteChar = quoteChar;
    this.escapeChar = escapeChar;
    this.fieldSeparator = fieldSeparator;
    this.maxInputBufferSize = maxInputBufferSize;
    this.maxRecordSize = maxRecordSize;

    this.lineSeparator = lineSeparator;

    this.parser = fields();

    if (initialPosition == 0) {
      if (headerRow) {
        Utils.checkNotNull(this.wrappingReader, "wrappingReader");
        try {
          headers = read();
        } catch (IOException e) {
          throw new RuntimeException(String.format(
              "IOException attempting to parse header row: %s",
              e.getMessage()
          ), e);
        }
      } else {
        headers = null;
      }

      for (int i=0; i<skipStartLines; i++) {
        try {
          final int numRead = wrappingReader.readLine(new StringBuilder());
          if (LOG.isTraceEnabled()) {
            LOG.trace(
                "Read {} bytes in skipping line number {}",
                numRead,
                i
            );
          }
        } catch (IOException e) {
          throw new RuntimeException(String.format(
              "IOException attempting to skip %d out of %d lines in reader: %s",
              i,
              skipStartLines,
              e.getMessage()
          ), e);
        }
      }
    } else {
      if (headerRow) {
        try {
          headers = read();
          initialPosition -= getUnadjustedReaderPosition();
        } catch (IOException e) {
          throw new RuntimeException(String.format(
              "IOException attempting to parse header row: %s",
              e.getMessage()
          ), e);
        }
      } else {
        headers = null;
      }
      try {
        IOUtils.skipFully(wrappingReader, initialPosition);
      } catch (IOException e) {
        throw new RuntimeException(String.format(
            "IOException attempting to call IOUtils.skipFully to skip %d bytes (initialPosition): %s",
            initialPosition,
            e.getMessage()
        ), e);
      }
    }

    readerPositionAfterHeaders = getUnadjustedReaderPosition();
  }

  @Override
  public String[] getHeaders() throws IOException {
    return headers;
  }

  @Override
  public String[] read() throws IOException {
    Utils.checkState(
        wrappingReader != null,
        "wrappingReader is null"
    );
    wrappingReader.mark(maxInputBufferSize);
    final int numRead = wrappingReader.readLine(inputBuffer);
    if (LOG.isTraceEnabled()) {
      LOG.trace(
          "Read {} characters in call to wrappingReader.readLine; current inputBuffer: {}",
          numRead,
          inputBuffer.toString()
      );
    }
    if (numRead < 0) {
      return null;
    }
    try {
      final List<String> parsed = parser.parse(inputBuffer.toString());
      if (parsed == null) {
        wrappingReader.reset();
        return null;
      } else {
        inputBuffer.setLength(0);
        return parsed.toArray(new String[parsed.size()]);
      }
    } catch (ParserException e) {
      wrappingReader.reset();
      throw e;
    }
  }

  @Override
  public long getReaderPosition() {
    return getUnadjustedReaderPosition() - readerPositionAfterHeaders;
  }

  public long getUnadjustedReaderPosition() {
    return wrappingReader != null ? wrappingReader.getPos() : 0l;
  }

  @Override
  public void close() throws IOException {
    if (wrappingReader != null) {
      wrappingReader.close();
    }
  }

  public List<String> parseStandaloneLine(String line) throws ParserException {
    String tryParse;
    final int inputBufferSize = inputBuffer.length();
    if (inputBufferSize > maxInputBufferSize) {
      throw new RuntimeException(String.format(
          "Input buffer size was %d, but max size is %d; failing with input seen so far: %s",
          inputBufferSize,
          maxInputBufferSize,
          inputBuffer.toString()
      ));
    } else if (inputBufferSize > 0) {
      tryParse = inputBuffer.toString() + line;
    } else {
      tryParse = line;
    }
    if (tryParse == null) {
      return null;
    }
    try {
      final List<String> parseResult = parser.parse(tryParse);
      inputBuffer.setLength(0);
      return parseResult;
    } catch (ParserException e) {
      inputBuffer.append(line);
      inputBuffer.append(lineSeparator);
      return null;
    }
  }

  @VisibleForTesting
  Parser<List<String>> fields() {
    return field().sepBy(fieldSeparator().toScanner("field separator"));
  }

  @VisibleForTesting
  Parser<String> field() {
    String esc = java.util.regex.Pattern.quote(String.valueOf(escapeChar));
    String quot = java.util.regex.Pattern.quote(String.valueOf(quoteChar));
    String fieldSep = java.util.regex.Pattern.quote(fieldSeparator);
    return quotedField()
        .map(s -> s.replaceAll(esc + "([" + esc + quot + "])", "$1"))
        .or(bareField())
        .map(s -> s.replaceAll(esc + "(" + fieldSep + ")", "$1"));
  }


  @VisibleForTesting
  Parser<String> quotedField() {
    Parser<Void> quote = quote().toScanner("quote");
    return Parsers.between(quote, quotedString().source(), quote);
  }


  @VisibleForTesting
  Parser<String> bareField() {
    return Patterns.sequence(Patterns.not(quote()),
        Patterns.sequence(escape(), fieldSeparator()).or(
            Patterns.and(
                Patterns.notString(fieldSeparator),
                Patterns.not(Patterns.sequence(Patterns.not(escape()), quote())),
                Patterns.not(lineSeparator())
            )
        )
        .many())
        .toScanner("bare field").source();
  }


  @VisibleForTesting
  Parser<Void> quotedString() {
    return Patterns.or(
        Patterns.sequence(escape(), quote()),
        Patterns.sequence(escape(), escape()),
        Patterns.sequence(escape(), fieldSeparator()),
        Patterns.many(
            CharPredicates.and(
                CharPredicates.notChar(quoteChar),
                CharPredicates.notChar(escapeChar)
            )
        )
    ).many().toScanner("quoted string");
  }

  @VisibleForTesting
  Pattern quote() {
    return Patterns.isChar(quoteChar);
  }

  @VisibleForTesting
  Pattern escape() {
    return Patterns.isChar(escapeChar);
  }

  @VisibleForTesting
  Pattern fieldSeparator() {
    return Patterns.string(fieldSeparator);
  }

  @VisibleForTesting
  Pattern lineSeparator() {
    return Patterns.string(lineSeparator);
  }


}