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

import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvParser;
import com.streamsets.pipeline.config.CsvRecordType;
import org.apache.commons.csv.CSVFormat;

public class DelimitedDataParserSettings {
  private CsvParser parser;
  private int skipStartLines;
  private CSVFormat format;
  private CsvHeader header;
  private int maxObjectLen;
  private CsvRecordType recordType;
  private boolean parseNull;
  private String nullConstant;
  private boolean allowExtraColumns;
  private String extraColumnPrefix;
  private char multiCharacterQuoteChar;
  private char multiCharacterEscapeChar;
  private String multiCharacterFieldDelimiter;
  private String multiCharacterLineDelimiter;

  // Univocity Parser configs
  private String univocityFieldSeparator;
  private char univocityEscape;
  private char univocityQuote;
  private int univocityMaxColumns;
  private int univocityMaxCharsPerColumn;
  private boolean univocitySkipEmptyLines;
  private char univocityCommentChar;
  private String univocityLineSeparator;

  public int getSkipStartLines() {
    return skipStartLines;
  }

  public CsvParser getParser() {
    return parser;
  }

  public CSVFormat getFormat() {
    return format;
  }

  public void setFormat(CSVFormat format) {
    this.format = format;
  }

  public CsvHeader getHeader() {
    return header;
  }

  public int getMaxObjectLen() {
    return maxObjectLen;
  }

  public CsvRecordType getRecordType() {
    return recordType;
  }

  public boolean parseNull() {
    return parseNull;
  }

  public String getNullConstant() {
    return nullConstant;
  }

  public boolean allowExtraColumns() {
    return allowExtraColumns;
  }

  public String getExtraColumnPrefix() {
    return extraColumnPrefix;
  }

  public char getMultiCharacterQuoteChar() {
    return multiCharacterQuoteChar;
  }

  public char getMultiCharacterEscapeChar() {
    return multiCharacterEscapeChar;
  }

  public String getMultiCharacterFieldDelimiter() {
    return multiCharacterFieldDelimiter;
  }

  public String getMultiCharacterLineDelimiter() {
    return multiCharacterLineDelimiter;
  }

  public String getUnivocityFieldSeparator() {
    return univocityFieldSeparator;
  }

  public char getUnivocityEscape() {
    return univocityEscape;
  }

  public char getUnivocityQuote() {
    return univocityQuote;
  }

  public int getUnivocityMaxColumns() {
    return univocityMaxColumns;
  }

  public int getUnivocityMaxCharsPerColumn() {
    return univocityMaxCharsPerColumn;
  }

  public boolean getUnivocitySkipEmptyLines() {
    return univocitySkipEmptyLines;
  }

  public char getUnivocityCommentChar() {
    return univocityCommentChar;
  }

  public String getUnivocityLineSeparator() {
    return univocityLineSeparator;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private CsvParser parser;
    private int skipStartLines;
    private CSVFormat format;
    private CsvHeader header;
    private int maxObjectLen;
    private CsvRecordType recordType;
    private boolean parseNull;
    private String nullConstant;
    private boolean allowExtraColumns;
    private String extraColumnPrefix;

    private char multiCharacterQuoteChar;
    private char multiCharacterEscapeChar;
    private String multiCharacterFieldDelimiter;
    private String multiCharacterLineDelimiter;

    // Univocity Parser configs
    private String univocityFieldSeparator;
    private char univocityEscape;
    private char univocityQuote;
    private int univocityMaxColumns;
    private int univocityMaxCharsPerColumn;
    private boolean univocitySkipEmptyLines;
    private char univocityCommentChar;
    private String univocityLineSeparator;

    private Builder() {
    }

    public Builder withParser(CsvParser parser) {
      this.parser = parser;
      return this;
    }

    public Builder withSkipStartLines(int skipStartLines) {
      this.skipStartLines = skipStartLines;
      return this;
    }

    public Builder withFormat(CSVFormat format) {
      this.format = format;
      return this;
    }

    public Builder withHeader(CsvHeader header) {
      this.header = header;
      return this;
    }

    public Builder withMaxObjectLen(int maxObjectLen) {
      this.maxObjectLen = maxObjectLen;
      return this;
    }

    public Builder withRecordType(CsvRecordType recordType) {
      this.recordType = recordType;
      return this;
    }

    public Builder withParseNull(boolean parseNull) {
      this.parseNull = parseNull;
      return this;
    }

    public Builder withNullConstant(String nullConstant) {
      this.nullConstant = nullConstant;
      return this;
    }

    public Builder withAllowExtraColumns(boolean allowExtraColumns) {
      this.allowExtraColumns = allowExtraColumns;
      return this;
    }

    public Builder withExtraColumnPrefix(String extraColumnPrefix) {
      this.extraColumnPrefix = extraColumnPrefix;
      return this;
    }

    public Builder withMultiCharQuoteChar(char quoteChar) {
      this.multiCharacterQuoteChar = quoteChar;
      return this;
    }

    public Builder withMultiCharEscapeChar(char escapeChar) {
      this.multiCharacterEscapeChar = escapeChar;
      return this;
    }

    public Builder withMultiCharacterFieldDelimiter(String fieldDelimiter) {
      this.multiCharacterFieldDelimiter = fieldDelimiter;
      return this;
    }

    public Builder withMultiCharacterLineDelimiter(String lineDelimiter) {
      this.multiCharacterLineDelimiter = lineDelimiter;
      return this;
    }

    public Builder withUnivocityFieldSeparator(String separator) {
      this.univocityFieldSeparator = separator;
      return this;
    }

    public Builder withUnivocityMaxColumns(int max) {
      this.univocityMaxColumns = max;
      return this;
    }

    public Builder withUnivocityMaxCharsPerColumn(int max) {
      this.univocityMaxCharsPerColumn = max;
      return this;
    }

    public Builder withUnivocityEscape(char character) {
      this.univocityEscape = character;
      return this;
    }

    public Builder withUnivocityQuote(char character) {
      this.univocityQuote = character;
      return this;
    }

    public Builder withUnivocitySkipEmptyLines(boolean skip) {
      this.univocitySkipEmptyLines = skip;
      return this;
    }

    public Builder withUnivocityCommentChar(char comment) {
      this.univocityCommentChar = comment;
      return this;
    }

    public Builder withUnivocityLineSeparator(String lineSeparator) {
      this.univocityLineSeparator = lineSeparator;
      return this;
    }

    public DelimitedDataParserSettings build() {
      DelimitedDataParserSettings delimitedDataParserSettings = new DelimitedDataParserSettings();
      delimitedDataParserSettings.parser = this.parser;
      delimitedDataParserSettings.header = this.header;
      delimitedDataParserSettings.allowExtraColumns = this.allowExtraColumns;
      delimitedDataParserSettings.maxObjectLen = this.maxObjectLen;
      delimitedDataParserSettings.recordType = this.recordType;
      delimitedDataParserSettings.skipStartLines = this.skipStartLines;
      delimitedDataParserSettings.nullConstant = this.nullConstant;
      delimitedDataParserSettings.extraColumnPrefix = this.extraColumnPrefix;
      delimitedDataParserSettings.format = this.format;
      delimitedDataParserSettings.parseNull = this.parseNull;
      delimitedDataParserSettings.multiCharacterFieldDelimiter = this.multiCharacterFieldDelimiter;
      delimitedDataParserSettings.multiCharacterLineDelimiter = this.multiCharacterLineDelimiter;
      delimitedDataParserSettings.multiCharacterQuoteChar = this.multiCharacterQuoteChar;
      delimitedDataParserSettings.multiCharacterEscapeChar = this.multiCharacterEscapeChar;
      delimitedDataParserSettings.univocityFieldSeparator = this.univocityFieldSeparator;
      delimitedDataParserSettings.univocityQuote = this.univocityQuote;
      delimitedDataParserSettings.univocityEscape = this.univocityEscape;
      delimitedDataParserSettings.univocityMaxColumns = this.univocityMaxColumns;
      delimitedDataParserSettings.univocityMaxCharsPerColumn = this.univocityMaxCharsPerColumn;
      delimitedDataParserSettings.univocitySkipEmptyLines = this.univocitySkipEmptyLines;
      delimitedDataParserSettings.univocityCommentChar = this.univocityCommentChar;
      delimitedDataParserSettings.univocityLineSeparator = this.univocityLineSeparator;
      return delimitedDataParserSettings;
    }
  }
}
