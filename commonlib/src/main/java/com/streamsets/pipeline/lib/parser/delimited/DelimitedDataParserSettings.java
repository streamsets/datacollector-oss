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
import com.streamsets.pipeline.config.CsvRecordType;
import org.apache.commons.csv.CSVFormat;

public class DelimitedDataParserSettings {
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

  public int getSkipStartLines() {
    return skipStartLines;
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

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
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

    private Builder() {
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

    public DelimitedDataParserSettings build() {
      DelimitedDataParserSettings delimitedDataParserSettings = new DelimitedDataParserSettings();
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
      return delimitedDataParserSettings;
    }
  }
}
