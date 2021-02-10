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

import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class CsvUnivocityDelimitedParserTest {

  @Test
  public void basicTest() throws IOException {
    String simpleCsv = "A,B,C\n1,2,3";

    CsvUnivocityDelimitedParser parser = new ParserBuilder(simpleCsv)
        .build();

    String[] row = parser.read();
    assertEquals(3, row.length);
    assertEquals("1", row[0]);
    assertEquals("2", row[1]);
    assertEquals("3", row[2]);

    String[] headers = parser.getHeaders();
    assertEquals(3, headers.length);
    assertEquals("A", headers[0]);
    assertEquals("B", headers[1]);
    assertEquals("C", headers[2]);

    parser.close();
  }

  @Test
  public void quoteInData() throws IOException {
    String simpleCsv = "A,B,C\n1,\"2,Z\",3";

    CsvUnivocityDelimitedParser parser = new ParserBuilder(simpleCsv)
        .withQuoteChar('"')
        .build();

    String[] row = parser.read();
    assertEquals(3, row.length);
    assertEquals("1", row[0]);
    assertEquals("2,Z", row[1]);
    assertEquals("3", row[2]);

    parser.close();
  }

  @Test
  public void quoteInHeader() throws IOException {
    String simpleCsv = "A,\"B,Z\",C\n1,2,3";

    CsvUnivocityDelimitedParser parser = new ParserBuilder(simpleCsv)
        .withQuoteChar('"')
        .build();

    String[] headers = parser.getHeaders();
    assertEquals(3, headers.length);
    assertEquals("A", headers[0]);
    assertEquals("B,Z", headers[1]);
    assertEquals("C", headers[2]);

    parser.close();
  }

  @Test
  public void escapeInData() throws IOException {
    String simpleCsv = "A,B,C\n1,\"2,\\\"Z\",3";

    CsvUnivocityDelimitedParser parser = new ParserBuilder(simpleCsv)
        .withQuoteChar('"')
        .withEscapeChar('\\')
        .build();

    String[] row = parser.read();
    assertEquals(3, row.length);
    assertEquals("1", row[0]);
    assertEquals("2,\"Z", row[1]);
    assertEquals("3", row[2]);

    parser.close();
  }

  @Test
  public void nonTypicalQuoteAndEscapeInData() throws IOException {
    String simpleCsv = "A,B,C\n1,a2,xaZa,3";

    CsvUnivocityDelimitedParser parser = new ParserBuilder(simpleCsv)
        .withQuoteChar('a')
        .withEscapeChar('x')
        .build();

    String[] row = parser.read();
    assertEquals(3, row.length);
    assertEquals("1", row[0]);
    assertEquals("2,aZ", row[1]);
    assertEquals("3", row[2]);

    parser.close();
  }

  @Test
  public void escapeInHeader() throws IOException {
    String simpleCsv = "A,\"B,\\\"Z\",C\n1,2,3";

    CsvUnivocityDelimitedParser parser = new ParserBuilder(simpleCsv)
        .withQuoteChar('"')
        .withEscapeChar('\\')
        .build();

    String[] headers = parser.getHeaders();
    assertEquals(3, headers.length);
    assertEquals("A", headers[0]);
    assertEquals("B,\"Z", headers[1]);
    assertEquals("C", headers[2]);

    parser.close();
  }

  @Test
  public void fieldSeparatorSingleChar() throws IOException {
    String simpleCsv = "A;B;C\n1;2;3";

    CsvUnivocityDelimitedParser parser = new ParserBuilder(simpleCsv)
        .withFieldSeparator(";")
        .build();

    String[] row = parser.read();
    assertEquals(3, row.length);
    assertEquals("1", row[0]);
    assertEquals("2", row[1]);
    assertEquals("3", row[2]);

    parser.close();
  }

  @Test
  public void fieldSeparatorMultiChar() throws IOException {
    String simpleCsv = "A;;;B;;;C\n1;;;2;;;3";

    CsvUnivocityDelimitedParser parser = new ParserBuilder(simpleCsv)
        .withFieldSeparator(";;;")
        .build();

    String[] row = parser.read();
    assertEquals(3, row.length);
    assertEquals("1", row[0]);
    assertEquals("2", row[1]);
    assertEquals("3", row[2]);

    parser.close();
  }

  @Test
  public void parseNoHeader() throws IOException {
    String simpleCsv = "A,B,C\n1,2,3";

    CsvUnivocityDelimitedParser parser = new ParserBuilder(simpleCsv)
        .withHeaderRow(false)
        .build();

    String[] header = parser.getHeaders();
    assertNull(header);

    String[] row = parser.read();
    assertEquals(3, row.length);
    assertEquals("A", row[0]);
    assertEquals("B", row[1]);
    assertEquals("C", row[2]);

    row = parser.read();
    assertEquals(3, row.length);
    assertEquals("1", row[0]);
    assertEquals("2", row[1]);
    assertEquals("3", row[2]);
    parser.close();
  }

  @Test
  public void skipLines() throws IOException {
    String simpleCsv = "Garbage\nAnother Garbage\nA,B,C\n1,2,3";

    CsvUnivocityDelimitedParser parser = new ParserBuilder(simpleCsv)
        .withSkipLines(2)
        .build();

    String[] row = parser.read();
    assertEquals(3, row.length);
    assertEquals("1", row[0]);
    assertEquals("2", row[1]);
    assertEquals("3", row[2]);

    String[] headers = parser.getHeaders();
    assertEquals(3, headers.length);
    assertEquals("A", headers[0]);
    assertEquals("B", headers[1]);
    assertEquals("C", headers[2]);

    parser.close();
  }

  @Test(expected = IOException.class)
  public void maxColumnsInHeader() throws IOException {
    String simpleCsv = "A,B,C\n1,2";

    CsvUnivocityDelimitedParser parser = new ParserBuilder(simpleCsv)
        .withMaxColumns(2)
        .build();
  }

  @Test(expected = IOException.class)
  public void maxColumnsInData() throws IOException {
    String simpleCsv = "A,B\n1,2,3";

    CsvUnivocityDelimitedParser parser = new ParserBuilder(simpleCsv)
        .withMaxColumns(2)
        .build();

    parser.read();
  }

  @Test(expected = IOException.class)
  public void maxCharsPerColumnInData() throws IOException {
    String simpleCsv = "A,B,C\n1,way too long,3";

    CsvUnivocityDelimitedParser parser = new ParserBuilder(simpleCsv)
        .withMaxCharsPerColumn(1)
        .build();

    parser.read();
  }

  @Test
  public void recoverFromMaxCharsPerColumnInData() throws IOException {
    String simpleCsv = "A,B,C\n1,2,3\n1,way too long,3\n6,7,8";

    CsvUnivocityDelimitedParser parser = new ParserBuilder(simpleCsv)
        .withMaxCharsPerColumn(1)
        .build();

    String[] row = parser.read();
    assertEquals(3, row.length);
    assertEquals("1", row[0]);
    assertEquals("2", row[1]);
    assertEquals("3", row[2]);

    try {
      parser.read();
      fail();
    } catch (IOException e) {
      // Ignore expected exception
    }

    // This is also "bad" in the sense that the parser doesn't recover from reaching the limit
    row = parser.read();
    assertEquals(1, row.length);
    assertEquals("1", row[0]);

    assertNull(parser.read());
  }

  @Test
  public void recoverFromMaxColumnsInData() throws IOException {
    String simpleCsv = "A,B,C\n1,2,3\n10,20,30,40\n1,2,3";

    CsvUnivocityDelimitedParser parser = new ParserBuilder(simpleCsv)
        .withMaxColumns(3)
        .build();

    String[] row = parser.read();
    assertEquals(3, row.length);
    assertEquals("1", row[0]);
    assertEquals("2", row[1]);
    assertEquals("3", row[2]);

    try {
      parser.read();
      fail();
    } catch (IOException e) {
      // Ignore expected exception
    }

    try {
      parser.read();
      fail();
    } catch (IOException e) {
      // Ignore expected exception
      // This is actually "bad" in the sense that the parser isn't able to recover when max number of columns is breached
    }
  }

  @Test(expected = IOException.class)
  public void maxCharsPerColumnInHeader() throws IOException {
    String simpleCsv = "A,way too long,C\n1,2,3";

    CsvUnivocityDelimitedParser parser = new ParserBuilder(simpleCsv)
        .withMaxCharsPerColumn(1)
        .build();
  }

  @Test
  public void readerPositionWithGarbageAndHeader() throws IOException {
    String simpleCsv = "Garbage\nAnother Garbage\nA,B,C\n1,2,3\n4,5,6";

    CsvUnivocityDelimitedParser parser = new ParserBuilder(simpleCsv)
        .withSkipLines(2)
        .withHeaderRow(true)
        .build();

    assertNotNull(parser.getHeaders());
    assertEquals(30, parser.getReaderPosition());

    assertNotNull(parser.read());
    assertEquals(36, parser.getReaderPosition());

    assertNotNull(parser.read());
    assertEquals(41, parser.getReaderPosition());

    assertNull(parser.read());
    assertEquals(41, parser.getReaderPosition());

    parser.close();
  }

  @Test
  public void readerPositionWithGarbageAndWithoutHeader() throws IOException {
    String simpleCsv = "Garbage\nAnother Garbage\nA,B,C\n1,2,3\n4,5,6";

    CsvUnivocityDelimitedParser parser = new ParserBuilder(simpleCsv)
        .withSkipLines(2)
        .withHeaderRow(false)
        .build();

    assertNull(parser.getHeaders());
    assertEquals(24, parser.getReaderPosition());

    assertNotNull(parser.read());
    assertEquals(30, parser.getReaderPosition());

    assertNotNull(parser.read());
    assertEquals(36, parser.getReaderPosition());

    assertNotNull(parser.read());
    assertEquals(41, parser.getReaderPosition());

    assertNull(parser.read());
    assertEquals(41, parser.getReaderPosition());

    parser.close();
  }

  @Test
  public void initialPositionWithGarbageAndHeader() throws IOException {
    String simpleCsv = "Garbage\nAnother Garbage\nA,B,C\n1,2,3\n4,5,6";

    CsvUnivocityDelimitedParser parser = new ParserBuilder(simpleCsv)
        .withInitialPosition(36)
        .withSkipLines(2)
        .withHeaderRow(true)
        .build();
    assertEquals(36, parser.getReaderPosition());

    String[] headers = parser.getHeaders();
    assertNotNull(headers);
    assertEquals(3, headers.length);
    assertEquals("A", headers[0]);
    assertEquals("B", headers[1]);
    assertEquals("C", headers[2]);

    String[] row = parser.read();
    assertNotNull(row);
    assertEquals(3, row.length);
    assertEquals("4", row[0]);
    assertEquals("5", row[1]);
    assertEquals("6", row[2]);
    assertEquals(41, parser.getReaderPosition());

    assertNull(parser.read());
    assertEquals(41, parser.getReaderPosition());

    parser.close();
  }

  @Test
  public void initialPositionWithGarbageAndWithoutHeader() throws IOException {
    String simpleCsv = "Garbage\nAnother Garbage\nA,B,C\n1,2,3\n4,5,6";

    CsvUnivocityDelimitedParser parser = new ParserBuilder(simpleCsv)
        .withInitialPosition(36)
        .withSkipLines(2)
        .withHeaderRow(false)
        .build();
    assertEquals(36, parser.getReaderPosition());
    assertNull(parser.getHeaders());

    String[] row = parser.read();
    assertNotNull(row);
    assertEquals(3, row.length);
    assertEquals("4", row[0]);
    assertEquals("5", row[1]);
    assertEquals("6", row[2]);
    assertEquals(41, parser.getReaderPosition());

    assertNull(parser.read());
    assertEquals(41, parser.getReaderPosition());

    parser.close();
  }

  @Test(expected = RuntimeException.class)
  public void initialPositionInvalid() throws IOException {
    String simpleCsv = "A,B,C\n1,2,3\n4,5,6";

    CsvUnivocityDelimitedParser parser = new ParserBuilder(simpleCsv)
        .withInitialPosition(9)
        .withHeaderRow(false)
        .build();
  }

  @Test(expected = RuntimeException.class)
  public void initialPositionOutsideOfStreamBoundary() throws IOException {
    String simpleCsv = "A,B,C\n1,2,3\n4,5,6";

    CsvUnivocityDelimitedParser parser = new ParserBuilder(simpleCsv)
        .withInitialPosition(666)
        .withHeaderRow(false)
        .build();
  }

  @Test
  public void widerHeaderThenData() throws IOException {
    String simpleCsv = "A,B,C,D,E,F\n1,2,3";

    CsvUnivocityDelimitedParser parser = new ParserBuilder(simpleCsv)
        .build();

    String[] row = parser.read();
    assertEquals(3, row.length);
    assertEquals("1", row[0]);
    assertEquals("2", row[1]);
    assertEquals("3", row[2]);

    String[] headers = parser.getHeaders();
    assertEquals(6, headers.length);
    assertEquals("A", headers[0]);
    assertEquals("B", headers[1]);
    assertEquals("C", headers[2]);
    assertEquals("D", headers[3]);
    assertEquals("E", headers[4]);
    assertEquals("F", headers[5]);

    parser.close();
  }

  @Test
  public void widerDataThenHeader() throws IOException {
    String simpleCsv = "A,B,C\n1,2,3,4,5,6";

    CsvUnivocityDelimitedParser parser = new ParserBuilder(simpleCsv)
        .build();

    String[] row = parser.read();
    assertEquals(6, row.length);
    assertEquals("1", row[0]);
    assertEquals("2", row[1]);
    assertEquals("3", row[2]);
    assertEquals("4", row[3]);
    assertEquals("5", row[4]);
    assertEquals("6", row[5]);

    String[] headers = parser.getHeaders();
    assertEquals(3, headers.length);
    assertEquals("A", headers[0]);
    assertEquals("B", headers[1]);
    assertEquals("C", headers[2]);

    parser.close();
  }

  @Test
  public void skipEmptyLines() throws IOException {
    String simpleCsv = "\nA,B,C\n\n1,2,3\n\n4,5,6";

    CsvUnivocityDelimitedParser parser = new ParserBuilder(simpleCsv)
        .withSkipEmptyLines(true)
        .build();

    String[] headers = parser.getHeaders();
    assertEquals(3, headers.length);
    assertEquals("A", headers[0]);
    assertEquals("B", headers[1]);
    assertEquals("C", headers[2]);

    String[] row = parser.read();
    assertEquals(3, row.length);
    assertEquals("1", row[0]);
    assertEquals("2", row[1]);
    assertEquals("3", row[2]);

    row = parser.read();
    assertEquals(3, row.length);
    assertEquals("4", row[0]);
    assertEquals("5", row[1]);
    assertEquals("6", row[2]);

    assertNull(parser.read());

    parser.close();
  }

  // This is imho a bug in the library. If first line is empty and we're reading with header
  // the parser reads the whole file and never returns any rows. Worth trying to repro without
  // any StreamSets code later on.
  @Test
  public void skipEmptyLinesDisabledInHeader() throws IOException {
    String simpleCsv = "\nA,B,C\n1,2,3";

    CsvUnivocityDelimitedParser parser = new ParserBuilder(simpleCsv)
        .withSkipEmptyLines(false)
        .build();

    assertNull(parser.getHeaders());

    assertEquals(12, parser.getReaderPosition());
    assertNull(parser.read());
    assertEquals(12, parser.getReaderPosition());

    parser.close();
  }

  @Test
  public void skipEmptyLinesDisabled() throws IOException {
    String simpleCsv = "A,B,C\n\n1,2,3\n\n4,5,6";

    CsvUnivocityDelimitedParser parser = new ParserBuilder(simpleCsv)
        .withSkipEmptyLines(false)
        .build();

    String[] headers = parser.getHeaders();
    assertEquals(3, headers.length);
    assertEquals("A", headers[0]);
    assertEquals("B", headers[1]);
    assertEquals("C", headers[2]);

    String[] row = parser.read();
    assertEquals(1, row.length);
    assertEquals(null, row[0]);

    row = parser.read();
    assertEquals(3, row.length);
    assertEquals("1", row[0]);
    assertEquals("2", row[1]);
    assertEquals("3", row[2]);

    row = parser.read();
    assertEquals(1, row.length);
    assertEquals(null, row[0]);

    row = parser.read();
    assertEquals(3, row.length);
    assertEquals("4", row[0]);
    assertEquals("5", row[1]);
    assertEquals("6", row[2]);

    assertNull(parser.read());

    parser.close();
  }

  @Test
  public void commentChar() throws IOException {
    String simpleCsv = "#Comment\nA,B,C\n#Comment\n1,2,3\n#Comment\n4,5,6\n#Comment";

    CsvUnivocityDelimitedParser parser = new ParserBuilder(simpleCsv)
        .withCommentChar('#')
        .build();

    String[] headers = parser.getHeaders();
    assertEquals(3, headers.length);
    assertEquals("A", headers[0]);
    assertEquals("B", headers[1]);
    assertEquals("C", headers[2]);

    String[] row = parser.read();
    assertEquals(3, row.length);
    assertEquals("1", row[0]);
    assertEquals("2", row[1]);
    assertEquals("3", row[2]);

    row = parser.read();
    assertEquals(3, row.length);
    assertEquals("4", row[0]);
    assertEquals("5", row[1]);
    assertEquals("6", row[2]);

    assertNull(parser.read());

    parser.close();
  }

  @Test
  public void commentDisabled() throws IOException {
    String simpleCsv = "#Comment\nA,B,C\n#Comment\n1,2,3\n#Comment\n4,5,6\n#Comment";

    CsvUnivocityDelimitedParser parser = new ParserBuilder(simpleCsv)
        .withCommentChar('\0')
        .build();

    String[] headers = parser.getHeaders();
    assertEquals(1, headers.length);
    assertEquals("#Comment", headers[0]);

    String[] row = parser.read();
    assertEquals(3, row.length);
    assertEquals("A", row[0]);
    assertEquals("B", row[1]);
    assertEquals("C", row[2]);

    row = parser.read();
    assertEquals(1, row.length);
    assertEquals("#Comment", row[0]);

    row = parser.read();
    assertEquals(3, row.length);
    assertEquals("1", row[0]);
    assertEquals("2", row[1]);
    assertEquals("3", row[2]);

    row = parser.read();
    assertEquals(1, row.length);
    assertEquals("#Comment", row[0]);

    row = parser.read();
    assertEquals(3, row.length);
    assertEquals("4", row[0]);
    assertEquals("5", row[1]);
    assertEquals("6", row[2]);

    row = parser.read();
    assertEquals(1, row.length);
    assertEquals("#Comment", row[0]);

    assertNull(parser.read());

    parser.close();
  }

  @Test
  public void lineSeparator() throws IOException {
    String simpleCsv = "A,B,C$$1,2,3$$4,5,6";

    CsvUnivocityDelimitedParser parser = new ParserBuilder(simpleCsv)
        .withLineSeparator("$$")
        .build();

    String[] headers = parser.getHeaders();
    assertEquals(3, headers.length);
    assertEquals("A", headers[0]);
    assertEquals("B", headers[1]);
    assertEquals("C", headers[2]);

    String[] row = parser.read();
    assertEquals(3, row.length);
    assertEquals("1", row[0]);
    assertEquals("2", row[1]);
    assertEquals("3", row[2]);

    row = parser.read();
    assertEquals(3, row.length);
    assertEquals("4", row[0]);
    assertEquals("5", row[1]);
    assertEquals("6", row[2]);

    assertNull(parser.read());

    parser.close();
  }


  private static class ParserBuilder {
    String content;
    char quoteChar = '"';
    char escapeChar = '"';
    String fieldSeparator = ",";
    char commentChar = '#';
    boolean headerRow = true;
    int skipStartLines = 0;
    int maxColumns = 1000;
    int maxCharsPerColumn = 1000;
    boolean skipEmptyLines = true;
    long initialPosition = 0;
    String lineSeparator = "\n";

    ParserBuilder(String content) {
      this.content = content;
    }

    ParserBuilder withQuoteChar(char quoteChar) {
      this.quoteChar = quoteChar;
      return this;
    }

    ParserBuilder withEscapeChar(char escapeChar) {
      this.escapeChar = escapeChar;
      return this;
    }

    ParserBuilder withFieldSeparator(String separator) {
      this.fieldSeparator = separator;
      return this;
    }

    ParserBuilder withHeaderRow(boolean header) {
      this.headerRow = header;
      return this;
    }

    ParserBuilder withSkipLines(int skipLines) {
      this.skipStartLines = skipLines;
      return this;
    }

    ParserBuilder withMaxColumns(int size) {
      this.maxColumns = size;
      return this;
    }

    ParserBuilder withMaxCharsPerColumn(int size) {
      this.maxCharsPerColumn = size;
      return this;
    }

    ParserBuilder withInitialPosition(long position) {
      this.initialPosition = position;
      return this;
    }

    ParserBuilder withSkipEmptyLines(boolean skip) {
      this.skipEmptyLines = skip;
      return this;
    }

    ParserBuilder withCommentChar(char comment) {
      this.commentChar = comment;
      return this;
    }

    ParserBuilder withLineSeparator(String lineSeparator) {
      this.lineSeparator = lineSeparator;
      return this;
    }

    CsvUnivocityDelimitedParser build() throws IOException {
      return new CsvUnivocityDelimitedParser(
          new StringReader(content),
          quoteChar,
          escapeChar,
          fieldSeparator,
          commentChar,
          lineSeparator,
          headerRow,
          skipStartLines,
          maxColumns,
          maxCharsPerColumn,
          skipEmptyLines,
          initialPosition
      );
    }
  }
}
