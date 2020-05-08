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

import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.hasSize;

public class TestMultiCharDelimitedParserIsolatedLines {

  private static final String[] NEWLINES_IN_CELLS_LINES = new String[] {
      "abc<>\"def start of long line",
      "continuation of long line",
      "end of long line\"<>last cell"
  };

  @Test
  public void testParser() {
    final List<String> fields = CsvMultiCharDelimitedParser.createNonReaderParser('"', '\\', "<>").parseStandaloneLine("abc<>def");
    final List<String> fields2 = CsvMultiCharDelimitedParser.createNonReaderParser('"', '\\', "|").parseStandaloneLine("abc|def");

    assertThat(fields, hasSize(2));
    assertThat(fields, contains("abc", "def"));
    assertThat(fields, equalTo(fields2));
  }

  @Test
  public void testSeparatorsRunTogether() {
    final List<String> fields = CsvMultiCharDelimitedParser.createNonReaderParser('"', '\\', "||").parseStandaloneLine("abc||def|ghi|||jkl||mno");
    assertThat(fields, hasSize(4));
    assertThat(fields, contains("abc", "def|ghi", "|jkl", "mno"));

    final List<String> fields2 = CsvMultiCharDelimitedParser.createNonReaderParser('"', '\\', "||").parseStandaloneLine("abc||def|ghi|||||jkl||mno");
    assertThat(fields2, hasSize(5));
    assertThat(fields2, contains("abc", "def|ghi", "", "|jkl", "mno"));
  }

  @Test
  public void testEscapes() {
    final List<String> fields = CsvMultiCharDelimitedParser.createNonReaderParser('"', '\\', "||").parseStandaloneLine("\"abc||\\\"def\"||ghi");
    assertThat(fields, hasSize(2));
  }

  @Test
  public void testNewlineInQuotes() {
    final CsvMultiCharDelimitedParser parser = CsvMultiCharDelimitedParser.createNonReaderParser('"', '\\', "||");
    final List<String> incompleteLine = parser.parseStandaloneLine("abc||def|ghi|||jkl||\"mno");
    assertThat(incompleteLine, nullValue());

    final List<String> completeLine = parser.parseStandaloneLine("pq\"||rst||uv");
    assertThat(completeLine, notNullValue());
  }

  @Test
  public void testMultipleNewlinesInQuotedCell() {
    // input buffer limit of 1000 chars, which should be enough
    final CsvMultiCharDelimitedParser parser = CsvMultiCharDelimitedParser.createNonReaderParser('"', '\\', "<>", 1000);
    List<String> parsed = null;
    for (String line : NEWLINES_IN_CELLS_LINES) {
      parsed = parser.parseStandaloneLine(line);
    }

    assertThat(parsed, notNullValue());
    assertThat(parsed, hasSize(3));
    assertThat(parsed, contains("abc", "def start of long line\ncontinuation of long line\nend of long line", "last cell"));
  }

  @Test(expected = RuntimeException.class)
  public void testInputBufferSizeFull() {
    // input buffer limit of 10 chars, which should not be enough
    final CsvMultiCharDelimitedParser parser = CsvMultiCharDelimitedParser.createNonReaderParser('"', '\\', "<>", 10);
    for (String line : NEWLINES_IN_CELLS_LINES) {
      parser.parseStandaloneLine(line);
    }
  }

  @Test
  public void testEscapeFieldSeparator() {
    final List<String> fields = CsvMultiCharDelimitedParser.createNonReaderParser('"', '\\', "||").parseStandaloneLine("abc\\||def||ghi");
    assertThat(fields, hasSize(2));
    assertThat(fields, contains("abc||def", "ghi"));
  }

  @Test
  public void testEscapeQuotedFieldSeparator() {
    final List<String> fields = CsvMultiCharDelimitedParser.createNonReaderParser('"', '\\', "||").parseStandaloneLine("\"abc\\||def\"||ghi");
    assertThat(fields, hasSize(2));
    assertThat(fields, contains("abc||def", "ghi"));
  }
}
