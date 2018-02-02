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

import org.jparsec.Parser;
import org.jparsec.error.ParserException;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class TestMultiCharDelimitedParser {

  private final String fieldSeparator = "|||";
  private final char quoteChar = '"';
  private final char escapeChar = '\\';
  private final String lineSeparator = "\n";

  private final CsvMultiCharDelimitedParser parser = new CsvMultiCharDelimitedParser(
      null,
      quoteChar,
      escapeChar,
      fieldSeparator,
      10000,
      1000,
      false,
      0l,
      0,
      lineSeparator
  );

  @Test
  public void testPrimitives() {
    assertThat(parser.bareField().parse("abc"), equalTo("abc"));
    assertThat(parser.field().parse("abc"), equalTo("abc"));
    assertThat(parser.quotedField().parse("\"def\""), equalTo("def"));
    assertThat(parser.field().parse("\"def\""), equalTo("def"));
    assertThat(parser.fieldSeparator().toScanner("field sep").parse(fieldSeparator), nullValue());
    assertThat(parser.quote().toScanner("quote").parse(String.valueOf(quoteChar)), nullValue());
    assertThat(parser.escape().toScanner("escape").parse(String.valueOf(escapeChar)), nullValue());
  }

  @Test
  public void testFields() {
    final Parser<List<String>> fieldsParser = parser.fields();
    assertThat(
        fieldsParser.parse(String.format("abc%1$sdef%1$sghi", fieldSeparator)),
        equalTo(Arrays.asList("abc", "def", "ghi"))
    );
    assertThat(
        fieldsParser.parse(String.format("abc%1$s\"d%1$sef\"%1$sghi", fieldSeparator)),
        equalTo(Arrays.asList("abc", String.format("d%1$sef", fieldSeparator), "ghi"))
    );
  }


  @Test
  public void testCompleteLines() {
    final Parser<List<String>> fieldsParser = parser.fields();
    assertThat(
        fieldsParser.parse(String.format("abc%1$sdef%1$sghi", fieldSeparator)),
        equalTo(Arrays.asList("abc", "def", "ghi"))
    );
    assertThat(
        fieldsParser.parse(String.format("abc%1$s\"d%1$sef\"%1$sghi", fieldSeparator)),
        equalTo(Arrays.asList("abc", String.format("d%1$sef", fieldSeparator), "ghi"))
    );
  }


  @Test(expected = ParserException.class)
  public void testInvalidQuotedField() {
    parser.quotedField().parse("abc");
  }

  @Test(expected = ParserException.class)
  public void testInvalidBareField() {
    parser.bareField().parse("\"abc\"");
  }

  @Test(expected = ParserException.class)
  public void testInvalidFieldSeparator() {
    assertThat(parser.fieldSeparator().toScanner("field sep").parse(fieldSeparator.substring(1)), nullValue());
  }

  @Test(expected = ParserException.class)
  public void testInvalidQuoteChar() {
    assertThat(parser.quote().toScanner("quote").parse("_"), nullValue());
  }

  @Test(expected = ParserException.class)
  public void testInvalidEscapeChar() {
    assertThat(parser.escape().toScanner("escape").parse("_"), nullValue());
  }
}
