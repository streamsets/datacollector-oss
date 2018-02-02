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

import com.streamsets.pipeline.api.ext.io.CountingReader;
import com.streamsets.pipeline.lib.csv.CsvMultiCharDelimitedParser;
import org.jparsec.error.ParserException;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class TestMultiCharDelimitedParserReaderMarkAndReset {

  private static final String SEPARATOR = "||";

  private static final String MALFORMED_INPUT = String.format(
      "first%1$ssecond%1$sthird\n" +
      "one%1$stwo%1$sthree\n" +
      "four%1$sfive%1$ssix\"\n" +
      "seven%1$seight%1$snine",
      SEPARATOR
  );

  @Test
  public void testParser() throws IOException {
    final StringReader stringReader = new StringReader(MALFORMED_INPUT);
    final CountingReader countingReader = new CountingReader(new BufferedReader(stringReader));
    final CsvMultiCharDelimitedParser parser = new CsvMultiCharDelimitedParser(
        countingReader,
        '"',
        '\\',
        SEPARATOR,
        10000,
        1000,
        true,
        0l,
        0,
        "\n"
    );

    long lastPos = 0l;
    long currentPos;

    final String[] headers = parser.getHeaders();
    currentPos = parser.getReaderPosition();
    final List<String> headerList = Arrays.asList(headers);
    assertThat(headerList, equalTo(Arrays.asList("first", "second", "third")));
    // the position should be equal before and after reading the headers (offsets start from after headers)
    assertThat(currentPos, equalTo(lastPos));
    lastPos = currentPos;

    // we should be able to read the first complete record
    final String[] row = parser.read();
    assertThat(Arrays.asList(row), equalTo(Arrays.asList("one", "two", "three")));
    currentPos = parser.getReaderPosition();
    assertThat(currentPos, greaterThan(lastPos));
    lastPos = currentPos;

    // the next one should fail due to the dangling quote
    try {
      parser.read();
      Assert.fail("Should have thrown ParserException");
    } catch (ParserException e) {
      currentPos = parser.getReaderPosition();

      // the reader position should have been reset now
      assertThat(currentPos, equalTo(lastPos));
    }

  }

}
