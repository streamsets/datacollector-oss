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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class TestMultiCharDelimitedParserReader {

  private static final String SEPARATOR = "-_-";

  private static final String FULL_DATA = String.format(
      "first%1$ssecond%1$sthird\n" +
      "one%1$stwo%1$sthree\n" +
      "four%1$sfive%1$ssix\n" +
      "seven%1$seight%1$snine",
      SEPARATOR
  );

  private final long skipBytes;
  private final int skipLines;
  private final boolean headerRow;
  private final List<String> expectedHeaders;
  private final List<List<String>> expectedRows;

  public TestMultiCharDelimitedParserReader(
      long skipBytes,
      int skipLines,
      boolean headerRow,
      List<String> expectedHeaders,
      List<List<String>> expectedRows
  ) {
    this.skipBytes = skipBytes;
    this.skipLines = skipLines;
    this.headerRow = headerRow;
    this.expectedHeaders = expectedHeaders;
    this.expectedRows = expectedRows;
  }

  @Parameterized.Parameters(
      name = "Skip bytes: {0}, Skip lines: {1}, Header row: {2}, Expected headers: {3}, Expected rows: {4}"
  )
  public static Collection<Object[]> data() throws Exception {
    final List<Object[]> data = new LinkedList<>();
    data.add(new Object[]{
        0l,
        0,
        true,
        Stream.of("first", "second", "third").collect(Collectors.toList()),
        Stream.of(
            Stream.of("one", "two", "three").collect(Collectors.toList()),
            Stream.of("four", "five", "six").collect(Collectors.toList()),
            Stream.of("seven", "eight", "nine").collect(Collectors.toList())
        ).collect(Collectors.toList())
    });
    data.add(new Object[]{
        0l,
        1,
        true,
        Stream.of("first", "second", "third").collect(Collectors.toList()),
        Stream.of(
            Stream.of("four", "five", "six").collect(Collectors.toList()),
            Stream.of("seven", "eight", "nine").collect(Collectors.toList())
        ).collect(Collectors.toList())
    });
    data.add(new Object[]{
        0l,
        0,
        false,
        null,
        Stream.of(
            Stream.of("first", "second", "third").collect(Collectors.toList()),
            Stream.of("one", "two", "three").collect(Collectors.toList()),
            Stream.of("four", "five", "six").collect(Collectors.toList()),
            Stream.of("seven", "eight", "nine").collect(Collectors.toList())
        ).collect(Collectors.toList())
    });
    return data;
  }

  @Test
  public void testParser() throws IOException {
    final StringReader stringReader = new StringReader(FULL_DATA);
    final CountingReader countingReader = new CountingReader(stringReader);
    final CsvMultiCharDelimitedParser parser = new CsvMultiCharDelimitedParser(
        countingReader,
        '"',
        '\\',
        SEPARATOR,
        10000,
        1000,
        headerRow,
        skipBytes,
        skipLines,
        "\n"
    );

    final String[] headers = parser.getHeaders();
    if (headerRow) {
      assertThat(headers, notNullValue());
      final List<String> headerList = Arrays.asList(headers);
      assertThat(headerList, equalTo(expectedHeaders));
    } else {
      assertThat(headers, nullValue());
    }

    for (List<String> expectedRow : expectedRows) {
      final String[] row = parser.read();
      assertThat(row, equalTo(expectedRow.toArray(new String[expectedRow.size()])));
    }

    final String[] end = parser.read();
    assertThat(end, nullValue());

  }

}
