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
package com.streamsets.pipeline.lib.csv;

import com.streamsets.pipeline.api.ext.io.ObjectLengthException;
import com.streamsets.pipeline.api.ext.io.CountingReader;
import com.streamsets.pipeline.api.ext.io.OverrunReader;
import org.apache.commons.csv.CSVFormat;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;

public class TestCsvParser {

  private OverrunReader getReader(String name) throws Exception {
    InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(name);
    return (is != null) ? new OverrunReader(new InputStreamReader(is), 0, false, false) : null;
  }

  @Test
  public void testParserNoHeaders() throws Exception {
    CsvParser parser = new CsvParser(getReader("TestCsvParser-default.csv"), CSVFormat.DEFAULT, -1);
    Assert.assertArrayEquals(null, parser.getHeaders());
  }

  @Test
  public void testParserHeaders() throws Exception {
    CsvParser parser = new CsvParser(getReader("TestCsvParser-default.csv"),
                                     CSVFormat.DEFAULT.withHeader((String[])null).withSkipHeaderRecord(true), -1);
    try {
      Assert.assertArrayEquals(new String[]{"h1", "h2", "h3", "h4"}, parser.getHeaders());
    } finally {
      parser.close();
    }
  }

  @Test
  public void testParserRecords() throws Exception {
    CsvParser parser = new CsvParser(getReader("TestCsvParser-default.csv"),
                                     CSVFormat.DEFAULT.withHeader((String[])null).withSkipHeaderRecord(true), -1);
    try {
      Assert.assertEquals(12, parser.getReaderPosition());

      String[] record = parser.read();
      Assert.assertEquals(20, parser.getReaderPosition());
      Assert.assertNotNull(record);
      Assert.assertArrayEquals(new String[]{"a", "b", "c", "d"}, record);

      record = parser.read();
      Assert.assertEquals(33,  parser.getReaderPosition());
      Assert.assertNotNull(record);
      Assert.assertArrayEquals(new String[]{"w", "x", "y", "z", "extra"}, record);

      Assert.assertNull(parser.read());
      Assert.assertEquals(33, parser.getReaderPosition());
    } finally {
      parser.close();
    }
  }

  @Test
  public void testParserRecordsFromOffset() throws Exception {
    CsvParser parser = new CsvParser(getReader("TestCsvParser-default.csv"),
                                     CSVFormat.DEFAULT.withHeader((String[])null).withSkipHeaderRecord(true), -1, 12, 0);
    try {
      Assert.assertEquals(12, parser.getReaderPosition());

      String[] record = parser.read();
      Assert.assertEquals(20, parser.getReaderPosition());
      Assert.assertNotNull(record);
      Assert.assertArrayEquals(new String[]{"a", "b", "c", "d"}, record);

      record = parser.read();
      Assert.assertEquals(33, parser.getReaderPosition());
      Assert.assertNotNull(record);
      Assert.assertArrayEquals(new String[]{"w", "x", "y", "z", "extra"}, record);

      Assert.assertNull(parser.read());
      Assert.assertEquals(33, parser.getReaderPosition());
    } finally {
      parser.close();
    }
    parser = new CsvParser(getReader("TestCsvParser-default.csv"),
                                     CSVFormat.DEFAULT.withHeader((String[])null).withSkipHeaderRecord(true), -1, 20, 0);
    try {
      Assert.assertEquals(20, parser.getReaderPosition());

      String[] record = parser.read();
      Assert.assertEquals(33, parser.getReaderPosition());
      Assert.assertNotNull(record);
      Assert.assertArrayEquals(new String[]{"w", "x", "y", "z", "extra"}, record);

      Assert.assertNull(parser.read());
      Assert.assertEquals(33, parser.getReaderPosition());
    } finally {
      parser.close();
    }
  }

  @Test
  public void testHeadersWithNullColumns() throws Exception {
    CsvParser parser = new CsvParser(new StringReader("a,,,d\naa,bb,cc,dd\n"),
            CSVFormat.DEFAULT.withHeader((String[])null).withSkipHeaderRecord(true), 15);
    try {
      Assert.assertEquals(6, parser.getReaderPosition());

      String[] record = parser.read();
      Assert.assertEquals(18, parser.getReaderPosition());
      Assert.assertNotNull(record);
      Assert.assertArrayEquals(new String[]{"a","empty-1","empty-2","d"}, parser.getHeaders());
      Assert.assertArrayEquals(new String[]{"aa", "bb", "cc","dd"}, record);

    } finally {
      parser.close();
    }
  }

  @Test
  public void testMaxObjectLen() throws Exception {
    CsvParser parser = new CsvParser(new StringReader("a,b,c\naa,bb,cc\ne,f,g\n"),
                                     CSVFormat.DEFAULT.withHeader((String[])null).withSkipHeaderRecord(false), 6);
    try {
      Assert.assertEquals(0, parser.getReaderPosition());

      String[] record = parser.read();
      Assert.assertEquals(6, parser.getReaderPosition());
      Assert.assertNotNull(record);
      Assert.assertArrayEquals(new String[]{"a", "b", "c"}, record);

      try {
        parser.read();
        Assert.fail();
      } catch (ObjectLengthException ex) {
      }
      Assert.assertEquals(15, parser.getReaderPosition());
      record = parser.read();
      Assert.assertNotNull(record);
      Assert.assertArrayEquals(new String[]{"e", "f", "g"}, record);
      Assert.assertNull(parser.read());
    } finally {
      parser.close();
    }
  }

  @Test
  public void testSkipLines() throws Exception {
    CsvParser parser = new CsvParser(
        new CountingReader(new StringReader("foo\nbar\r\na,b,c\naa,bb,cc\ne,f,g\n")),
        CSVFormat.DEFAULT.withHeader((String[])null).withSkipHeaderRecord(false),
        -1,
        0,
        2
    );
    try {
      Assert.assertEquals(9, parser.getReaderPosition());

      String[] record = parser.read();
      Assert.assertEquals(15, parser.getReaderPosition());
      Assert.assertNotNull(record);
      Assert.assertArrayEquals(new String[]{"a", "b", "c"}, record);
      record = parser.read();
      Assert.assertNotNull(record);
      Assert.assertArrayEquals(new String[]{"aa", "bb", "cc"}, record);
      Assert.assertEquals(24, parser.getReaderPosition());
      record = parser.read();
      Assert.assertNotNull(record);
      Assert.assertArrayEquals(new String[]{"e", "f", "g"}, record);
      Assert.assertEquals(30, parser.getReaderPosition());
      record = parser.read();
      Assert.assertNull(record);
      Assert.assertEquals(30, parser.getReaderPosition());
    } finally {
      parser.close();
    }
  }

  @Test(expected = IOException.class)
  public void testUnwrapIllegalStateException() throws Exception {
    new CsvParser(
      new CountingReader(new StringReader("0,\"020\"1,\"BS:5252525  ORDER:99999\"4")),
      CSVFormat.DEFAULT.withHeader((String[])null).withSkipHeaderRecord(true),
      -1,
      0,
      0
    );
  }
}
