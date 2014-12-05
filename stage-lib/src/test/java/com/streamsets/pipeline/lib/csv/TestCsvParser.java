/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.commons.csv.CSVFormat;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

public class TestCsvParser {

  private Reader getReader(String name) throws Exception {
    InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(name);
    return (is != null) ? new InputStreamReader(is) : null;
  }

  @Test
  public void testParserNoHeaders() throws Exception {
    CsvParser parser = new CsvParser(getReader("TestCsvParser-default.csv"), CSVFormat.DEFAULT);
    Assert.assertArrayEquals(new String[]{}, parser.getHeaders());
  }

  @Test
  public void testParserHeaders() throws Exception {
    CsvParser parser = new CsvParser(getReader("TestCsvParser-default.csv"), CSVFormat.DEFAULT.withHeader());
    try {
      Assert.assertArrayEquals(new String[]{"h1", "h2", "h3", "h4"}, parser.getHeaders());
    } finally {
      parser.close();
    }
  }

  @Test
  public void testParserRecords() throws Exception {
    CsvParser parser = new CsvParser(getReader("TestCsvParser-default.csv"), CSVFormat.DEFAULT.withHeader());
    try {
      Assert.assertEquals(0, parser.getReaderPosition());

      String[] record = parser.read();
      Assert.assertEquals(20, parser.getReaderPosition());
      Assert.assertNotNull(record);
      Assert.assertArrayEquals(new String[]{"a", "b", "c", "d"}, record);

      record = parser.read();
      Assert.assertEquals(-1,  parser.getReaderPosition());
      Assert.assertNotNull(record);
      Assert.assertArrayEquals(new String[]{"w", "x", "y", "z", "extra"}, record);

      Assert.assertNull(parser.read());
      Assert.assertEquals(-1, parser.getReaderPosition());
    } finally {
      parser.close();
    }
  }

  @Test
  public void testParserRecordsFromOffset() throws Exception {
    CsvParser parser = new CsvParser(getReader("TestCsvParser-default.csv"), CSVFormat.DEFAULT.withHeader(), 12);
    try {
      Assert.assertEquals(12, parser.getReaderPosition());

      String[] record = parser.read();
      Assert.assertEquals(20, parser.getReaderPosition());
      Assert.assertNotNull(record);
      Assert.assertArrayEquals(new String[]{"a", "b", "c", "d"}, record);

      record = parser.read();
      Assert.assertEquals(-1, parser.getReaderPosition());
      Assert.assertNotNull(record);
      Assert.assertArrayEquals(new String[]{"w", "x", "y", "z", "extra"}, record);

      Assert.assertNull(parser.read());
      Assert.assertEquals(-1, parser.getReaderPosition());
    } finally {
      parser.close();
    }
    parser = new CsvParser(getReader("TestCsvParser-default.csv"), CSVFormat.DEFAULT.withHeader(), 20);
    try {
      Assert.assertEquals(20, parser.getReaderPosition());

      String[] record = parser.read();
      Assert.assertEquals(-1, parser.getReaderPosition());
      Assert.assertNotNull(record);
      Assert.assertArrayEquals(new String[]{"w", "x", "y", "z", "extra"}, record);

      Assert.assertNull(parser.read());
      Assert.assertEquals(-1, parser.getReaderPosition());
    } finally {
      parser.close();
    }
  }

}
