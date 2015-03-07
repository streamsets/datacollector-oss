/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.csv;

import com.streamsets.pipeline.lib.io.ObjectLengthException;
import com.streamsets.pipeline.lib.io.OverrunReader;
import org.apache.commons.csv.CSVFormat;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;

public class TestCsvParser {

  private OverrunReader getReader(String name) throws Exception {
    InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(name);
    return (is != null) ? new OverrunReader(new InputStreamReader(is), 0, false) : null;
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
                                     CSVFormat.DEFAULT.withHeader((String[])null).withSkipHeaderRecord(true), -1, 12);
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
                                     CSVFormat.DEFAULT.withHeader((String[])null).withSkipHeaderRecord(true), -1, 20);
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

}
