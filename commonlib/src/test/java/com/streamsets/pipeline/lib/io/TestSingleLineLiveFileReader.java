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
package com.streamsets.pipeline.lib.io;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class TestSingleLineLiveFileReader {
  private File testDir;

  @Before
  public void setUp() throws IOException {
    testDir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(testDir.mkdirs());
  }

  private Path createFile(List<String> lines) throws IOException {
    Path file = new File(testDir, UUID.randomUUID().toString()).toPath();
    try (Writer writer = new FileWriter(file.toFile())) {
      IOUtils.writeLines(lines, "", writer);
    }
    return file;
  }

  private String readChunk(LiveFileChunk chunk) throws IOException {
    char[] data = new char[chunk.getLength()];
    IOUtils.readFully(chunk.getReader(), data);
    return new String(data);
  }

  @Test
  public void testMethods() throws Exception {
    Path file = createFile(Arrays.asList("Hello1\n", "Hello2\n"));
    LiveFile lf = new LiveFile(file);
    LiveFileReader lfr = new SingleLineLiveFileReader(LogRollModeFactory.REVERSE_COUNTER.get(file.getFileName().toString(), ""),
                                           null, lf, Charset.defaultCharset(), 0, 10);
    Assert.assertEquals(Charset.defaultCharset(), lfr.getCharset());
    Assert.assertEquals(lf, lfr.getLiveFile());
    lfr.close();
    lfr.close();
  }

  @Test
  public void testValidCharsets() throws Exception {
    Path file = createFile(Arrays.asList("Hello1\n", "Hello2\n"));
    LiveFile lf = new LiveFile(file);
    new SingleLineLiveFileReader(LogRollModeFactory.REVERSE_COUNTER.get(file.getFileName().toString(), ""),null, lf, Charset.forName("US-ASCII"), 0, 10);
    new SingleLineLiveFileReader(LogRollModeFactory.REVERSE_COUNTER.get(file.getFileName().toString(), ""),null, lf, StandardCharsets.UTF_8, 0, 10);
    new SingleLineLiveFileReader(LogRollModeFactory.REVERSE_COUNTER.get(file.getFileName().toString(), ""),null, lf, Charset.forName("GBK"), 0, 10);
    new SingleLineLiveFileReader(LogRollModeFactory.REVERSE_COUNTER.get(file.getFileName().toString(), ""),null, lf, Charset.forName("ISO-8859-1"), 0, 10);
    new SingleLineLiveFileReader(LogRollModeFactory.REVERSE_COUNTER.get(file.getFileName().toString(), ""),null, lf, Charset.forName("shift_jis"), 0, 10);
    new SingleLineLiveFileReader(LogRollModeFactory.REVERSE_COUNTER.get(file.getFileName().toString(), ""),null, lf, Charset.forName("euc-jp"), 0, 10);
    new SingleLineLiveFileReader(LogRollModeFactory.REVERSE_COUNTER.get(file.getFileName().toString(), ""),null, lf, Charset.forName("euc-kr"), 0, 10);
    new SingleLineLiveFileReader(LogRollModeFactory.REVERSE_COUNTER.get(file.getFileName().toString(), ""),null, lf, Charset.forName("koi8-r"), 0, 10);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidCharset1() throws Exception {
    Path file = createFile(Arrays.asList("Hello1\n", "Hello2\n"));
    LiveFile lf = new LiveFile(file);
    new SingleLineLiveFileReader(LogRollModeFactory.REVERSE_COUNTER.get(file.getFileName().toString(), ""),null, lf, Charset.forName("UTF-16"), 0, 10);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidCharset2() throws Exception {
    Path file = createFile(Arrays.asList("Hello1\n", "Hello2\n"));
    LiveFile lf = new LiveFile(file);
    new SingleLineLiveFileReader(LogRollModeFactory.REVERSE_COUNTER.get(file.getFileName().toString(), ""),null, lf, Charset.forName("UTF-32"), 0, 10);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidCharset3() throws Exception {
    Path file = createFile(Arrays.asList("Hello1\n", "Hello2\n"));
    LiveFile lf = new LiveFile(file);
    new SingleLineLiveFileReader(LogRollModeFactory.REVERSE_COUNTER.get(file.getFileName().toString(), ""),null, lf, Charset.forName("IBM500"), 0, 10);
  }

  @Test(expected = IOException.class)
  public void testInvalidOffset() throws Exception {
    Path file = createFile(Arrays.asList("Hello"));
    LiveFile lf = new LiveFile(file);
    new SingleLineLiveFileReader(LogRollModeFactory.REVERSE_COUNTER.get(file.getFileName().toString(), ""),null, lf, Charset.defaultCharset(), 10, 10);
  }

  @Test
  public void testOneLineReadFromBeginningFullLinesNoTruncate() throws Exception {
    Path file = createFile(Arrays.asList("Hello1\n", "Hello2\n"));
    LiveFile lf = new LiveFile(file);
    LiveFileReader lfr = new SingleLineLiveFileReader(LogRollModeFactory.REVERSE_COUNTER.get(file.getFileName().toString(), ""),null, lf, Charset.defaultCharset(), 0, 10);

    Assert.assertTrue(lfr.hasNext());
    LiveFileChunk chunk = lfr.next(0);
    Assert.assertNotNull(chunk);
    Assert.assertFalse(chunk.isTruncated());
    Assert.assertEquals(0, chunk.getOffset());
    Assert.assertEquals(7, chunk.getLength());
    Assert.assertEquals("Hello1\n", readChunk(chunk));
    Assert.assertEquals(7, lfr.getOffset());

    Assert.assertTrue(lfr.hasNext());
    chunk = lfr.next(0);
    Assert.assertNotNull(chunk);
    Assert.assertFalse(chunk.isTruncated());
    Assert.assertEquals(7, chunk.getOffset());
    Assert.assertEquals(7, chunk.getLength());
    Assert.assertEquals("Hello2\n", readChunk(chunk));
    Assert.assertEquals(14, lfr.getOffset());

    Assert.assertTrue(lfr.hasNext());
    chunk = lfr.next(0);
    Assert.assertNull(chunk);
    Assert.assertEquals(14, lfr.getOffset());

    Assert.assertTrue(lfr.hasNext());
    chunk = lfr.next(0);
    Assert.assertNull(chunk);
    Assert.assertEquals(14, lfr.getOffset());

    Files.move(file, Paths.get(file.getParent().toString(), UUID.randomUUID().toString()));
    Thread.sleep(SingleLineLiveFileReader.REFRESH_INTERVAL + 1);

    Assert.assertFalse(lfr.hasNext());
    Assert.assertEquals(14, lfr.getOffset());

    // next() after EOF returns null
    Assert.assertNull(lfr.next(0));

    lfr.close();
  }

  @Test
  public void testOneLineReadFromBeginningLastLineNoEOLNoTruncate() throws Exception {
    Path file = createFile(Arrays.asList("Hello1\n", "Hello2"));
    LiveFile lf = new LiveFile(file);
    LiveFileReader lfr = new SingleLineLiveFileReader(LogRollModeFactory.REVERSE_COUNTER.get(file.getFileName().toString(), ""),null, lf, Charset.defaultCharset(), 0, 10);

    Assert.assertTrue(lfr.hasNext());
    LiveFileChunk chunk = lfr.next(0);
    Assert.assertNotNull(chunk);
    Assert.assertFalse(chunk.isTruncated());
    Assert.assertEquals(0, chunk.getOffset());
    Assert.assertEquals(7, chunk.getLength());
    Assert.assertEquals("Hello1\n", readChunk(chunk));
    Assert.assertEquals(7, lfr.getOffset());

    Assert.assertTrue(lfr.hasNext());
    chunk = lfr.next(0);
    Assert.assertNull(chunk);
    Assert.assertEquals(7, lfr.getOffset());

    Files.move(file, Paths.get(file.getParent().toString(), UUID.randomUUID().toString()));
    Thread.sleep(SingleLineLiveFileReader.REFRESH_INTERVAL + 1);

    Assert.assertTrue(lfr.hasNext());
    chunk = lfr.next(0);

    Assert.assertNotNull(chunk);
    Assert.assertFalse(chunk.isTruncated());
    Assert.assertEquals(7, chunk.getOffset());
    Assert.assertEquals(6, chunk.getLength());
    Assert.assertEquals("Hello2", readChunk(chunk));
    Assert.assertEquals(13, lfr.getOffset());

    Assert.assertFalse(lfr.hasNext());
    Assert.assertEquals(13, lfr.getOffset());

    lfr.close();
  }

  @Test
  public void testOneLineReadFromExactOffsetFullLinesNoTruncate() throws Exception {
    Path file = createFile(Arrays.asList("Hello1\n", "Hello2\n"));
    LiveFile lf = new LiveFile(file);
    LiveFileReader lfr = new SingleLineLiveFileReader(LogRollModeFactory.REVERSE_COUNTER.get(file.getFileName().toString(), ""),null, lf, Charset.defaultCharset(), 7, 10);

    Assert.assertTrue(lfr.hasNext());
    LiveFileChunk chunk = lfr.next(0);
    Assert.assertNotNull(chunk);
    Assert.assertFalse(chunk.isTruncated());
    Assert.assertEquals(7, chunk.getOffset());
    Assert.assertEquals(7, chunk.getLength());
    Assert.assertEquals("Hello2\n", readChunk(chunk));
    Assert.assertEquals(14, lfr.getOffset());

    Assert.assertTrue(lfr.hasNext());
    chunk = lfr.next(0);
    Assert.assertNull(chunk);
    Assert.assertEquals(14, lfr.getOffset());

    Assert.assertTrue(lfr.hasNext());
    chunk = lfr.next(0);
    Assert.assertNull(chunk);
    Assert.assertEquals(14, lfr.getOffset());

    Files.move(file, Paths.get(file.getParent().toString(), UUID.randomUUID().toString()));
    Thread.sleep(SingleLineLiveFileReader.REFRESH_INTERVAL + 1);

    Assert.assertFalse(lfr.hasNext());

    lfr.close();
  }

  @Test
  public void testOneLineReadFromExactOffsetLastLineNoEOLNoTruncate() throws Exception {
    Path file = createFile(Arrays.asList("Hello1\n", "Hello2"));
    LiveFile lf = new LiveFile(file);
    LiveFileReader lfr = new SingleLineLiveFileReader(LogRollModeFactory.REVERSE_COUNTER.get(file.getFileName().toString(), ""),null, lf, Charset.defaultCharset(), 7, 10);


    Assert.assertTrue(lfr.hasNext());
    LiveFileChunk chunk = lfr.next(0);
    Assert.assertNull(chunk);

    Files.move(file, Paths.get(file.getParent().toString(), UUID.randomUUID().toString()));
    Thread.sleep(SingleLineLiveFileReader.REFRESH_INTERVAL + 1);

    Assert.assertTrue(lfr.hasNext());

    chunk = lfr.next(0);
    Assert.assertNotNull(chunk);

    Assert.assertFalse(chunk.isTruncated());
    Assert.assertEquals(7, chunk.getOffset());
    Assert.assertEquals(6, chunk.getLength());
    Assert.assertEquals("Hello2", readChunk(chunk));
    Assert.assertEquals(13, lfr.getOffset());

    Assert.assertFalse(lfr.hasNext());
    Assert.assertEquals(13, lfr.getOffset());

    Assert.assertFalse(lfr.hasNext());

    lfr.close();
  }

  @Test
  public void testMultiLineReadFromBeginningFullLinesNoTruncate() throws Exception {
    Path file = createFile(Arrays.asList("Hello1\n", "Hello2\n"));
    LiveFile lf = new LiveFile(file);
    LiveFileReader lfr = new SingleLineLiveFileReader(LogRollModeFactory.REVERSE_COUNTER.get(file.getFileName().toString(), ""),null, lf, Charset.defaultCharset(), 0, 20);

    Assert.assertTrue(lfr.hasNext());
    LiveFileChunk chunk = lfr.next(0);
    Assert.assertNotNull(chunk);
    Assert.assertFalse(chunk.isTruncated());
    Assert.assertEquals(0, chunk.getOffset());
    Assert.assertEquals(14, chunk.getLength());
    Assert.assertEquals("Hello1\nHello2\n", readChunk(chunk));

    Assert.assertTrue(lfr.hasNext());
    chunk = lfr.next(0);
    Assert.assertNull(chunk);
    Assert.assertEquals(14, lfr.getOffset());

    Files.move(file, Paths.get(file.getParent().toString(), UUID.randomUUID().toString()));
    Thread.sleep(SingleLineLiveFileReader.REFRESH_INTERVAL + 1);

    Assert.assertFalse(lfr.hasNext());
    Assert.assertEquals(14, lfr.getOffset());

    lfr.close();
  }

  @Test
  public void testMultiLineLineReadFromBeginningLastLineNoEOLNoTruncate() throws Exception {
    Path file = createFile(Arrays.asList("Hello1\n", "Hello2"));
    LiveFile lf = new LiveFile(file);
    LiveFileReader lfr = new SingleLineLiveFileReader(LogRollModeFactory.REVERSE_COUNTER.get(file.getFileName().toString(), ""),null, lf, Charset.defaultCharset(), 0, 20);

    Assert.assertTrue(lfr.hasNext());
    LiveFileChunk chunk = lfr.next(0);
    Assert.assertNotNull(chunk);
    Assert.assertFalse(chunk.isTruncated());
    Assert.assertEquals(0, chunk.getOffset());
    Assert.assertEquals(7, chunk.getLength());
    Assert.assertEquals("Hello1\n", readChunk(chunk));
    Assert.assertEquals(7, lfr.getOffset());

    Assert.assertTrue(lfr.hasNext());
    chunk = lfr.next(0);
    Assert.assertNull(chunk);
    Assert.assertEquals(7, lfr.getOffset());

    Files.move(file, Paths.get(file.getParent().toString(), UUID.randomUUID().toString()));
    Thread.sleep(SingleLineLiveFileReader.REFRESH_INTERVAL + 1);

    Assert.assertTrue(lfr.hasNext());
    chunk = lfr.next(0);

    Assert.assertNotNull(chunk);
    Assert.assertFalse(chunk.isTruncated());
    Assert.assertEquals(7, chunk.getOffset());
    Assert.assertEquals(13, lfr.getOffset());
    Assert.assertEquals("Hello2", readChunk(chunk));

    Assert.assertFalse(lfr.hasNext());
    Assert.assertEquals(13, lfr.getOffset());

    lfr.close();
  }


  @Test
  public void testOneLineReadFromBeginningFullLinesTruncate() throws Exception {
    Path file = createFile(Arrays.asList("Hello123456\n", "Hello2\n"));
    LiveFile lf = new LiveFile(file);
    LiveFileReader lfr = new SingleLineLiveFileReader(LogRollModeFactory.REVERSE_COUNTER.get(file.getFileName().toString(), ""),null, lf, Charset.defaultCharset(), 0, 10);

    Assert.assertTrue(lfr.hasNext());
    LiveFileChunk chunk = lfr.next(0);
    Assert.assertNotNull(chunk);
    Assert.assertTrue(chunk.isTruncated());
    Assert.assertEquals(0, chunk.getOffset());
    Assert.assertEquals(10, chunk.getLength());
    Assert.assertEquals("Hello12345", readChunk(chunk));
    Assert.assertEquals(-10, lfr.getOffset());

    Assert.assertTrue(lfr.hasNext());
    chunk = lfr.next(0);
    Assert.assertNotNull(chunk);
    Assert.assertFalse(chunk.isTruncated());
    Assert.assertEquals(12, chunk.getOffset());
    Assert.assertEquals(7, chunk.getLength());
    Assert.assertEquals("Hello2\n", readChunk(chunk));
    Assert.assertEquals(19, lfr.getOffset());

    Assert.assertTrue(lfr.hasNext());
    chunk = lfr.next(0);
    Assert.assertNull(chunk);
    Assert.assertEquals(19, lfr.getOffset());

    Assert.assertTrue(lfr.hasNext());
    chunk = lfr.next(0);
    Assert.assertNull(chunk);
    Assert.assertEquals(19, lfr.getOffset());

    Files.move(file, Paths.get(file.getParent().toString(), UUID.randomUUID().toString()));
    Thread.sleep(SingleLineLiveFileReader.REFRESH_INTERVAL + 1);

    Assert.assertFalse(lfr.hasNext());
    Assert.assertEquals(19, lfr.getOffset());

    lfr.close();
  }

  @Test
  public void testTruncateMultipleReads() throws Exception {
    Path file = createFile(Arrays.asList("Hello1234567890\n", "Hello\n"));
    LiveFile lf = new LiveFile(file);
    LiveFileReader lfr = new SingleLineLiveFileReader(LogRollModeFactory.REVERSE_COUNTER.get(file.getFileName().toString(), ""),null, lf, Charset.defaultCharset(), 0, 6);

    Assert.assertTrue(lfr.hasNext());
    LiveFileChunk chunk = lfr.next(0);
    Assert.assertNotNull(chunk);
    Assert.assertTrue(chunk.isTruncated());
    Assert.assertEquals(0, chunk.getOffset());
    Assert.assertEquals(6, chunk.getLength());
    Assert.assertEquals("Hello1", readChunk(chunk));
    Assert.assertEquals(-6, lfr.getOffset());

    Assert.assertTrue(lfr.hasNext());
    chunk = lfr.next(0);
    Assert.assertNull(chunk);
    Assert.assertEquals(-12, lfr.getOffset());

    Assert.assertTrue(lfr.hasNext());
    chunk = lfr.next(0);
    Assert.assertNotNull(chunk);

    Assert.assertFalse(chunk.isTruncated());
    Assert.assertEquals(16, chunk.getOffset());
    Assert.assertEquals(6, chunk.getLength());
    Assert.assertEquals("Hello\n", readChunk(chunk));
    Assert.assertEquals(22, lfr.getOffset());

    Assert.assertTrue(lfr.hasNext());
    chunk = lfr.next(0);
    Assert.assertNull(chunk);
    Assert.assertEquals(22, lfr.getOffset());

    Assert.assertTrue(lfr.hasNext());
    chunk = lfr.next(0);
    Assert.assertNull(chunk);
    Assert.assertEquals(22, lfr.getOffset());

    Files.move(file, Paths.get(file.getParent().toString(), UUID.randomUUID().toString()));
    Thread.sleep(SingleLineLiveFileReader.REFRESH_INTERVAL + 1);

    Assert.assertFalse(lfr.hasNext());
    Assert.assertEquals(22, lfr.getOffset());

    lfr.close();
  }

  @Test
  public void testOneLineReadFromTruncatedOffset() throws Exception {
    Path file = createFile(Arrays.asList("Hello1\n", "Hello\n"));
    LiveFile lf = new LiveFile(file);
    LiveFileReader lfr = new SingleLineLiveFileReader(LogRollModeFactory.REVERSE_COUNTER.get(file.getFileName().toString(), ""),null, lf, Charset.defaultCharset(), -3, 10);

    Assert.assertTrue(lfr.hasNext());
    LiveFileChunk chunk = lfr.next(0);
    Assert.assertNotNull(chunk);

    Assert.assertFalse(chunk.isTruncated());
    Assert.assertEquals(7, chunk.getOffset());
    Assert.assertEquals(6, chunk.getLength());
    Assert.assertEquals("Hello\n", readChunk(chunk));
    Assert.assertEquals(13, lfr.getOffset());

    Assert.assertTrue(lfr.hasNext());
    Assert.assertEquals(13, lfr.getOffset());

    lfr.close();
  }

  @Test
  public void testTruncatedChunkAndNoEOLInLast() throws Exception {
    Path file = createFile(Arrays.asList("Hello1"));
    LiveFile lf = new LiveFile(file);
    LiveFileReader lfr = new SingleLineLiveFileReader(LogRollModeFactory.REVERSE_COUNTER.get(file.getFileName().toString(), ""),null, lf, Charset.defaultCharset(), 0, 6);

    Assert.assertTrue(lfr.hasNext());
    LiveFileChunk chunk = lfr.next(0);
    Assert.assertNotNull(chunk);

    Assert.assertTrue(chunk.isTruncated());
    Assert.assertEquals(0, chunk.getOffset());
    Assert.assertEquals(6, chunk.getLength());
    Assert.assertEquals("Hello1", readChunk(chunk));
    Assert.assertEquals(-6, lfr.getOffset());

    Assert.assertTrue(lfr.hasNext());
    Assert.assertEquals(-6, lfr.getOffset());

    chunk = lfr.next(0);
    Assert.assertNull(chunk);
    Assert.assertEquals(-6, lfr.getOffset());


    lfr.close();
  }

  @Test
  public void testNextTimeout() throws Exception {
    Path file = createFile(Arrays.asList("Hello1"));
    LiveFile lf = new LiveFile(file);
    LiveFileReader lfr = new SingleLineLiveFileReader(LogRollModeFactory.REVERSE_COUNTER.get(file.getFileName().toString(), ""),null, lf, Charset.defaultCharset(), 0, 10);

    Assert.assertTrue(lfr.hasNext());
    long start = System.currentTimeMillis();
    LiveFileChunk chunk = lfr.next(10);
    Assert.assertNull(chunk);
    Assert.assertTrue(System.currentTimeMillis() - start >= 10);
    lfr.close();
  }

  @Test
  public void testReadWithinTimeout() throws Exception {
    Path file = createFile(Arrays.asList("Hello1\n"));
    LiveFile lf = new LiveFile(file);
    LiveFileReader lfr = new SingleLineLiveFileReader(LogRollModeFactory.REVERSE_COUNTER.get(file.getFileName().toString(), ""),null, lf, Charset.defaultCharset(), 0, 10);

    Assert.assertTrue(lfr.hasNext());
    long start = System.currentTimeMillis();
    LiveFileChunk chunk = lfr.next(1000);
    Assert.assertNotNull(chunk);
    Assert.assertEquals("Hello1\n", readChunk(chunk));
    Assert.assertTrue(System.currentTimeMillis() - start < 1000);
    lfr.close();
  }

  @Test
  public void testCRLFLines() throws Exception {
    Path file = createFile(Arrays.asList("Hello1\r\n", "Hello\r\n"));
    LiveFile lf = new LiveFile(file);

    //multiple lines in one chunk
    LiveFileReader lfr = new SingleLineLiveFileReader(LogRollModeFactory.REVERSE_COUNTER.get(file.getFileName().toString(), ""),null, lf, Charset.defaultCharset(), 0, 20);
    Assert.assertTrue(lfr.hasNext());
    LiveFileChunk chunk = lfr.next(0);
    Assert.assertNotNull(chunk);
    Assert.assertFalse(chunk.isTruncated());
    Assert.assertEquals(0, chunk.getOffset());
    Assert.assertEquals(15, chunk.getLength());
    Assert.assertEquals("Hello1\r\nHello\r\n", readChunk(chunk));
    Assert.assertEquals(15, lfr.getOffset());

    //1.5 lines in one chunk
    lfr = new SingleLineLiveFileReader(LogRollModeFactory.REVERSE_COUNTER.get(file.getFileName().toString(), ""),null, lf, Charset.defaultCharset(), 0, 10);
    Assert.assertTrue(lfr.hasNext());
    chunk = lfr.next(0);
    Assert.assertNotNull(chunk);
    Assert.assertFalse(chunk.isTruncated());
    Assert.assertEquals(0, chunk.getOffset());
    Assert.assertEquals(8, chunk.getLength());
    Assert.assertEquals("Hello1\r\n", readChunk(chunk));
    Assert.assertEquals(8, lfr.getOffset());
    chunk = lfr.next(0);
    Assert.assertNotNull(chunk);
    Assert.assertTrue(lfr.hasNext());
    Assert.assertNotNull(chunk);
    Assert.assertFalse(chunk.isTruncated());
    Assert.assertEquals(8, chunk.getOffset());
    Assert.assertEquals(7, chunk.getLength());
    Assert.assertEquals("Hello\r\n", readChunk(chunk));
    Assert.assertEquals(15, lfr.getOffset());

    //first line truncated after \r\n
    lfr = new SingleLineLiveFileReader(LogRollModeFactory.REVERSE_COUNTER.get(file.getFileName().toString(), ""),null, lf, Charset.defaultCharset(), 0, 8);
    Assert.assertTrue(lfr.hasNext());
    chunk = lfr.next(0);
    Assert.assertNotNull(chunk);
    Assert.assertFalse(chunk.isTruncated());
    Assert.assertEquals(0, chunk.getOffset());
    Assert.assertEquals(8, chunk.getLength());
    Assert.assertEquals("Hello1\r\n", readChunk(chunk));
    Assert.assertEquals(8, lfr.getOffset());
    chunk = lfr.next(0);
    Assert.assertNotNull(chunk);
    Assert.assertTrue(lfr.hasNext());
    Assert.assertNotNull(chunk);
    Assert.assertFalse(chunk.isTruncated());
    Assert.assertEquals(8, chunk.getOffset());
    Assert.assertEquals(7, chunk.getLength());
    Assert.assertEquals("Hello\r\n", readChunk(chunk));
    Assert.assertEquals(15, lfr.getOffset());

    //first line truncated after \r
    lfr = new SingleLineLiveFileReader(LogRollModeFactory.REVERSE_COUNTER.get(file.getFileName().toString(), ""),null, lf, Charset.defaultCharset(), 0, 7);
    Assert.assertTrue(lfr.hasNext());
    chunk = lfr.next(0);
    Assert.assertNotNull(chunk);
    Assert.assertTrue(chunk.isTruncated());
    Assert.assertEquals(0, chunk.getOffset());
    Assert.assertEquals(7, chunk.getLength());
    Assert.assertEquals("Hello1\r", readChunk(chunk));
    Assert.assertEquals(-7, lfr.getOffset());
    chunk = lfr.next(0);
    Assert.assertNotNull(chunk);
    Assert.assertTrue(lfr.hasNext());
    Assert.assertNotNull(chunk);
    Assert.assertFalse(chunk.isTruncated());
    Assert.assertEquals(8, chunk.getOffset());
    Assert.assertEquals(7, chunk.getLength());
    Assert.assertEquals("Hello\r\n", readChunk(chunk));
    Assert.assertEquals(15, lfr.getOffset());

    lfr.close();
  }

  @Test
  public void testTag() throws Exception {
    Path file = createFile(Arrays.asList("Hello1\r\n", "Hello\r\n"));
    LiveFile lf = new LiveFile(file);

    //multiple lines in one chunk
    LiveFileReader lfr =
        new SingleLineLiveFileReader(LogRollModeFactory.REVERSE_COUNTER.get(file.getFileName().toString(), ""), "tag", lf,
                           Charset.defaultCharset(), 0, 20);
    Assert.assertTrue(lfr.hasNext());
    LiveFileChunk chunk = lfr.next(0);
    Assert.assertNotNull(chunk);
    Assert.assertEquals("tag", chunk.getTag());
    lfr.close();
  }

}
