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

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.lib.parser.shaded.com.google.code.regexp.Pattern;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;

public class TestMultiLineLiveFileReader {

  private static LiveFileChunk toChunk(String ... lines) {
    return toChunk(false, lines);
  }

  private static List<String> toLines(List<LiveFileChunk> chunks) {
    List<String> lines = new ArrayList<>();
    for (LiveFileChunk chunk : chunks) {
      if (chunk != null) {
        for (FileLine line : chunk.getLines()) {
          lines.add(line.getText());
        }
      }
    }
    return lines;
  }

  private static LiveFileChunk toChunk(boolean truncated, String ... lines) {
    byte[][] lineBytes = new byte[lines.length][];
    int totalSize = 0;
    for (int i = 0; i < lines.length; i++) {
      lineBytes[i] = lines[i].getBytes();
      totalSize += lineBytes[i].length;
    }
    byte[] buffer = new byte[totalSize];
    int pos = 0;
    for (int i = 0; i < lineBytes.length; i++) {
      System.arraycopy(lineBytes[i], 0, buffer, pos, lineBytes[i].length);
      pos += lineBytes[i].length;
    }
    return new LiveFileChunk("tag", Mockito.mock(LiveFile.class), StandardCharsets.UTF_8, buffer, 0, pos, truncated);
  }

  private static class MockLineLiveFileReader implements LiveFileReader {
    private final Queue<LiveFileChunk> chunks;
    private final boolean returnNulls;
    private int nextCount;
    private long offset;

    public MockLineLiveFileReader(boolean returnNulls, LiveFileChunk ... chunks) {
      this.chunks = new ArrayDeque<>(ImmutableList.copyOf(chunks));
      this.returnNulls = returnNulls;
    }

    @Override
    public LiveFile getLiveFile() {
      return Mockito.mock(LiveFile.class);
    }

    @Override
    public Charset getCharset() {
      return StandardCharsets.UTF_8;
    }

    @Override
    public long getOffset() {
      return offset;
    }

    @Override
    public boolean hasNext() throws IOException {
      return !chunks.isEmpty();
    }

    @Override
    public LiveFileChunk next(long waitMillis) throws IOException {
      LiveFileChunk chunk = null;
      if (!returnNulls || nextCount % 2 != 0) {
        chunk =chunks.poll();
        offset += chunk.getLength();
      }
      nextCount++;
      return chunk;
    }

    @Override
    public void close() throws IOException {

    }
  }

  @Test
  public void testMockChunks() {
    LiveFileChunk chunk = toChunk("Hello\n");
    Assert.assertEquals(1, chunk.getLines().size());
    Assert.assertEquals("Hello\n", chunk.getLines().get(0).getText());
    chunk = toChunk("Hello\n", "Bye\n");
    Assert.assertEquals(2, chunk.getLines().size());

    Assert.assertEquals("Hello\n", chunk.getLines().get(0).getText());
    Assert.assertEquals(0, chunk.getLines().get(0).getOffset());
    Assert.assertEquals(0, chunk.getLines().get(0).getFileOffset());
    Assert.assertEquals("Hello\n".length(), chunk.getLines().get(0).getLength());

    Assert.assertEquals("Bye\n", chunk.getLines().get(1).getText());
    Assert.assertEquals("Hello\n".length(), chunk.getLines().get(1).getOffset());
    Assert.assertEquals("Hello\n".length(), chunk.getLines().get(1).getFileOffset());
    Assert.assertEquals("Bye\n".length(), chunk.getLines().get(1).getLength());
  }

  @Test
  public void testChunksToLines() {
    List<LiveFileChunk> chunks = Arrays.asList(toChunk("Hello\n", "Hola\n"), null, toChunk("Bye\n"));
    Assert.assertEquals(ImmutableList.of("Hello\n", "Hola\n", "Bye\n"), toLines(chunks));
  }

  @Test
  public void testMockLineLiveFileReader() throws Exception {
    Assert.assertFalse(new MockLineLiveFileReader(false).hasNext());
    Assert.assertEquals(0, new MockLineLiveFileReader(false).getOffset());
    LiveFileChunk chunk1 = toChunk("Hello\n");
    LiveFileReader reader = new MockLineLiveFileReader(false, chunk1);
    Assert.assertTrue(reader.hasNext());
    Assert.assertEquals(0, reader.getOffset());
    Assert.assertEquals(chunk1, reader.next(0));
    Assert.assertEquals(chunk1.getLength(), reader.getOffset());
    Assert.assertFalse(reader.hasNext());
    reader.close();

    LiveFileChunk chunk2= toChunk("Bye\n");
    reader = new MockLineLiveFileReader(true, chunk1, chunk2);
    Assert.assertEquals(0, reader.getOffset());
    Assert.assertTrue(reader.hasNext());
    Assert.assertEquals(null, reader.next(0));
    Assert.assertEquals(0, reader.getOffset());
    Assert.assertTrue(reader.hasNext());
    Assert.assertEquals(chunk1, reader.next(0));
    Assert.assertEquals(chunk1.getLength(), reader.getOffset());
    Assert.assertTrue(reader.hasNext());
    Assert.assertEquals(null, reader.next(0));
    Assert.assertEquals(chunk1.getLength(), reader.getOffset());
    Assert.assertTrue(reader.hasNext());
    Assert.assertEquals(chunk2, reader.next(0));
    Assert.assertEquals(chunk1.getLength() + chunk2.getLength(), reader.getOffset());
    Assert.assertFalse(reader.hasNext());
    Assert.assertEquals(chunk1.getLength() + chunk2.getLength(), reader.getOffset());

    // next() after EOF returns null
    Assert.assertNull(reader.next(0));

    reader.close();
  }

  private void testAllSingleLines(boolean returnNulls) throws Exception {
    LiveFileChunk chunk1 = toChunk("Hello\n");
    LiveFileChunk chunk2= toChunk("Bye\n");
    List<LiveFileChunk> expected = ImmutableList.of(chunk1, chunk2);
    List<LiveFileChunk> got = new ArrayList<>();

    LiveFileReader reader = new MockLineLiveFileReader(returnNulls, chunk1, chunk2);
    reader = new MultiLineLiveFileReader("t", reader, Pattern.compile(".*"));
    while (reader.hasNext()) {
      LiveFileChunk chunk = reader.next(0);
      got.add(chunk);
    }
    reader.close();
    Assert.assertEquals(toLines(expected), toLines(got));
  }

  @Test
  public void testAllSingleLinesNotNulls() throws Exception {
    testAllSingleLines(false);
  }

  @Test
  public void testAllSingleLinesNulls() throws Exception {
    testAllSingleLines(true);
  }

  private void testMultiLineWithinChunks(boolean returnNulls) throws Exception {
    LiveFileChunk chunk1 = toChunk("A0\n", "A1\n", "B1\n", "A2\n");
    LiveFileChunk chunk2= toChunk("A3\n", "B3\n");
    List<LiveFileChunk> got = new ArrayList<>();

    LiveFileReader reader = new MockLineLiveFileReader(returnNulls, chunk1, chunk2);
    reader = new MultiLineLiveFileReader("t", reader, Pattern.compile("A.*"));
    while (reader.hasNext()) {
      LiveFileChunk chunk = reader.next(0);
      got.add(chunk);
    }
    reader.close();
    Assert.assertEquals(ImmutableList.of("A0\n", "A1\nB1\n", "A2\n", "A3\nB3\n"), toLines(got));
  }

  @Test
  public void testMultiLineWithinChunksNotNulls() throws Exception {
    testMultiLineWithinChunks(false);
  }

  @Test
  public void testMultiLineWithinChunksNulls() throws Exception {
    testMultiLineWithinChunks(true);
  }

  private void testMultiLineLastChunk(boolean returnNulls) throws Exception {
    LiveFileChunk chunk1 = toChunk("A0\n", "A1\n", "B1\n", "A2\n", "B2\n");
    List<LiveFileChunk> got = new ArrayList<>();

    LiveFileReader reader = new MockLineLiveFileReader(returnNulls, chunk1);
    reader = new MultiLineLiveFileReader("t", reader, Pattern.compile("A.*"));
    while (reader.hasNext()) {
      LiveFileChunk chunk = reader.next(0);
      got.add(chunk);
    }
    reader.close();
    Assert.assertEquals(ImmutableList.of("A0\n", "A1\nB1\n", "A2\nB2\n"), toLines(got));
  }

  @Test
  public void testMultiLineLastChunkNotNulls() throws Exception {
    testMultiLineLastChunk(false);
  }

  @Test
  public void testMultiLineLastChunkNulls() throws Exception {
    testMultiLineLastChunk(true);
  }

  private void testMultiLineAcrossChunks(boolean returnNulls) throws Exception {
    LiveFileChunk chunk1 = toChunk("A0\n", "A1\n", "B1\n", "A2\n");
    LiveFileChunk chunk2= toChunk("B2\n", "A3\n", "B3\n");
    LiveFileChunk chunk3= toChunk("A4\n", "A5\n");
    List<LiveFileChunk> got = new ArrayList<>();

    LiveFileReader reader = new MockLineLiveFileReader(returnNulls, chunk1, chunk2, chunk3);
    reader = new MultiLineLiveFileReader("t", reader, Pattern.compile("A.*"));
    while (reader.hasNext()) {
      LiveFileChunk chunk = reader.next(0);
      got.add(chunk);
    }
    reader.close();
    Assert.assertEquals(ImmutableList.of("A0\n", "A1\nB1\n", "A2\nB2\n", "A3\nB3\n", "A4\n", "A5\n"), toLines(got));
  }

  @Test
  public void testMultiLineAcrossChunksNotNulls() throws Exception {
    testMultiLineAcrossChunks(false);
  }

  @Test
  public void testMultiLineAcrossChunksNulls() throws Exception {
    testMultiLineAcrossChunks(true);
  }

  @Test
  public void testOffset() throws Exception {
    LiveFileChunk chunk1 = toChunk("A0\n", "A1\n", "B1\n", "A2\n");
    LiveFileChunk chunk2= toChunk("B2\n", "A3\n", "B3\n");
    LiveFileChunk chunk3= toChunk("A4\n", "A5\n");
    List<LiveFileChunk> got = new ArrayList<>();

    LiveFileReader reader = new MockLineLiveFileReader(false, chunk1, chunk2, chunk3);
    reader = new MultiLineLiveFileReader("t", reader, Pattern.compile("A.*"));
    Assert.assertEquals(0, reader.getOffset());

    Assert.assertTrue(reader.hasNext());
    LiveFileChunk chunk = reader.next(0); //A0 A1 B1
    Assert.assertEquals(3 * 3, reader.getOffset());

    Assert.assertTrue(reader.hasNext());
    chunk = reader.next(0); //A2 B2
    Assert.assertEquals(3 * 3 + 2 * 3, reader.getOffset());

    Assert.assertTrue(reader.hasNext());
    chunk = reader.next(0); //A3 B3 A4
    Assert.assertEquals(3 * 3 + 2 * 3 + 3 * 3, reader.getOffset());

    Assert.assertTrue(reader.hasNext());
    chunk = reader.next(0);  //A5
    Assert.assertEquals(3 * 3 + 2 * 3 + 3 * 3 + 1 * 3, reader.getOffset());

    Assert.assertFalse(reader.hasNext());
    Assert.assertEquals(3 * 3 + 2 * 3 + 3 * 3 + 1 * 3, reader.getOffset());

    reader.close();

  }
}
