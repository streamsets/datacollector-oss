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
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class TestLiveFileChunk {

  @Test
  public void testChunkGetters() throws IOException {
    LiveFile file = Mockito.mock(LiveFile.class);
    LiveFileChunk chunk = new LiveFileChunk("tag", file, StandardCharsets.UTF_8, "Hola\nHello".getBytes(), 1, 9, true);
    Assert.assertEquals("tag", chunk.getTag());
    Assert.assertEquals(file, chunk.getFile());
    Assert.assertEquals("Hola", IOUtils.readLines(chunk.getReader()).get(0));
    Assert.assertEquals("Hell", IOUtils.readLines(chunk.getReader()).get(1));
    Assert.assertEquals(1, chunk.getOffset());
    Assert.assertEquals(9, chunk.getLength());
    Assert.assertTrue(chunk.isTruncated());
    Assert.assertEquals(2, chunk.getLines().size());
    Assert.assertEquals("Hola\n", chunk.getLines().get(0).getText());
    Assert.assertEquals(1, chunk.getLines().get(0).getFileOffset());
    Assert.assertArrayEquals(chunk.getBuffer(), chunk.getLines().get(0).getBuffer());
    Assert.assertEquals(0, chunk.getLines().get(0).getOffset());
    Assert.assertEquals(5, chunk.getLines().get(0).getLength());
    Assert.assertEquals("Hell", chunk.getLines().get(1).getText());
    Assert.assertEquals(6, chunk.getLines().get(1).getFileOffset());
    Assert.assertEquals(chunk.getBuffer(), chunk.getLines().get(1).getBuffer());
    Assert.assertEquals(5, chunk.getLines().get(1).getOffset());
    Assert.assertEquals(4, chunk.getLines().get(1).getLength());
  }

  @Test
  public void testChunkLinesLF() throws IOException {
    byte[] data = "Hello\nBye\n".getBytes(StandardCharsets.UTF_8);
    LiveFileChunk chunk = new LiveFileChunk(null, null, StandardCharsets.UTF_8, data, 1, data.length, true);
    Assert.assertEquals(2, chunk.getLines().size());
    Assert.assertEquals("Hello\n", chunk.getLines().get(0).getText());
    Assert.assertEquals(1, chunk.getLines().get(0).getFileOffset());
    Assert.assertEquals("Bye\n", chunk.getLines().get(1).getText());
    Assert.assertEquals(7, chunk.getLines().get(1).getFileOffset());
  }

  @Test
  public void testChunkLinesCRLF() throws IOException {
    byte[] data = "Hello\r\nBye\r\n".getBytes(StandardCharsets.UTF_8);
    LiveFileChunk chunk = new LiveFileChunk(null, null, StandardCharsets.UTF_8, data, 1, data.length, true);
    Assert.assertEquals(2, chunk.getLines().size());
    Assert.assertEquals("Hello\r\n", chunk.getLines().get(0).getText());
    Assert.assertEquals(1, chunk.getLines().get(0).getFileOffset());
    Assert.assertEquals("Bye\r\n", chunk.getLines().get(1).getText());
    Assert.assertEquals(8, chunk.getLines().get(1).getFileOffset());
  }

}
