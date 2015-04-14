/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.io;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.Charset;

public class TestLiveFileChunk {

  @Test
  public void testChunkGetters() throws IOException {
    LiveFileChunk chunk = new LiveFileChunk("Hello".getBytes(), Charset.forName("UTF-8"), 1, 4, true);
    Assert.assertEquals("Hell", IOUtils.readLines(chunk.getReader()).get(0));
    Assert.assertEquals(1, chunk.getOffset());
    Assert.assertEquals(4, chunk.getLength());
    Assert.assertTrue(chunk.isTruncated());
    Assert.assertEquals(1, chunk.getLines().size());
    Assert.assertEquals("Hell", chunk.getLines().get(0).getText());
    Assert.assertEquals(1, chunk.getLines().get(0).getFileOffset());
  }

  @Test
  public void testChunkLinesLF() throws IOException {
    byte[] data = "Hello\nBye\n".getBytes(Charset.forName("UTF-8"));
    LiveFileChunk chunk = new LiveFileChunk(data, Charset.forName("UTF-8"), 1, data.length, true);
    Assert.assertEquals(2, chunk.getLines().size());
    Assert.assertEquals("Hello\n", chunk.getLines().get(0).getText());
    Assert.assertEquals(1, chunk.getLines().get(0).getFileOffset());
    Assert.assertEquals("Bye\n", chunk.getLines().get(1).getText());
    Assert.assertEquals(7, chunk.getLines().get(1).getFileOffset());
  }

  @Test
  public void testChunkLinesCRLF() throws IOException {
    byte[] data = "Hello\r\nBye\r\n".getBytes(Charset.forName("UTF-8"));
    LiveFileChunk chunk = new LiveFileChunk(data, Charset.forName("UTF-8"), 1, data.length, true);
    Assert.assertEquals(2, chunk.getLines().size());
    Assert.assertEquals("Hello\r\n", chunk.getLines().get(0).getText());
    Assert.assertEquals(1, chunk.getLines().get(0).getFileOffset());
    Assert.assertEquals("Bye\r\n", chunk.getLines().get(1).getText());
    Assert.assertEquals(8, chunk.getLines().get(1).getFileOffset());
  }

}
