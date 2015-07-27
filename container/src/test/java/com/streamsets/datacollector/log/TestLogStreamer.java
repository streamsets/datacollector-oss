/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.log;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.streamsets.datacollector.log.LogStreamer;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.util.UUID;

public class TestLogStreamer {
  private static File LOG;

  @BeforeClass
  public static void setup() throws Exception {
    File dir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(dir.mkdirs());
    LOG = new File(dir, "x.log");
    Writer writer = new FileWriter(LOG);
    for (int i = 0; i < 10; i++) {
      writer.write("" + i + ":ABC\n");
    }
    writer.close();
  }

  @Test
  public void testStreamerFullLen() throws Exception {
    LogStreamer streamer = new LogStreamer(LOG.getAbsolutePath(), LOG.length(), 6);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    streamer.stream(baos);
    baos.close();
    Assert.assertEquals(LOG.length() - 6, streamer.getNewEndingOffset());
    Assert.assertEquals("9:ABC\n", baos.toString());

    streamer = new LogStreamer(LOG.getAbsolutePath(), streamer.getNewEndingOffset(), 6);
    baos = new ByteArrayOutputStream();
    streamer.stream(baos);
    baos.close();
    Assert.assertEquals(LOG.length() - 6 - 6, streamer.getNewEndingOffset());
    Assert.assertEquals("8:ABC\n", baos.toString());
  }

  @Test
  public void testStreamerPartialLen() throws Exception {
    LogStreamer streamer = new LogStreamer(LOG.getAbsolutePath(), 3, 6);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    streamer.stream(baos);
    baos.close();
    Assert.assertEquals(0, streamer.getNewEndingOffset());
    Assert.assertEquals("0:A", baos.toString());
  }

}
