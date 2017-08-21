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
