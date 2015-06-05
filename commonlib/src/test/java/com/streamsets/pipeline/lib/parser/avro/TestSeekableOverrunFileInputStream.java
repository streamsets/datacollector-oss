/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.avro;

import com.streamsets.pipeline.lib.util.SdcAvroTestUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;

public class TestSeekableOverrunFileInputStream {

  @Test
  public void testSeek() throws Exception {
    File file = SdcAvroTestUtil.createAvroDataFile();
    FileInputStream fIn = new FileInputStream(file);
    SeekableOverrunFileInputStream inputStream = new SeekableOverrunFileInputStream(fIn, 200, true);
    Assert.assertEquals(0, fIn.getChannel().position());
    inputStream.read(new byte[10]);
    Assert.assertEquals(10, fIn.getChannel().position());
    inputStream.seek(20);
    Assert.assertEquals(20, fIn.getChannel().position());
  }

  @Test
  public void testTell() throws Exception {
    File file = SdcAvroTestUtil.createAvroDataFile();
    FileInputStream fIn = new FileInputStream(file);
    SeekableOverrunFileInputStream inputStream = new SeekableOverrunFileInputStream(fIn, 200, true);
    Assert.assertEquals(0, inputStream.tell());
    inputStream.read(new byte[10]);
    Assert.assertEquals(10, inputStream.tell());
    fIn.getChannel().position(20);
    Assert.assertEquals(20, inputStream.tell());
  }

  @Test
  public void testLength() throws Exception {
    File file = SdcAvroTestUtil.createAvroDataFile();
    FileInputStream fIn = new FileInputStream(file);
    SeekableOverrunFileInputStream inputStream = new SeekableOverrunFileInputStream(fIn, 200, true);
    Assert.assertEquals(file.length(), inputStream.length());
  }
}
