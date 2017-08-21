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
