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
package com.streamsets.pipeline.lib.io.fileref;

import com.google.common.io.Files;
import com.streamsets.pipeline.api.FileRef;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.hashing.HashingUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.io.File;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Paths;
import java.util.UUID;

@RunWith(Parameterized.class)
public class TestVerifyChecksumWrapperStream {
  private final HashingUtil.HashType hashType;
  private String checksum;
  private File testDir;
  private Stage.Context context;

  @Parameterized.Parameters(name = "{index}: Checksum Algorithm - {0}")
  public static Object[] data() throws Exception {
    return HashingUtil.HashType.values();
  }

  public TestVerifyChecksumWrapperStream(HashingUtil.HashType hashType) {
    this.hashType = hashType;
  }

  @Before
  public void setup() throws Exception {
    testDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    testDir.mkdirs();
    FileRefTestUtil.writePredefinedTextToFile(testDir);
    context = Mockito.mock(Stage.Context.class);
    checksum = Files.hash(
        Paths.get(FileRefTestUtil.getSourceFilePath(testDir)).toFile(),
        HashingUtil.getHasher(hashType)
    ).toString();
  }

  @After
  public void tearDown() throws Exception {
    testDir.delete();
  }

  @Test
  public void testIOReadParameterLess() throws Exception {
    FileRef fileRef = FileRefTestUtil.getLocalFileRef(testDir, false, checksum, hashType);
    try (InputStream is = fileRef.createInputStream(context, InputStream.class)) {
      for(;is.read() != -1;);
    }
  }

  @Test
  public void testIOReadParameterized() throws Exception {
    FileRef fileRef = FileRefTestUtil.getLocalFileRef(testDir, false, checksum, hashType);
    try (InputStream is = fileRef.createInputStream(context, InputStream.class)) {
      byte[] b = new byte[10];
      for(;is.read(b, 0, b.length) > 0;);
    }
  }

  @Test
  public void testIOReadsMixed() throws Exception {
    FileRef fileRef = FileRefTestUtil.getLocalFileRef(testDir, false, checksum, hashType);
    try (InputStream is = fileRef.createInputStream(context, InputStream.class)) {
      while (FileRefTestUtil.randomReadMethodsWithInputStream(is) > 0);
    }
  }

  @Test
  public void testNIOReadWithDirectBuffer() throws Exception {
    FileRef fileRef = FileRefTestUtil.getLocalFileRef(testDir, false, checksum, hashType);
    try (ReadableByteChannel is = fileRef.createInputStream(context, ReadableByteChannel.class)) {
      ByteBuffer b = ByteBuffer.allocateDirect(10);
      while(is.read(b) > 0) {
        b.clear();
      }
    }
  }

  @Test
  public void testNIOReadWithHeapByteBuffer() throws Exception {
    FileRef fileRef = FileRefTestUtil.getLocalFileRef(testDir, false, checksum, hashType);
    try (ReadableByteChannel is = fileRef.createInputStream(context, ReadableByteChannel.class)) {
      ByteBuffer b = ByteBuffer.allocate(10);
      for(;is.read(b) > 0;) {
        b.clear();
      }
    }
  }
}
