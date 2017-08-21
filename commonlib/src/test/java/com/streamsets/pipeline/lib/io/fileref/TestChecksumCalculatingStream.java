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

import com.streamsets.pipeline.lib.generator.StreamCloseEventHandler;
import com.streamsets.pipeline.lib.hashing.HashingUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TestChecksumCalculatingStream {

  @Test
  public void testWithoutStreamHandler() throws Exception {
    try (ChecksumCalculatingWrapperStream<InputStream> stream
             = new ChecksumCalculatingWrapperStream<InputStream>(
                 new ByteArrayInputStream(FileRefTestUtil.TEXT.getBytes()), HashingUtil.HashType.MD5, null)
    ) {}
  }

  @Test
  public void testWithStreamHandler() throws Exception {
    final AtomicBoolean streamHandlerCalled = new AtomicBoolean(false);
    ChecksumCalculatingWrapperStream<InputStream> stream = null;

    final StreamCloseEventHandler streamCloseEventHandler = new StreamCloseEventHandler<Map<String, Object>>() {
      @Override
      public void handleCloseEvent(Map<String, Object> eventInfo) {
        streamHandlerCalled.compareAndSet(false, true);
      }
    };

    try {
      stream = new ChecksumCalculatingWrapperStream<InputStream>(
          new ByteArrayInputStream(FileRefTestUtil.TEXT.getBytes()), HashingUtil.HashType.MD5, streamCloseEventHandler);
      stream.read(new byte[FileRefTestUtil.TEXT.length()]);
    } finally {
      if (stream != null) {
        stream.close();
      }
    }
    Assert.assertTrue(streamHandlerCalled.get());
  }

  @Test
  public void testChecksumAfterStreamClose() throws Exception {
    ChecksumCalculatingWrapperStream<InputStream> stream = null;
    try {
      stream = new ChecksumCalculatingWrapperStream<InputStream>(
          new ByteArrayInputStream(FileRefTestUtil.TEXT.getBytes()), HashingUtil.HashType.SHA1, null);
      stream.read(new byte[FileRefTestUtil.TEXT.length()]);
    } finally {
      if (stream != null) {
        stream.close();
      }
    }
    Assert.assertEquals(
        HashingUtil.getHasher(HashingUtil.HashType.SHA1)
            .hashString(FileRefTestUtil.TEXT, Charset.defaultCharset()).toString(),
        stream.getCalculatedChecksum()
    );
  }

  @Test(expected = IllegalStateException.class)
  public void testChecksumBeforeStreamClose() throws Exception {
    ChecksumCalculatingWrapperStream<InputStream> stream = null;
    try {
      stream = new ChecksumCalculatingWrapperStream<InputStream>(
          new ByteArrayInputStream(FileRefTestUtil.TEXT.getBytes()), HashingUtil.HashType.MD5, null);
      stream.read(new byte[FileRefTestUtil.TEXT.length()]);
      stream.getCalculatedChecksum();
      Assert.fail("Getting checksum before stream close should fail");
    } finally {
      if (stream != null) {
        stream.close();
      }
    }
  }

  @Test
  public void testMultipleStreamCloseNoError() throws Exception {
    ChecksumCalculatingWrapperStream<InputStream> stream = null;

    final AtomicInteger atomicInteger = new AtomicInteger(0);

    final StreamCloseEventHandler streamCloseEventHandler = new StreamCloseEventHandler<Map<String, Object>>() {
      @Override
      public void handleCloseEvent(Map<String, Object> eventInfo) {
        atomicInteger.incrementAndGet();
      }
    };

    try {
      stream = new ChecksumCalculatingWrapperStream<InputStream>(
          new ByteArrayInputStream(FileRefTestUtil.TEXT.getBytes()), HashingUtil.HashType.MD5, streamCloseEventHandler);
      stream.read(new byte[FileRefTestUtil.TEXT.length()]);
    } finally {
      if (stream != null) {
        stream.close();
        stream.close();
      }
    }
    Assert.assertEquals(1, atomicInteger.get());
  }
}
