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

import com.google.common.util.concurrent.RateLimiter;
import com.streamsets.pipeline.api.FileRef;
import com.streamsets.pipeline.api.Stage;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TestRateLimitingWrapperStream {
  private static final int RATE_LIMIT = new Random().nextInt(4) + 1;
  private File testDir;
  private Stage.Context context;

  @Before
  public void setup() throws Exception {
    testDir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(testDir.mkdirs());
    FileRefTestUtil.writePredefinedTextToFile(testDir);
    context = Mockito.mock(Stage.Context.class);
  }

  @After
  public void tearDown() throws Exception {
    testDir.delete();
  }

  private static FileRef getLocalFileRef(File testDir, double rateLimit) throws IOException {
    return new LocalFileRef.Builder()
        .filePath(FileRefTestUtil.getSourceFilePath(testDir))
        .bufferSize(FileRefTestUtil.TEXT.getBytes().length / 2)
        .totalSizeInBytes(FileRefTestUtil.TEXT.getBytes().length)
        .createMetrics(false)
        .rateLimit(rateLimit)
        .build();
  }

  private <T extends AutoCloseable> void intercept(T stream, final AtomicInteger bytesWishToBeRead, final AtomicBoolean isRateLimiterAcquired) {
    RateLimiter rateLimiter =  (RateLimiter) Whitebox.getInternalState(stream, "rateLimiter");
    Assert.assertEquals(RATE_LIMIT, rateLimiter.getRate(), 0);
    Mockito.doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
        Object[] args = invocationOnMock.getArguments();
        bytesWishToBeRead.compareAndSet(-1, (int)args[0]);
        return invocationOnMock.callRealMethod();
      }
    }).when((RateLimitingWrapperStream)stream).performPreReadOperation(Mockito.anyInt());

    Mockito.doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
        //basically book keep that we are acquiring the rate limit.
        isRateLimiterAcquired.compareAndSet(false, true);
        return null;
      }
    }).when((RateLimitingWrapperStream)stream).acquire(Mockito.anyInt());
  }

  private static <T extends AutoCloseable> long getRemainingStreamSize(T stream) {
    return (long) Whitebox.getInternalState(stream, "remainingStreamSize");
  }

  private <T extends AutoCloseable> void checkState(
      T stream,
      long remainingFileSize,
      int readCalledWithBytesToBeRead,
      AtomicInteger bytesWishToBeRead,
      AtomicBoolean isRateLimiterAcquired
  ) {
    Assert.assertEquals(remainingFileSize, getRemainingStreamSize(stream));
    Assert.assertEquals(readCalledWithBytesToBeRead, bytesWishToBeRead.get());
    long optimizedBytesWishToBeRead = Math.min(remainingFileSize, readCalledWithBytesToBeRead);
    if (optimizedBytesWishToBeRead > 0) {
      Assert.assertTrue(isRateLimiterAcquired.get());
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegativeRateLimit() throws Exception {
    new RateLimitingWrapperStream<>(new ByteArrayInputStream("a".getBytes()), 1, -2);
  }

  @Test
  public void testIOReadParameterLess() throws Exception {
    FileRef fileRef = getLocalFileRef(testDir, RATE_LIMIT);
    long fileSize = Files.size(Paths.get(FileRefTestUtil.getSourceFilePath(testDir)));
    long remainingFileSize = fileSize;
    try (InputStream is = Mockito.spy(fileRef.createInputStream(context, InputStream.class))) {
      AtomicInteger bytesWishToBeRead = new AtomicInteger(-1);
      AtomicBoolean isRateLimiterAcquired = new AtomicBoolean(false);
      intercept(is, bytesWishToBeRead, isRateLimiterAcquired);
      Assert.assertEquals(fileSize, getRemainingStreamSize(is));
      while(is.read() != -1) {
        remainingFileSize--;
        checkState(is, remainingFileSize, 1, bytesWishToBeRead, isRateLimiterAcquired);
        bytesWishToBeRead.set(-1);
        isRateLimiterAcquired.set(false);
      }
    }
  }

  @Test
  public void testIOReadParameterized1() throws Exception {
    FileRef fileRef = getLocalFileRef(testDir, RATE_LIMIT);
    long fileSize = Files.size(Paths.get(FileRefTestUtil.getSourceFilePath(testDir)));
    long remainingFileSize = fileSize;
    try (InputStream is = Mockito.spy(fileRef.createInputStream(context, InputStream.class))) {
      AtomicInteger bytesWishToBeRead = new AtomicInteger(-1);
      AtomicBoolean isRateLimiterAcquired = new AtomicBoolean(false);
      intercept(is, bytesWishToBeRead, isRateLimiterAcquired);
      Assert.assertEquals(fileSize, getRemainingStreamSize(is));
      int bytesRead;
      byte[] b = new byte[10];
      while( (bytesRead = is.read(b)) > 0) {
        remainingFileSize -= bytesRead;
        checkState(is, remainingFileSize, b.length, bytesWishToBeRead, isRateLimiterAcquired);
        bytesWishToBeRead.set(-1);
        isRateLimiterAcquired.set(false);
      }
      Assert.assertFalse(isRateLimiterAcquired.get());
    }
  }

  @Test
  public void testIOReadParameterized2() throws Exception {
    FileRef fileRef = getLocalFileRef(testDir, RATE_LIMIT);
    long fileSize = Files.size(Paths.get(FileRefTestUtil.getSourceFilePath(testDir)));
    long remainingFileSize = fileSize;
    try (InputStream is = Mockito.spy(fileRef.createInputStream(context, InputStream.class))) {
      AtomicInteger bytesWishToBeRead = new AtomicInteger(-1);
      AtomicBoolean isRateLimiterAcquired = new AtomicBoolean(false);
      intercept(is, bytesWishToBeRead, isRateLimiterAcquired);
      Assert.assertEquals(fileSize, getRemainingStreamSize(is));
      int bytesRead;
      byte[] b = new byte[10];
      while( (bytesRead = is.read(b, 0, b.length)) > 0) {
        remainingFileSize -= bytesRead;
        checkState(is, remainingFileSize, b.length, bytesWishToBeRead, isRateLimiterAcquired);
        bytesWishToBeRead.set(-1);
        isRateLimiterAcquired.set(false);
      }
      Assert.assertFalse(isRateLimiterAcquired.get());
    }
  }

  @Test
  public void testIOReadsMixed() throws Exception {
    FileRef fileRef = getLocalFileRef(testDir, RATE_LIMIT);
    long fileSize = Files.size(Paths.get(FileRefTestUtil.getSourceFilePath(testDir)));
    long remainingFileSize = fileSize;
    try (InputStream is = Mockito.spy(fileRef.createInputStream(context, InputStream.class))) {
      AtomicInteger bytesWishToBeRead = new AtomicInteger(-1);
      AtomicBoolean isRateLimiterAcquired = new AtomicBoolean(false);
      intercept(is, bytesWishToBeRead, isRateLimiterAcquired);
      Assert.assertEquals(fileSize, getRemainingStreamSize(is));
      int bytesRead;
      while ((bytesRead= FileRefTestUtil.randomReadMethodsWithInputStream(is)) > 0) {
        remainingFileSize -= bytesRead;
        Assert.assertTrue(isRateLimiterAcquired.get());
        Assert.assertEquals(remainingFileSize, getRemainingStreamSize(is));
        bytesWishToBeRead.set(-1);
        isRateLimiterAcquired.set(false);
      }
      Assert.assertFalse(isRateLimiterAcquired.get());
    }
  }

  @Test
  public void testNIOReadWithDirectBuffer() throws Exception {
    FileRef fileRef = getLocalFileRef(testDir, RATE_LIMIT);
    long fileSize = Files.size(Paths.get(FileRefTestUtil.getSourceFilePath(testDir)));
    long remainingFileSize = fileSize;
    try (ReadableByteChannel is = Mockito.spy(fileRef.createInputStream(context, ReadableByteChannel.class))) {
      AtomicInteger bytesWishToBeRead = new AtomicInteger(-1);
      AtomicBoolean isRateLimiterAcquired = new AtomicBoolean(false);
      intercept(is, bytesWishToBeRead, isRateLimiterAcquired);
      Assert.assertEquals(fileSize, getRemainingStreamSize(is));
      ByteBuffer b = ByteBuffer.allocateDirect(10);
      int bytesRead;
      int freeSpaceInBuffer = b.remaining();
      while((bytesRead = is.read(b)) > 0) {
        remainingFileSize -= bytesRead;
        checkState(is, remainingFileSize, freeSpaceInBuffer, bytesWishToBeRead, isRateLimiterAcquired);
        bytesWishToBeRead.set(-1);
        isRateLimiterAcquired.set(false);
        b.compact();
        freeSpaceInBuffer = b.remaining();
      }
      Assert.assertFalse(isRateLimiterAcquired.get());
    }
  }

  @Test
  public void testNIOReadWithHeapByteBuffer() throws Exception {
    FileRef fileRef = getLocalFileRef(testDir, RATE_LIMIT);
    long fileSize = Files.size(Paths.get(FileRefTestUtil.getSourceFilePath(testDir)));
    long remainingFileSize = fileSize;
    try (ReadableByteChannel is = Mockito.spy(fileRef.createInputStream(context, ReadableByteChannel.class))) {
      AtomicInteger bytesWishToBeRead = new AtomicInteger(-1);
      AtomicBoolean isRateLimiterAcquired = new AtomicBoolean(false);
      intercept(is, bytesWishToBeRead, isRateLimiterAcquired);
      Assert.assertEquals(fileSize, getRemainingStreamSize(is));
      ByteBuffer b = ByteBuffer.allocate(10);
      int bytesRead;
      int freeSpaceInBuffer = b.remaining();
      while((bytesRead = is.read(b)) > 0) {
        remainingFileSize -= bytesRead;
        checkState(is, remainingFileSize, freeSpaceInBuffer, bytesWishToBeRead, isRateLimiterAcquired);
        bytesWishToBeRead.set(-1);
        isRateLimiterAcquired.set(false);
        b.compact();
        freeSpaceInBuffer = b.remaining();
      }
      Assert.assertFalse(isRateLimiterAcquired.get());
    }
  }
}
