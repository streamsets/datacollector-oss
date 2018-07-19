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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.streamsets.pipeline.api.FileRef;
import com.streamsets.pipeline.api.ProtoConfigurableEntity;
import com.streamsets.pipeline.api.Stage;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;

import java.io.File;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.Mockito.when;

public class TestMetricsEnabledWrapperStream {
  private File testDir;
  private Stage.Context context;
  private MapGauge gauge;

  public static class MapGauge implements Gauge<Map<String, Object>> {

    private final Map<String, Object> stateMap;

    public MapGauge() {
      stateMap = new ConcurrentHashMap<>();
      stateMap.put(FileRefUtil.COMPLETED_FILE_COUNT, 0l);
    }

    @Override
    public Map<String, Object> getValue() {
      return stateMap;
    }
  }

  @Before
  public void setup() throws Exception {
    testDir = new File("target", UUID.randomUUID().toString());
    testDir.mkdirs();
    FileRefTestUtil.writePredefinedTextToFile(testDir);
    gauge = new MapGauge();
    context = Mockito.mock(Stage.Context.class);
    when(context.createGauge(Mockito.any(), Mockito.any())).thenReturn(gauge);
    when(context.getGauge(Mockito.any())).thenReturn(gauge);
    when(context.getMeter(Mockito.any())).thenReturn(new Meter());
    when(context.getRunnerId()).thenReturn(0);
  }

  @After
  public void tearDown() throws Exception {
    testDir.delete();
  }

  private <T extends  AutoCloseable> long getRemainingBytes(T stream) {
    return ((Counter)Whitebox.getInternalState(stream, "remainingBytesCounter")).getCount();
  }

  private <T extends  AutoCloseable> long getSentBytes(T stream) {
    return ((Counter)Whitebox.getInternalState(stream, "sentBytesCounter")).getCount();
  }

  private <T extends AutoCloseable> void checkStateDuringReads(long fileSize, long remainingFileSize, T is) {
    Assert.assertEquals(remainingFileSize, getRemainingBytes(is));
    Assert.assertEquals(fileSize - remainingFileSize, getSentBytes(is));
    Assert.assertEquals(fileSize, getRemainingBytes(is) + getSentBytes(is));
  }


  private <T extends AutoCloseable>  void checkStateAfterReadCompletion(long fileSize, long remainingFileSize, T is) {
    Assert.assertEquals(0, remainingFileSize);
    Assert.assertEquals(fileSize, getSentBytes(is));
    Assert.assertEquals(remainingFileSize, getRemainingBytes(is));
  }

  @Test
  public void testIOReadParameterLess() throws Exception {
    FileRef fileRef = FileRefTestUtil.getLocalFileRef(testDir, true, null, null);
    long fileSize = Files.size(Paths.get(FileRefTestUtil.getSourceFilePath(testDir)));
    long remainingFileSize = fileSize;
    try (InputStream is = fileRef.createInputStream(context, InputStream.class)) {
      Assert.assertEquals(remainingFileSize, getRemainingBytes(is));
      while(is.read() != -1) {
        remainingFileSize--;
        checkStateDuringReads(fileSize, remainingFileSize, is);
      }
      checkStateAfterReadCompletion(fileSize, remainingFileSize, is);
    }
  }

  @Test
  public void testIOReadParameterized() throws Exception {
    FileRef fileRef = FileRefTestUtil.getLocalFileRef(testDir, true, null, null);
    long fileSize = Files.size(Paths.get(FileRefTestUtil.getSourceFilePath(testDir)));
    long remainingFileSize = fileSize;
    try (InputStream is = fileRef.createInputStream(context, InputStream.class)) {
      Assert.assertEquals(remainingFileSize, getRemainingBytes(is));
      int bytesRead;
      byte[] b = new byte[10];
      while( (bytesRead = is.read(b, 0, b.length)) > 0) {
        remainingFileSize -= bytesRead;
        checkStateDuringReads(fileSize, remainingFileSize, is);
      }
      checkStateAfterReadCompletion(fileSize, remainingFileSize, is);
    }
  }

  @Test
  public void testIOReadsMixed() throws Exception {
    FileRef fileRef = FileRefTestUtil.getLocalFileRef(testDir, true, null, null);
    long fileSize = Files.size(Paths.get(FileRefTestUtil.getSourceFilePath(testDir)));
    long remainingFileSize = fileSize;
    try (InputStream is = fileRef.createInputStream(context, InputStream.class)) {
      Assert.assertEquals(remainingFileSize, getRemainingBytes(is));
      int bytesRead;
      while ((bytesRead= FileRefTestUtil.randomReadMethodsWithInputStream(is)) > 0) {
        remainingFileSize -= bytesRead;
        checkStateDuringReads(fileSize, remainingFileSize, is);
      }
      checkStateAfterReadCompletion(fileSize, remainingFileSize, is);
    }
  }

  @Test
  public void testNIOReadWithDirectBuffer() throws Exception {
    FileRef fileRef = FileRefTestUtil.getLocalFileRef(testDir, true, null, null);
    long fileSize = Files.size(Paths.get(FileRefTestUtil.getSourceFilePath(testDir)));
    long remainingFileSize = fileSize;
    try (ReadableByteChannel is = fileRef.createInputStream(context, ReadableByteChannel.class)) {
      Assert.assertEquals(remainingFileSize, getRemainingBytes(is));
      int bytesRead;
      ByteBuffer b = ByteBuffer.allocateDirect(10);
      while((bytesRead = is.read(b)) > 0) {
        remainingFileSize -= bytesRead;
        checkStateDuringReads(fileSize, remainingFileSize, is);
        b.compact();
      }
      checkStateAfterReadCompletion(fileSize, remainingFileSize, is);
    }
  }

  @Test
  public void testNIOReadWithHeapByteBuffer() throws Exception {
    FileRef fileRef = FileRefTestUtil.getLocalFileRef(testDir, true, null, null);
    long fileSize = Files.size(Paths.get(FileRefTestUtil.getSourceFilePath(testDir)));
    long remainingFileSize = fileSize;
    try (ReadableByteChannel is = fileRef.createInputStream(context, ReadableByteChannel.class)) {
      Assert.assertEquals(remainingFileSize, getRemainingBytes(is));
      int bytesRead;
      ByteBuffer b = ByteBuffer.allocate(10);
      while((bytesRead = is.read(b)) > 0) {
        remainingFileSize -= bytesRead;
        checkStateDuringReads(fileSize, remainingFileSize, is);
        b.compact();
      }
      checkStateAfterReadCompletion(fileSize, remainingFileSize, is);
    }
  }

  @Test
  public void testCompletedFileCountForMultipleClose() throws Exception {
    InputStream is = FileRefTestUtil.getLocalFileRef(testDir, true, null, null).createInputStream(context, InputStream.class);
    try {
      while (is.read() != -1);
    } finally {
      is.close();
      is.close();
    }
    Map<String, Object> gaugeMap = (Map<String, Object>)(context.getGauge(FileRefUtil.fileStatisticGaugeName(context)).getValue());
    long completedCount = (long)gaugeMap.get(FileRefUtil.COMPLETED_FILE_COUNT);
    Assert.assertEquals(1, completedCount);
  }

  @Test
  public void testConvertBytesToDisplayFormat() throws Exception {
    //Bytes
    Assert.assertEquals("1023 B", MetricEnabledWrapperStream.convertBytesToDisplayFormat(1023d));
    Assert.assertEquals("512 B", MetricEnabledWrapperStream.convertBytesToDisplayFormat(512d));
    Assert.assertEquals("0 B", MetricEnabledWrapperStream.convertBytesToDisplayFormat(0d));

    //KB
    Assert.assertEquals("1 KB", MetricEnabledWrapperStream.convertBytesToDisplayFormat(1025d));
    Assert.assertEquals("1.5 KB", MetricEnabledWrapperStream.convertBytesToDisplayFormat(1540d));
    Assert.assertEquals("1.55 KB", MetricEnabledWrapperStream.convertBytesToDisplayFormat(1588d));
    Assert.assertEquals("1023.99 KB", MetricEnabledWrapperStream.convertBytesToDisplayFormat(1048570d));

    //MB
    Assert.assertEquals("1 MB", MetricEnabledWrapperStream.convertBytesToDisplayFormat(1048576d));
    Assert.assertEquals("100.98 MB", MetricEnabledWrapperStream.convertBytesToDisplayFormat(105885205d));
    Assert.assertEquals("1024 MB", MetricEnabledWrapperStream.convertBytesToDisplayFormat(1073741810d));

    //GB
    Assert.assertEquals("1 GB", MetricEnabledWrapperStream.convertBytesToDisplayFormat(1073741824d));
    Assert.assertEquals("1024 GB", MetricEnabledWrapperStream.convertBytesToDisplayFormat(1099511627774d));

    //TB
    Assert.assertEquals("1 TB", MetricEnabledWrapperStream.convertBytesToDisplayFormat(1099511627776d));
    Assert.assertEquals("1025 TB", MetricEnabledWrapperStream.convertBytesToDisplayFormat(1126999418470400d));
  }
}
