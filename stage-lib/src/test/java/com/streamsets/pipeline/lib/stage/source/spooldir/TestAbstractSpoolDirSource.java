/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.lib.stage.source.spooldir;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.dirspooler.DirectorySpooler;
import com.streamsets.pipeline.runner.StageContext;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.nio.file.Files;
import java.util.UUID;

public class TestAbstractSpoolDirSource {

  private String createTestDir() {
    File f = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(f.mkdirs());
    return f.getAbsolutePath();
  }

  private Source.Context getMockContext() {
    Source.Context context = Mockito.mock(StageContext.class);
    Mockito.when(context.getMetrics()).thenReturn(new MetricRegistry());
    Mockito.when(context.createMeter(Mockito.anyString())).thenCallRealMethod();
    return context;
  }

  public static class TSpoolDirSource extends AbstractSpoolDirSource {
    File file;
    long offset;
    int maxBatchSize;
    long offsetIncrement;
    boolean produceCalled;

    @Override
    protected long produce(File file, long offset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      produceCalled = true;
      Assert.assertEquals(this.file, file);
      Assert.assertEquals(this.offset, offset);
      Assert.assertEquals(this.maxBatchSize, maxBatchSize);
      Assert.assertNotNull(batchMaker);
      return offset + offsetIncrement;
    }
  }

  private TSpoolDirSource createSource(String initialFile) {
    TSpoolDirSource source = new TSpoolDirSource();
    source.postProcessing = DirectorySpooler.FilePostProcessing.ARCHIVE.toString();
    source.filePattern = "file-[0-9].log";
    source.maxSpoolFiles = 10;
    source.spoolDir = createTestDir();
    source.archiveDir = createTestDir();
    source.retentionTimeMins = 10;
    source.initialFileToProcess = initialFile;
    source.poolingTimeoutSecs = 0;
    return source;
  }

  @Test
  public void testInitDestroy() throws Exception {
    Stage.Info info = Mockito.mock(Stage.Info.class);
    Source.Context context = getMockContext();
    AbstractSpoolDirSource source = createSource("file-0.log");
    source.init(info, context);
    Assert.assertTrue(source.getSpooler().isRunning());
    Assert.assertEquals(context, source.getSpooler().getContext());
    Assert.assertEquals(source.postProcessing, source.getSpooler().getPostProcessing().toString());
    Assert.assertEquals(source.filePattern, source.getSpooler().getFilePattern());
    Assert.assertEquals(source.maxSpoolFiles, source.getSpooler().getMaxSpoolFiles());
    Assert.assertEquals(source.spoolDir, source.getSpooler().getSpoolDir());
    Assert.assertEquals(source.archiveDir, source.getSpooler().getArchiveDir());
    Assert.assertEquals(source.retentionTimeMins * 60 * 1000, source.getSpooler().getArchiveRetentionMillis());
    Assert.assertEquals(source.initialFileToProcess, source.getSpooler().getCurrentFile());
    source.destroy();
    Assert.assertFalse(source.getSpooler().isRunning());
  }

  @Test
  public void getOffsetMethods() throws Exception {
    AbstractSpoolDirSource source = createSource(null);
    Assert.assertNull(source.getFileFromSourceOffset(null));
    Assert.assertEquals("x", source.getFileFromSourceOffset("x"));
    Assert.assertEquals(0, source.getOffsetFromSourceOffset(null));
    Assert.assertEquals(0, source.getOffsetFromSourceOffset("x"));
    Assert.assertEquals("x", source.getFileFromSourceOffset(source.createSourceOffset("x", 1)));
    Assert.assertEquals(1, source.getOffsetFromSourceOffset(source.createSourceOffset("x", 1)));
  }

  @Test
  public void testProduceNoInitialFileNoFileInSpoolDirNullOffset() throws Exception {
    Stage.Info info = Mockito.mock(Stage.Info.class);
    TSpoolDirSource source = createSource(null);
    try {
      source.init(info, getMockContext());
      String offset = source.produce(null, 10, Mockito.mock(BatchMaker.class));
      Assert.assertEquals(null, offset);
      Assert.assertFalse(source.produceCalled);
    } finally {
      source.destroy();
    }
  }

  @Test
  public void testProduceNoInitialFileWithFileInSpoolDirNullOffset() throws Exception {
    Stage.Info info = Mockito.mock(Stage.Info.class);
    TSpoolDirSource source = createSource(null);
    File file = new File(source.spoolDir, "file-0.log").getAbsoluteFile();
    Files.createFile(file.toPath());
    try {
      source.init(info, getMockContext());
      source.file = file;
      source.offset = 0;
      source.maxBatchSize = 10;
      String offset = source.produce(null, 10, Mockito.mock(BatchMaker.class));
      Assert.assertEquals(source.createSourceOffset("file-0.log", 0), offset);
      Assert.assertTrue(source.produceCalled);
    } finally {
      source.destroy();
    }
  }

  @Test
  public void testProduceNoInitialFileNoFileInSpoolDirNotNullOffset() throws Exception {
    Stage.Info info = Mockito.mock(Stage.Info.class);
    TSpoolDirSource source = createSource(null);
    try {
      source.init(info, getMockContext());
      String offset = source.produce("file-0.log", 10, Mockito.mock(BatchMaker.class));
      Assert.assertEquals(source.createSourceOffset("file-0.log", 0), offset);
      Assert.assertFalse(source.produceCalled);
    } finally {
      source.destroy();
    }
  }

  @Test
  public void testProduceNoInitialFileWithFileInSpoolDirNotNullOffset() throws Exception {
    Stage.Info info = Mockito.mock(Stage.Info.class);
    TSpoolDirSource source = createSource(null);
    File file = new File(source.spoolDir, "file-0.log").getAbsoluteFile();
    Files.createFile(file.toPath());
    try {
      source.init(info, getMockContext());
      source.file = file;
      source.offset = 0;
      source.maxBatchSize = 10;
      String offset = source.produce("file-0.log", 10, Mockito.mock(BatchMaker.class));
      Assert.assertEquals(source.createSourceOffset("file-0.log", 0), offset);
      Assert.assertTrue(source.produceCalled);
    } finally {
      source.destroy();
    }
  }

  @Test
  public void testProduceNoInitialFileWithFileInSpoolDirNonZeroOffset() throws Exception {
    Stage.Info info = Mockito.mock(Stage.Info.class);
    TSpoolDirSource source = createSource(null);
    File file = new File(source.spoolDir, "file-0.log").getAbsoluteFile();
    Files.createFile(file.toPath());
    try {
      source.init(info, getMockContext());
      source.file = file;
      source.offset = 1;
      source.maxBatchSize = 10;
      String offset = source.produce(source.createSourceOffset("file-0.log", 1), 10, Mockito.mock(BatchMaker.class));
      Assert.assertTrue(source.produceCalled);
      Assert.assertEquals(source.createSourceOffset("file-0.log", 1), offset);

      source.produceCalled = false;
      source.offsetIncrement = 1;
      offset = source.produce(offset, 10, Mockito.mock(BatchMaker.class));
      Assert.assertEquals(source.createSourceOffset("file-0.log", 2), offset);
      Assert.assertTrue(source.produceCalled);
    } finally {
      source.destroy();
    }
  }

  @Test
  public void testAdvanceToNextSpoolFile() throws Exception {
    Stage.Info info = Mockito.mock(Stage.Info.class);
    TSpoolDirSource source = createSource(null);
    File file1 = new File(source.spoolDir, "file-0.log").getAbsoluteFile();
    File file2 = new File(source.spoolDir, "file-1.log").getAbsoluteFile();
    Files.createFile(file1.toPath());
    Files.createFile(file2.toPath());
    try {
      source.init(info, getMockContext());
      source.file = file1;
      source.offset = 0;
      source.maxBatchSize = 10;
      String offset = source.produce(source.createSourceOffset("file-0.log", 0), 10, Mockito.mock(BatchMaker.class));
      Assert.assertTrue(source.produceCalled);
      Assert.assertEquals(source.createSourceOffset("file-0.log", 0), offset);

      source.produceCalled = false;
      source.offsetIncrement = -1;
      offset = source.produce(offset, 10, Mockito.mock(BatchMaker.class));
      Assert.assertEquals(source.createSourceOffset("file-0.log", -1), offset);
      Assert.assertTrue(source.produceCalled);

      source.produceCalled = false;
      source.file = file2;
      source.offset = 0;
      source.offsetIncrement = 0;
      offset = source.produce(offset, 10, Mockito.mock(BatchMaker.class));
      Assert.assertEquals(source.createSourceOffset("file-1.log", 0), offset);
      Assert.assertTrue(source.produceCalled);

    } finally {
      source.destroy();
    }
  }

}
