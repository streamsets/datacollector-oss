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
package com.streamsets.pipeline.stage.origin.spooldir;

import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.Compression;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.OnParseError;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.lib.dirspooler.PathMatcherMode;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class TestSpoolDirSourceSubDirectories {
  /*
   * Don't use the constant defined in SpoolDirSource in order to regression test the source.
   */
  private static final String NULL_FILE_OFFSET = "NULL_FILE_ID-48496481-5dc5-46ce-9c31-3ab3e034730c::0";

  private String createTestDir() {
    File f = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(f.mkdirs());
    return f.getAbsolutePath();
  }

  private TSpoolDirSource createSource(String initialFile) {
    SpoolDirConfigBean conf = new SpoolDirConfigBean();
    conf.dataFormat = DataFormat.TEXT;
    conf.spoolDir = createTestDir();
    conf.batchSize = 10;
    conf.overrunLimit = 100;
    conf.poolingTimeoutSecs = 1;
    conf.filePattern = "file-[0-9].log";
    conf.pathMatcherMode = PathMatcherMode.GLOB;
    conf.maxSpoolFiles = 10;
    conf.initialFileToProcess = initialFile;
    conf.dataFormatConfig.compression = Compression.NONE;
    conf.dataFormatConfig.filePatternInArchive = "*";
    conf.errorArchiveDir = null;
    conf.postProcessing = PostProcessingOptions.ARCHIVE;
    conf.archiveDir = createTestDir();
    conf.retentionTimeMins = 10;
    conf.dataFormatConfig.textMaxLineLen = 10;
    conf.dataFormatConfig.onParseError = OnParseError.ERROR;
    conf.dataFormatConfig.maxStackTraceLines = 0;
    conf.processSubdirectories = true;
    conf.useLastModified = FileOrdering.TIMESTAMP;
    return new TSpoolDirSource(conf);
  }

  @Test
  public void testInitDestroy() throws Exception {
    TSpoolDirSource source = createSource("/dir1/file-0.log");
    PushSourceRunner runner = new PushSourceRunner.Builder(TSpoolDirSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      Assert.assertTrue(source.getSpooler().isRunning());
      Assert.assertEquals(runner.getContext(), source.getSpooler().getContext());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testAllowLateDirectory() throws Exception {
    File f = new File("target", UUID.randomUUID().toString());

    SpoolDirConfigBean conf = new SpoolDirConfigBean();
    conf.dataFormat = DataFormat.TEXT;
    conf.spoolDir = f.getAbsolutePath();
    conf.batchSize = 10;
    conf.overrunLimit = 100;
    conf.poolingTimeoutSecs = 1;
    conf.filePattern = "file-[0-9].log";
    conf.pathMatcherMode = PathMatcherMode.GLOB;
    conf.maxSpoolFiles = 10;
    conf.initialFileToProcess = null;
    conf.dataFormatConfig.compression = Compression.NONE;
    conf.dataFormatConfig.filePatternInArchive = "*";
    conf.errorArchiveDir = null;
    conf.postProcessing = PostProcessingOptions.ARCHIVE;
    conf.archiveDir = createTestDir();
    conf.retentionTimeMins = 10;
    conf.dataFormatConfig.textMaxLineLen = 10;
    conf.dataFormatConfig.onParseError = OnParseError.ERROR;
    conf.dataFormatConfig.maxStackTraceLines = 0;

    TSpoolDirSource source = new TSpoolDirSource(conf);
    PushSourceRunner runner = new PushSourceRunner.Builder(TSpoolDirSource.class, source).addOutputLane("lane").build();
    //Late Directories not allowed, init should fail.
    conf.allowLateDirectory = false;
    try {
      runner.runInit();
      Assert.fail("Should throw an exception if the directory does not exist");
    } catch (StageException e) {
      //Expected
    }

    //Late Directories allowed, wait and should be able to detect the file and read.
    conf.allowLateDirectory = true;
    source = new TSpoolDirSource(conf);

    List<Record> records = Collections.synchronizedList(new ArrayList<>(10));
    AtomicInteger batchCount = new AtomicInteger(0);

    runner.runInit();
    try {
      runner.runProduce(new HashMap<>(), 10, output -> {
        synchronized (records) {
          records.addAll(output.getRecords().get("lane"));
        }
        batchCount.incrementAndGet();
        runner.setStop();
      });
      runner.waitOnProduce();

      TestOffsetUtil.compare(NULL_FILE_OFFSET, runner.getOffsets());

      Assert.assertTrue(f.mkdirs());

      File file = new File(source.spoolDir, "/dir1/file-0.log").getAbsoluteFile();
      Assert.assertTrue(file.getParentFile().mkdirs());
      Files.createFile(file.toPath());

      source.file = file;
      source.offset = 1;
      source.maxBatchSize = 10;

      Thread.sleep(1000);

      records.clear();

      PushSourceRunner runner2 = new PushSourceRunner.Builder(TSpoolDirSource.class, source).addOutputLane("lane").build();

      String offset = "/dir1/file-0.log::1";
      runner2.runInit();
      runner2.runProduce(ImmutableMap.of(Source.POLL_SOURCE_OFFSET_KEY, offset), 10, output -> {
        synchronized (records) {
          records.addAll(output.getRecords().get("lane"));
        }
        batchCount.incrementAndGet();
        runner2.setStop();
      });
      runner2.waitOnProduce();

      TestOffsetUtil.compare(offset, runner2.getOffsets());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testProduceNoInitialFileWithFileInSpoolDirNullOffset() throws Exception {
    TSpoolDirSource source = createSource("/dir1/file-0.log");
    PushSourceRunner runner = new PushSourceRunner.Builder(TSpoolDirSource.class, source).addOutputLane("lane").build();
    File file = new File(source.spoolDir, "/dir1/file-0.log").getAbsoluteFile();
    Assert.assertTrue(file.getParentFile().mkdirs());
    Files.createFile(file.toPath());

    runner.runInit();
    source.file = file;
    source.offset = 0;
    source.maxBatchSize = 10;
    try {
      runner.runProduce(new HashMap<>(), 10, output -> {
        runner.setStop();
      });
      runner.waitOnProduce();

      TestOffsetUtil.compare("dir1/file-0.log::0", runner.getOffsets());
      //Assert.assertTrue(source.produceCalled);
    } finally {
      runner.runDestroy();
    }
  }


  @Test
  public void testProduceNoInitialFileNoFileInSpoolDirNotNullOffset() throws Exception {
    TSpoolDirSource source = createSource(null);
    PushSourceRunner runner = new PushSourceRunner.Builder(TSpoolDirSource.class, source).addOutputLane("lane").build();

    runner.runInit();
    try {
      String offset = "/dir1/file-0.log::0";
      runner.runProduce(ImmutableMap.of(Source.POLL_SOURCE_OFFSET_KEY, offset), 10, output -> {
        runner.setStop();
      });
      runner.waitOnProduce();

      TestOffsetUtil.compare(offset, runner.getOffsets());
      //Assert.assertFalse(source.produceCalled);
    } finally {
      runner.runDestroy();
    }
  }


  @Test
  public void testProduceNoInitialFileWithFileInSpoolDirNotNullOffset() throws Exception {
    TSpoolDirSource source = createSource("/dir1/file-0.log");
    PushSourceRunner runner = new PushSourceRunner.Builder(TSpoolDirSource.class, source).addOutputLane("lane").build();
    File file = new File(source.spoolDir, "/dir1/file-0.log").getAbsoluteFile();
    Assert.assertTrue(file.getParentFile().mkdirs());
    Files.createFile(file.toPath());
    List<Record> records = Collections.synchronizedList(new ArrayList<>(10));
    AtomicInteger batchCount = new AtomicInteger(0);

    runner.runInit();
    source.file = file;
    source.offset = 0;
    source.maxBatchSize = 10;
    try {
      String offset = "/dir1/file-1.log::0";
      runner.runProduce(ImmutableMap.of(Source.POLL_SOURCE_OFFSET_KEY, offset), 10, output -> {
        synchronized (records) {
          records.addAll(output.getRecords().get("lane"));
        }
        batchCount.incrementAndGet();
        runner.setStop();
      });
      runner.waitOnProduce();

      TestOffsetUtil.compare("dir1/file-0.log::0", runner.getOffsets());
      //Assert.assertTrue(source.produceCalled);
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testProduceNoInitialFileWithFileInSpoolDirNonZeroOffset() throws Exception {
    TSpoolDirSource source = createSource(null);
    PushSourceRunner runner = new PushSourceRunner.Builder(TSpoolDirSource.class, source).addOutputLane("lane").build();
    File file = new File(source.spoolDir, "/dir1/file-0.log").getAbsoluteFile();
    Assert.assertTrue(file.getParentFile().mkdirs());
    Files.createFile(file.toPath());

    runner.runInit();
    source.file = file;
    source.offset = 1;
    source.maxBatchSize = 10;
    try {
      String offset = "/dir1/file-0.log::1";
      runner.runProduce(ImmutableMap.of(Source.POLL_SOURCE_OFFSET_KEY, offset), 10, output -> {
        runner.setStop();
      });
      runner.waitOnProduce();

      TestOffsetUtil.compare(offset, runner.getOffsets());
      //Assert.assertTrue(source.produceCalled);
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testNoMoreFilesEmptyBatch() throws Exception {
    TSpoolDirSource source = createSource(null);
    PushSourceRunner runner = new PushSourceRunner.Builder(TSpoolDirSource.class, source).addOutputLane("lane").build();
    File file = new File(source.spoolDir, "dir1/file-0.log").getAbsoluteFile();
    Assert.assertTrue(file.getParentFile().mkdirs());
    Files.createFile(file.toPath());
    AtomicInteger batchCount = new AtomicInteger(0);

    runner.runInit();

    try {
      source.file = file;
      source.offset = 0;
      source.maxBatchSize = 10;
      source.offsetIncrement = -1;

      runner.runProduce(new HashMap<>(), 1000, output -> {
        batchCount.incrementAndGet();

        if (batchCount.get() < 2) {
          Assert.assertEquals("dir1/file-0.log", output.getOffsetEntity());
          Assert.assertEquals("{\"POS\":\"-1\"}", output.getNewOffset());
          //Assert.assertTrue(source.produceCalled);

          source.produceCalled = false;
        } else {
          runner.setStop();
        }
      });

      runner.waitOnProduce();

      Assert.assertEquals(2, batchCount.get());
      TestOffsetUtil.compare("dir1/file-0.log::-1", runner.getOffsets());

      //Produce will not be called as this file-0.log will not be eligible for produce
      //Assert.assertFalse(source.produceCalled);
    } finally {
      runner.runDestroy();
    }
  }

}
