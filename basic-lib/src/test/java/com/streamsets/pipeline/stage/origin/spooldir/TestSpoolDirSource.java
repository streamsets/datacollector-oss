/**
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

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.Compression;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.config.OnParseError;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.lib.dirspooler.PathMatcherMode;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;

public class TestSpoolDirSource {
  /*
   * Don't use the constant defined in SpoolDirSource in order to regression test the source.
   */
  private static final String NULL_FILE_OFFSET = "NULL_FILE_ID-48496481-5dc5-46ce-9c31-3ab3e034730c::0";

  private String createTestDir() {
    File f = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(f.mkdirs());
    return f.getAbsolutePath();
  }

  public static class TSpoolDirSource extends SpoolDirSource {
    File file;
    long offset;
    int maxBatchSize;
    long offsetIncrement;
    boolean produceCalled;
    String spoolDir;

    public TSpoolDirSource(SpoolDirConfigBean conf) {
      super(conf);
      this.spoolDir = conf.spoolDir;
    }

    @Override
    public String produce(File file, String offset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      long longOffset = Long.parseLong(offset);
      produceCalled = true;
      Assert.assertEquals(this.file, file);
      Assert.assertEquals(this.offset, longOffset);
      Assert.assertEquals(this.maxBatchSize, maxBatchSize);
      Assert.assertNotNull(batchMaker);
      return String.valueOf(longOffset + offsetIncrement);
    }
  }

  private TSpoolDirSource createSource(String initialFile) {
    SpoolDirConfigBean conf = new SpoolDirConfigBean();
    conf.dataFormat = DataFormat.TEXT;
    // add trailing slash to ensure that works properly
    conf.spoolDir = createTestDir() + "/";
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

    return new TSpoolDirSource(conf);
  }

  @Test
  public void testInitDestroy() throws Exception {
    TSpoolDirSource source = createSource("file-0.log");
    SourceRunner runner = new SourceRunner.Builder(TSpoolDirSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      Assert.assertTrue(source.getSpooler().isRunning());
      Assert.assertEquals(runner.getContext(), source.getSpooler().getContext());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void getOffsetMethods() throws Exception {
    TSpoolDirSource source = createSource("file-0.log");
    Assert.assertNull(source.getFileFromSourceOffset(null));
    Assert.assertEquals("x", source.getFileFromSourceOffset("x"));
    Assert.assertEquals("0", source.getOffsetFromSourceOffset(null));
    Assert.assertEquals("0", source.getOffsetFromSourceOffset("x"));
    Assert.assertEquals("x", source.getFileFromSourceOffset(source.createSourceOffset("x", "1")));
    Assert.assertEquals("1", source.getOffsetFromSourceOffset(source.createSourceOffset("x", "1")));
  }
  @Test
  public void testOffsetMethods() throws Exception {
    TSpoolDirSource source = createSource(null);
    Assert.assertEquals(NULL_FILE_OFFSET, source.createSourceOffset(null, "0"));
    Assert.assertEquals("file1::0", source.createSourceOffset("file1", "0"));
    Assert.assertEquals("0", source.getOffsetFromSourceOffset(NULL_FILE_OFFSET));
    Assert.assertNull(source.getFileFromSourceOffset(NULL_FILE_OFFSET));
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
    SourceRunner runner = new SourceRunner.Builder(TSpoolDirSource.class, source).addOutputLane("lane").build();
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
    runner = new SourceRunner.Builder(TSpoolDirSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      StageRunner.Output output = runner.runProduce(null, 10);
      Assert.assertEquals(NULL_FILE_OFFSET, output.getNewOffset());
      Assert.assertEquals(0, runner.getEventRecords().size());

      Assert.assertTrue(f.mkdirs());

      File file = new File(source.spoolDir, "file-0.log").getAbsoluteFile();
      Files.createFile(file.toPath());

      source.file = file;
      source.offset = 1;
      source.maxBatchSize = 10;

      Thread.sleep(1000);

      output = runner.runProduce(source.createSourceOffset("file-0.log", "1"), 10);
      Assert.assertEquals(source.createSourceOffset("file-0.log", "1"), output.getNewOffset());
      Assert.assertEquals(1, runner.getEventRecords().size());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testProduceNoInitialFileNoFileInSpoolDirNullOffset() throws Exception {
    TSpoolDirSource source = createSource(null);
    SourceRunner runner = new SourceRunner.Builder(TSpoolDirSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      StageRunner.Output output = runner.runProduce(null, 10);
      Assert.assertEquals(NULL_FILE_OFFSET, output.getNewOffset());
      Assert.assertFalse(source.produceCalled);
      Assert.assertEquals(0, runner.getEventRecords().size());
    } finally {
      runner.runDestroy();
    }
  }


  @Test
  public void testProduceNoInitialFileWithFileInSpoolDirNullOffset() throws Exception {
    TSpoolDirSource source = createSource("file-0.log");
    SourceRunner runner = new SourceRunner.Builder(TSpoolDirSource.class, source).addOutputLane("lane").build();
    File file = new File(source.spoolDir, "file-0.log").getAbsoluteFile();
    Files.createFile(file.toPath());
    runner.runInit();
    source.file = file;
    source.offset = 0;
    source.maxBatchSize = 10;
    try {
      StageRunner.Output output = runner.runProduce(null, 10);
      Assert.assertEquals(source.createSourceOffset("file-0.log", "0"), output.getNewOffset());
      Assert.assertTrue(source.produceCalled);
    } finally {
      runner.runDestroy();
    }
  }


  @Test
  public void testProduceNoInitialFileNoFileInSpoolDirNotNullOffset() throws Exception {
    TSpoolDirSource source = createSource(null);
    SourceRunner runner = new SourceRunner.Builder(TSpoolDirSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      StageRunner.Output output = runner.runProduce("file-0.log", 10);
      Assert.assertEquals(source.createSourceOffset("file-0.log", "0"), output.getNewOffset());
      Assert.assertFalse(source.produceCalled);
    } finally {
      runner.runDestroy();
    }
  }


  @Test
  public void testProduceNoInitialFileWithFileInSpoolDirNotNullOffset() throws Exception {
    TSpoolDirSource source = createSource("file-0.log");
    SourceRunner runner = new SourceRunner.Builder(TSpoolDirSource.class, source).addOutputLane("lane").build();
    File file = new File(source.spoolDir, "file-0.log").getAbsoluteFile();
    Files.createFile(file.toPath());
    runner.runInit();
    source.file = file;
    source.offset = 0;
    source.maxBatchSize = 10;
    try {
      StageRunner.Output output = runner.runProduce("file-0.log", 10);
      Assert.assertEquals(source.createSourceOffset("file-0.log", "0"), output.getNewOffset());
      Assert.assertTrue(source.produceCalled);
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testProduceNoInitialFileWithFileInSpoolDirNonZeroOffset() throws Exception {
    TSpoolDirSource source = createSource(null);
    SourceRunner runner = new SourceRunner.Builder(TSpoolDirSource.class, source).addOutputLane("lane").build();
    File file = new File(source.spoolDir, "file-0.log").getAbsoluteFile();
    Files.createFile(file.toPath());
    runner.runInit();
    source.file = file;
    source.offset = 1;
    source.maxBatchSize = 10;
    try {
      StageRunner.Output output = runner.runProduce(source.createSourceOffset("file-0.log", "1"), 10);
      Assert.assertEquals(source.createSourceOffset("file-0.log", "1"), output.getNewOffset());
      Assert.assertTrue(source.produceCalled);
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testNoMoreFilesEmptyBatch() throws Exception {
    TSpoolDirSource source = createSource(null);
    SourceRunner runner = new SourceRunner.Builder(TSpoolDirSource.class, source).addOutputLane("lane").build();
    File file = new File(source.spoolDir, "file-0.log").getAbsoluteFile();
    Files.createFile(file.toPath());
    runner.runInit();

    try {
      source.file = file;
      source.offset = 0;
      source.maxBatchSize = 10;
      source.offsetIncrement = -1;

      StageRunner.Output output = runner.runProduce(null, 1000);
      Assert.assertEquals(source.createSourceOffset("file-0.log", "-1"), output.getNewOffset());
      Assert.assertTrue(source.produceCalled);

      source.produceCalled = false;
      output = runner.runProduce(output.getNewOffset(), 1000);
      Assert.assertEquals(source.createSourceOffset("file-0.log", "-1"), output.getNewOffset());
      //Produce will not be called as this file-0.log will not be eligible for produce
      Assert.assertFalse(source.produceCalled);
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testAdvanceToNextSpoolFile() throws Exception {
    TSpoolDirSource source = createSource(null);
    SourceRunner runner = new SourceRunner.Builder(TSpoolDirSource.class, source).addOutputLane("lane").build();
    File file1 = new File(source.spoolDir, "file-0.log").getAbsoluteFile();
    Files.createFile(file1.toPath());
    File file2 = new File(source.spoolDir, "file-1.log").getAbsoluteFile();
    Files.createFile(file2.toPath());
    runner.runInit();
    source.file = file1;
    source.offset = 0;
    source.maxBatchSize = 10;
    try {
      StageRunner.Output output = runner.runProduce(source.createSourceOffset("file-0.log", "0"), 10);
      Assert.assertEquals(source.createSourceOffset("file-0.log", "0"), output.getNewOffset());
      Assert.assertTrue(source.produceCalled);
      Assert.assertEquals(1, runner.getEventRecords().size());

      source.produceCalled = false;
      source.offsetIncrement = -1;
      output = runner.runProduce(output.getNewOffset(), 10);
      Assert.assertEquals(source.createSourceOffset("file-0.log", "-1"), output.getNewOffset());
      Assert.assertTrue(source.produceCalled);
      Assert.assertEquals(2, runner.getEventRecords().size());

      source.file = file2;
      output = runner.runProduce(output.getNewOffset(), 10);
      source.produceCalled = false;
      source.offset = 0;
      source.offsetIncrement = 0;
      output = runner.runProduce(output.getNewOffset(), 10);
      Assert.assertEquals(source.createSourceOffset("file-1.log", "-1"), output.getNewOffset());
      Assert.assertFalse(source.produceCalled);
      Assert.assertEquals(4, runner.getEventRecords().size());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testRecoverableDataParserError() throws Exception {
    File f = new File("target", UUID.randomUUID().toString());
    f.mkdir();

    SpoolDirConfigBean conf = new SpoolDirConfigBean();
    conf.dataFormat = DataFormat.DELIMITED;
    conf.spoolDir = f.getAbsolutePath();
    conf.batchSize = 10;
    conf.overrunLimit = 100;
    conf.poolingTimeoutSecs = 1;
    conf.filePattern = "file-[0-9].log";
    conf.pathMatcherMode = PathMatcherMode.GLOB;
    conf.maxSpoolFiles = 10;
    conf.initialFileToProcess = "file-0.log";
    conf.dataFormatConfig.compression = Compression.NONE;
    conf.dataFormatConfig.filePatternInArchive = "*";
    conf.dataFormatConfig.csvHeader = CsvHeader.WITH_HEADER;
    conf.errorArchiveDir = null;
    conf.postProcessing = PostProcessingOptions.NONE;
    conf.retentionTimeMins = 10;
    conf.allowLateDirectory = false;
    conf.dataFormatConfig.textMaxLineLen = 10;
    conf.dataFormatConfig.onParseError = OnParseError.ERROR;
    conf.dataFormatConfig.maxStackTraceLines = 0;

    FileOutputStream outputStream = new FileOutputStream(new File(conf.spoolDir, "file-0.log"));
    IOUtils.writeLines(ImmutableList.of("A,B", "a,b,c", "a,b"), "\n", outputStream);
    outputStream.close();

    SpoolDirSource source = new SpoolDirSource(conf);
    SourceRunner runner = new SourceRunner.Builder(SpoolDirSource.class, source)
      .setOnRecordError(OnRecordError.TO_ERROR)
      .addOutputLane("lane")
      .build();
    runner.runInit();
    try {

      StageRunner.Output output = runner.runProduce(null, 10);
      Assert.assertNotNull(output);

      // Verify proper record
      List<Record> records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(1, records.size());

      // And error record
      records = runner.getErrorRecords();
      Assert.assertEquals(1, records.size());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testMultipleFilesSameTimeStamp() throws Exception {
    File f = new File("target", UUID.randomUUID().toString());
    f.mkdir();

    SpoolDirConfigBean conf = new SpoolDirConfigBean();
    conf.dataFormat = DataFormat.DELIMITED;
    conf.useLastModified = FileOrdering.TIMESTAMP;
    conf.spoolDir = f.getAbsolutePath();
    conf.batchSize = 10;
    conf.overrunLimit = 100;
    conf.poolingTimeoutSecs = 1;
    conf.filePattern = "*";
    conf.pathMatcherMode = PathMatcherMode.GLOB;
    conf.maxSpoolFiles = 10;
    conf.dataFormatConfig.compression = Compression.NONE;
    conf.dataFormatConfig.filePatternInArchive = "*";
    conf.dataFormatConfig.csvHeader = CsvHeader.WITH_HEADER;
    conf.errorArchiveDir = null;
    conf.postProcessing = PostProcessingOptions.NONE;
    conf.retentionTimeMins = 10;
    conf.allowLateDirectory = false;
    conf.dataFormatConfig.textMaxLineLen = 10;
    conf.dataFormatConfig.onParseError = OnParseError.ERROR;
    conf.dataFormatConfig.maxStackTraceLines = 0;
    long timestamp = System.currentTimeMillis() - 100000;

    for (int i = 0; i < 8; i++) {
      File current = new File(conf.spoolDir, Utils.format("file-{}.log", i));
      try(FileOutputStream outputStream = new FileOutputStream(current)) {
        IOUtils.writeLines(ImmutableList.of("A,B", Utils.format("a-{},b-{}", i, i), "a,b"), "\n", outputStream);
      }
      Assert.assertTrue(current.setLastModified(timestamp));
    }

    File current = new File(conf.spoolDir,"a.log");
    try(FileOutputStream outputStream = new FileOutputStream(current)) {
      IOUtils.writeLines(ImmutableList.of("A,B", "Gollum,Sauron", "Aragorn,Boromir"), "\n", outputStream);
    }
    Assert.assertTrue(current.setLastModified(System.currentTimeMillis()));

    SpoolDirSource source = new SpoolDirSource(conf);
    SourceRunner runner = new SourceRunner.Builder(SpoolDirSource.class, source)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    String lastOffset = null;
    try {
      for (int i = 0; i < 8; i++) {
        StageRunner.Output output = runner.runProduce(lastOffset, 10);
        lastOffset = output.getNewOffset();
        List<Record> records = output.getRecords().get("lane");
        Assert.assertNotNull(records);
        Assert.assertTrue(!records.isEmpty());
        Assert.assertEquals(2, records.size());

        Assert.assertEquals(Utils.format("file-{}.log", i), records.get(0).getHeader().getAttribute(HeaderAttributeConstants.FILE_NAME));
        Assert.assertEquals(
          String.valueOf(Files.getLastModifiedTime(Paths.get(f.getAbsolutePath(), Utils.format("file-{}.log", i))).toMillis()),
          records.get(0).getHeader().getAttribute(HeaderAttributeConstants.LAST_MODIFIED_TIME)
        );
        Assert.assertEquals("a-" + i, records.get(0).get("/A").getValueAsString());
        Assert.assertEquals("b-" + i, records.get(0).get("/B").getValueAsString());

        Assert.assertEquals("a", records.get(1).get("/A").getValueAsString());
        Assert.assertEquals("b", records.get(1).get("/B").getValueAsString());

        // And error record
        records = runner.getErrorRecords();
        Assert.assertEquals(0, records.size());
      }
      StageRunner.Output output = runner.runProduce(lastOffset, 10);
      lastOffset = output.getNewOffset();
      List<Record> records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertTrue(!records.isEmpty());
      Assert.assertEquals(2, records.size());

      Assert.assertEquals("a.log", records.get(0).getHeader().getAttribute(HeaderAttributeConstants.FILE_NAME));
      Assert.assertEquals("Gollum", records.get(0).get("/A").getValueAsString());
      Assert.assertEquals("Sauron", records.get(0).get("/B").getValueAsString());

      Assert.assertEquals("Aragorn", records.get(1).get("/A").getValueAsString());
      Assert.assertEquals("Boromir", records.get(1).get("/B").getValueAsString());

      // And error record
      records = runner.getErrorRecords();
      Assert.assertEquals(0, records.size());

      Assert.assertTrue(runner.runProduce(lastOffset, 10).getRecords().get("lane").isEmpty());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testErrorFileWithoutPreview() throws Exception {
    errorFile(false);
  }

  @Test
  public void testErrorFileInPreview() throws Exception {
    errorFile(true);
  }

  public void errorFile(boolean preview) throws Exception {
    File spoolDir = new File("target", UUID.randomUUID().toString());
    spoolDir.mkdir();
    File errorDir = new File("target", UUID.randomUUID().toString());
    errorDir.mkdir();

    SpoolDirConfigBean conf = new SpoolDirConfigBean();
    conf.dataFormat = DataFormat.JSON;
    conf.spoolDir = spoolDir.getAbsolutePath();
    conf.batchSize = 10;
    conf.overrunLimit = 100;
    conf.poolingTimeoutSecs = 1;
    conf.filePattern = "file-[0-9].log";
    conf.pathMatcherMode = PathMatcherMode.GLOB;
    conf.maxSpoolFiles = 10;
    conf.initialFileToProcess = "file-0.log";
    conf.dataFormatConfig.compression = Compression.NONE;
    conf.dataFormatConfig.filePatternInArchive = "*";
    conf.dataFormatConfig.csvHeader = CsvHeader.WITH_HEADER;
    conf.errorArchiveDir = errorDir.getAbsolutePath();
    conf.postProcessing = PostProcessingOptions.NONE;
    conf.retentionTimeMins = 10;
    conf.allowLateDirectory = false;
    conf.dataFormatConfig.jsonContent = JsonMode.MULTIPLE_OBJECTS;
    conf.dataFormatConfig.onParseError = OnParseError.ERROR;

    FileOutputStream outputStream = new FileOutputStream(new File(conf.spoolDir, "file-0.log"));
    // Incorrect JSON
    IOUtils.writeLines(ImmutableList.of("{a"), "\n", outputStream);
    outputStream.close();

    SpoolDirSource source = new SpoolDirSource(conf);
    SourceRunner runner = new SourceRunner.Builder(SpoolDirSource.class, source)
      .setPreview(preview)
      .setOnRecordError(OnRecordError.TO_ERROR)
      .addOutputLane("lane")
      .build();
    runner.runInit();
    try {
      StageRunner.Output output = runner.runProduce(null, 10);
      Assert.assertNotNull(output);

      // Verify proper record
      List<Record> records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(0, records.size());

      // Depending on the preview flag, we should see the file in one directory or the other
      if(preview) {
        Assert.assertEquals(0, errorDir.list().length);
        Assert.assertEquals(1, spoolDir.list().length);
      } else {
        Assert.assertEquals(1, errorDir.list().length);
        Assert.assertEquals(0, spoolDir.list().length);
      }

    } finally {
      runner.runDestroy();
    }
  }

}
