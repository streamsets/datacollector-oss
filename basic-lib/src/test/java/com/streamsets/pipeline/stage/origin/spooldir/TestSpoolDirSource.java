/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.stage.origin.spooldir;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.Compression;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.CsvRecordType;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.config.OnParseError;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
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

    public TSpoolDirSource(DataFormat dataFormat, String spoolDir, int batchSize, long poolingTimeoutSecs,
        String filePattern,
        int maxSpoolFiles, String initialFileToProcess, String errorArchiveDir, PostProcessingOptions postProcessing,
        String archiveDir, long retentionTimeMins, CsvMode csvFileFormat, CsvHeader csvHeaders,
        JsonMode jsonContent, int maxJsonObjectLen, int maxLogLineLength, String xmlRecordElement,
        int maxXmlObjectLen) {
      super(dataFormat, "UTF-8", false, 100, spoolDir, batchSize, poolingTimeoutSecs, filePattern, maxSpoolFiles,
        initialFileToProcess, Compression.NONE, "*",  errorArchiveDir, postProcessing, archiveDir, retentionTimeMins,
        csvFileFormat,
        csvHeaders, -1, 'A', 'A', 'A', jsonContent, maxJsonObjectLen, maxLogLineLength, xmlRecordElement,
        maxXmlObjectLen, null, 0, false, null, null, null, null, null, false, null, OnParseError.ERROR, -1, null,
        CsvRecordType.LIST);
      this.spoolDir = spoolDir;
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
    return new TSpoolDirSource(DataFormat.TEXT, createTestDir(), 10, 1, "file-[0-9].log", 10, initialFile, null,
                               PostProcessingOptions.ARCHIVE, createTestDir(), 10, null, null, null,
                               0, 1024, null, 0);
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
  public void testProduceNoInitialFileNoFileInSpoolDirNullOffset() throws Exception {
    TSpoolDirSource source = createSource(null);
    SourceRunner runner = new SourceRunner.Builder(TSpoolDirSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      StageRunner.Output output = runner.runProduce(null, 10);
      Assert.assertEquals(NULL_FILE_OFFSET, output.getNewOffset());
      Assert.assertFalse(source.produceCalled);
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

      source.produceCalled = false;
      source.offsetIncrement = -1;
      output = runner.runProduce(output.getNewOffset(), 10);
      Assert.assertEquals(source.createSourceOffset("file-0.log", "-1"), output.getNewOffset());
      Assert.assertTrue(source.produceCalled);

      source.file = file2;
      output = runner.runProduce(output.getNewOffset(), 10);
      source.produceCalled = false;
      source.offset = 0;
      source.offsetIncrement = 0;
      output = runner.runProduce(output.getNewOffset(), 10);
      Assert.assertEquals(source.createSourceOffset("file-1.log", "-1"), output.getNewOffset());
      Assert.assertFalse(source.produceCalled);
    } finally {
      runner.runDestroy();
    }
  }

}
