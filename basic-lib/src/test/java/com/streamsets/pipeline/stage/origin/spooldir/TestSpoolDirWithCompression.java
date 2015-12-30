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

import com.google.common.io.Resources;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.Compression;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.OnParseError;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class TestSpoolDirWithCompression {

  private static File testDir;

  @BeforeClass
  public static void setUpClass() throws IOException, InterruptedException, URISyntaxException, CompressorException {
    testDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(testDir.mkdirs());
    Files.copy(Paths.get(Resources.getResource("logArchive.zip").toURI()), Paths.get(testDir.getAbsolutePath(),
        "logArchive1.zip"));
    Files.copy(Paths.get(Resources.getResource("logArchive.zip").toURI()), Paths.get(testDir.getAbsolutePath(),
        "logArchive2.zip"));
    Files.copy(Paths.get(Resources.getResource("logArchive.tar.gz").toURI()), Paths.get(testDir.getAbsolutePath(),
        "logArchive1.tar.gz"));
    Files.copy(Paths.get(Resources.getResource("logArchive.tar.gz").toURI()), Paths.get(testDir.getAbsolutePath(),
        "logArchive2.tar.gz"));
    Files.copy(Paths.get(Resources.getResource("testAvro.tar.gz").toURI()), Paths.get(testDir.getAbsolutePath(),
      "testAvro1.tar.gz"));
    Files.copy(Paths.get(Resources.getResource("testAvro.tar.gz").toURI()), Paths.get(testDir.getAbsolutePath(),
      "testAvro2.tar.gz"));

    File bz2File = new File(testDir, "testFile1.bz2");
    CompressorOutputStream bzip2 = new CompressorStreamFactory()
      .createCompressorOutputStream("bzip2", new FileOutputStream(bz2File));
    bzip2.write(IOUtils.toByteArray(Resources.getResource("testLogFile.txt").openStream()));
    bzip2.close();

    bz2File = new File(testDir, "testFile2.bz2");
    bzip2 = new CompressorStreamFactory()
      .createCompressorOutputStream("bzip2", new FileOutputStream(bz2File));
    bzip2.write(IOUtils.toByteArray(Resources.getResource("testLogFile.txt").openStream()));
    bzip2.close();
  }

  @Test
  public void testProduceZipFile() throws Exception {
    SpoolDirSource source = createZipSource();
    SourceRunner runner = new SourceRunner.Builder(SpoolDirDSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      List<Record> allRecords = new ArrayList<>();
      String offset = null;
      for(int i = 0; i < 50; i++) {
        BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
        offset = source.produce(offset, 1000, batchMaker);
        Assert.assertNotNull(offset);

        StageRunner.Output output = SourceRunner.getOutput(batchMaker);
        List<Record> records = output.getRecords().get("lane");
        allRecords.addAll(records);
      }
      Assert.assertEquals(37044, allRecords.size());
      Assert.assertTrue(offset.equals("logArchive2.zip::-1"));

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testProduceTarGzipTextFile() throws Exception {
    SpoolDirSource source = createTarGzipSource();
    SourceRunner runner = new SourceRunner.Builder(SpoolDirDSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      List<Record> allRecords = new ArrayList<>();
      String offset = null;
      for(int i = 0; i < 50; i++) {
        BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
        offset = source.produce(offset, 1000, batchMaker);
        Assert.assertNotNull(offset);

        StageRunner.Output output = SourceRunner.getOutput(batchMaker);
        List<Record> records = output.getRecords().get("lane");
        allRecords.addAll(records);
      }
      Assert.assertEquals(37044, allRecords.size());
      Assert.assertTrue(offset.equals("logArchive2.tar.gz::-1"));

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testProduceTarGzipAvroFile() throws Exception {
    SpoolDirSource source = createTarGzipAvroSource();
    SourceRunner runner = new SourceRunner.Builder(SpoolDirDSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      List<Record> allRecords = new ArrayList<>();
      String offset = null;
      for(int i = 0; i < 50; i++) {
        BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
        offset = source.produce(offset, 1000, batchMaker);
        Assert.assertNotNull(offset);

        StageRunner.Output output = SourceRunner.getOutput(batchMaker);
        List<Record> records = output.getRecords().get("lane");
        allRecords.addAll(records);
      }
      Assert.assertEquals(48000, allRecords.size());
      Assert.assertTrue(offset.equals("testAvro2.tar.gz::-1"));

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testProduceBz2File() throws Exception {
    SpoolDirSource source = createBz2Source();
    SourceRunner runner = new SourceRunner.Builder(SpoolDirDSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      List<Record> allRecords = new ArrayList<>();
      String offset = null;
      for(int i = 0; i < 10; i++) {
        BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
        offset = source.produce(offset, 1000, batchMaker);
        Assert.assertNotNull(offset);

        StageRunner.Output output = SourceRunner.getOutput(batchMaker);
        List<Record> records = output.getRecords().get("lane");
        allRecords.addAll(records);
      }
      Assert.assertEquals(4, allRecords.size());
      Assert.assertTrue(offset.equals("testFile2.bz2::-1"));

    } finally {
      runner.runDestroy();
    }
  }

  private SpoolDirSource createZipSource() {
    SpoolDirConfigBean conf = new SpoolDirConfigBean();
    conf.dataFormat = DataFormat.TEXT;
    conf.spoolDir = testDir.getAbsolutePath();
    conf.batchSize = 1000;
    conf.overrunLimit = 65;
    conf.poolingTimeoutSecs = 1;
    conf.filePattern = "logArchive*.zip";
    conf.maxSpoolFiles = 10;
    conf.initialFileToProcess = null;
    conf.dataFormatConfig.compression = Compression.ARCHIVE;
    conf.dataFormatConfig.filePatternInArchive = "*/*.log";
    conf.errorArchiveDir = null;
    conf.postProcessing = PostProcessingOptions.NONE;
    conf.archiveDir = testDir.getAbsolutePath();
    conf.retentionTimeMins = 10;
    conf.dataFormatConfig.textMaxLineLen = 10;
    conf.dataFormatConfig.onParseError = OnParseError.ERROR;
    conf.dataFormatConfig.maxStackTraceLines = 0;

    return new SpoolDirSource(conf);
  }

  private SpoolDirSource createTarGzipSource() {
    SpoolDirConfigBean conf = new SpoolDirConfigBean();
    conf.dataFormat = DataFormat.TEXT;
    conf.spoolDir = testDir.getAbsolutePath();
    conf.batchSize = 1000;
    conf.overrunLimit = 65;
    conf.poolingTimeoutSecs = 1;
    conf.filePattern = "logArchive*.tar.gz";
    conf.maxSpoolFiles = 10;
    conf.initialFileToProcess = null;
    conf.dataFormatConfig.compression = Compression.COMPRESSED_ARCHIVE;
    conf.dataFormatConfig.filePatternInArchive = "*/[!.]*.log";
    conf.errorArchiveDir = null;
    conf.postProcessing = PostProcessingOptions.NONE;
    conf.archiveDir = testDir.getAbsolutePath();
    conf.retentionTimeMins = 10;
    conf.dataFormatConfig.textMaxLineLen = 10;
    conf.dataFormatConfig.onParseError = OnParseError.ERROR;
    conf.dataFormatConfig.maxStackTraceLines = 0;

    return new SpoolDirSource(conf);
  }

  private SpoolDirSource createTarGzipAvroSource() {
    SpoolDirConfigBean conf = new SpoolDirConfigBean();
    conf.dataFormat = DataFormat.AVRO;
    conf.spoolDir = testDir.getAbsolutePath();
    conf.batchSize = 1000;
    conf.overrunLimit = 65;
    conf.poolingTimeoutSecs = 1;
    conf.filePattern = "testAvro*.tar.gz";
    conf.maxSpoolFiles = 10;
    conf.initialFileToProcess = null;
    conf.dataFormatConfig.compression = Compression.COMPRESSED_ARCHIVE;
    conf.dataFormatConfig.filePatternInArchive = "[!.]*.avro";
    conf.errorArchiveDir = null;
    conf.postProcessing = PostProcessingOptions.NONE;
    conf.archiveDir = testDir.getAbsolutePath();
    conf.retentionTimeMins = 10;
    conf.dataFormatConfig.onParseError = OnParseError.ERROR;
    conf.dataFormatConfig.maxStackTraceLines = 0;

    return new SpoolDirSource(conf);
  }

  private SpoolDirSource createBz2Source() {
    SpoolDirConfigBean conf = new SpoolDirConfigBean();
    conf.dataFormat = DataFormat.TEXT;
    conf.spoolDir = testDir.getAbsolutePath();
    conf.batchSize = 1000;
    conf.overrunLimit = 65;
    conf.poolingTimeoutSecs = 1;
    conf.filePattern = "testFile*.bz2";
    conf.maxSpoolFiles = 10;
    conf.initialFileToProcess = null;
    conf.dataFormatConfig.compression = Compression.COMPRESSED_FILE;
    conf.dataFormatConfig.filePatternInArchive = "*";
    conf.errorArchiveDir = null;
    conf.postProcessing = PostProcessingOptions.NONE;
    conf.archiveDir = testDir.getAbsolutePath();
    conf.retentionTimeMins = 10;
    conf.dataFormatConfig.textMaxLineLen = 10;
    conf.dataFormatConfig.onParseError = OnParseError.ERROR;
    conf.dataFormatConfig.maxStackTraceLines = 0;

    return new SpoolDirSource(conf);
  }

}
