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

import com.google.common.io.Resources;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.Compression;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.OnParseError;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.lib.dirspooler.PathMatcherMode;
import com.streamsets.pipeline.sdk.DataCollectorServicesUtils;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import com.streamsets.pipeline.lib.dirspooler.SpoolDirConfigBean;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static com.streamsets.pipeline.config.OriginAvroSchemaSource.SOURCE;

public class TestSpoolDirWithCompression {

  private static File testDir;

  @BeforeClass
  public static void setUpClass() throws IOException, InterruptedException, URISyntaxException, CompressorException {
    DataCollectorServicesUtils.loadDefaultServices();

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
    PushSourceRunner runner = new PushSourceRunner.Builder(SpoolDirDSource.class, source).addOutputLane("lane").build();

    runner.runInit();

    try {
      AtomicInteger batchCount = new AtomicInteger();
      final List<Record> records = Collections.synchronizedList(new ArrayList<>(10));

      runner.runProduce(new HashMap<>(), 1000, output -> {
        synchronized (records) {
          records.addAll(output.getRecords().get("lane"));
        }

        if (batchCount.incrementAndGet() > 50) {
          runner.setStop();
        }
      });

      runner.waitOnProduce();
      Assert.assertNotNull(runner.getOffsets());
      Assert.assertEquals(37044, records.size());

      TestOffsetUtil.compare("logArchive2.zip::-1", runner.getOffsets());
    } finally {
      source.destroy();
      runner.runDestroy();
    }
  }

  @Test
  public void testProduceTarGzipTextFile() throws Exception {
    SpoolDirSource source = createTarGzipSource();
    PushSourceRunner runner = new PushSourceRunner.Builder(SpoolDirDSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      final List<Record> records = Collections.synchronizedList(new ArrayList<>(10));
      AtomicInteger batchCount = new AtomicInteger(0);

      runner.runProduce(new HashMap<>(), 1000, output -> {
        synchronized (records) {
          records.addAll(output.getRecords().get("lane"));
        }
        batchCount.incrementAndGet();

        if (batchCount.get() > 50) {
          runner.setStop();
        }
      });

      runner.waitOnProduce();

      Assert.assertNotNull(runner.getOffsets());
      Assert.assertEquals(37044, records.size());
      TestOffsetUtil.compare("logArchive2.tar.gz::-1", runner.getOffsets());
    } finally {
      source.destroy();
      runner.runDestroy();
    }
  }

  @Test
  public void testProduceTarGzipAvroFile() throws Exception {
    SpoolDirSource source = createTarGzipAvroSource();
    PushSourceRunner runner = new PushSourceRunner.Builder(SpoolDirDSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      final List<Record> records = Collections.synchronizedList(new ArrayList<>(10));
      AtomicInteger batchCount = new AtomicInteger(0);

      runner.runProduce(new HashMap<>(), 1000, output -> {
        synchronized (records) {
          records.addAll(output.getRecords().get("lane"));
        }
        batchCount.incrementAndGet();

        if (batchCount.get() > 50) {
          runner.setStop();
        }
      });
      runner.waitOnProduce();

      Assert.assertNotNull(runner.getOffsets());

      Assert.assertEquals(48000, records.size());
      TestOffsetUtil.compare("testAvro2.tar.gz::-1", runner.getOffsets());
    } finally {
      source.destroy();
      runner.runDestroy();
    }
  }

  @Test
  public void testProduceBz2File() throws Exception {
    SpoolDirSource source = createBz2Source();
    PushSourceRunner runner = new PushSourceRunner.Builder(SpoolDirDSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      final List<Record> records = Collections.synchronizedList(new ArrayList<>(10));
      AtomicInteger batchCount = new AtomicInteger(0);

      runner.runProduce(new HashMap<>(), 1000, output -> {
        synchronized (records) {
          records.addAll(output.getRecords().get("lane"));
        }
        batchCount.incrementAndGet();

        if (batchCount.get() > 10) {
          runner.setStop();
        }
      });

      runner.waitOnProduce();

      Assert.assertNotNull(runner.getOffsets());
      Assert.assertEquals(4, records.size());
      TestOffsetUtil.compare("testFile2.bz2::-1", runner.getOffsets());
    } finally {
      source.destroy();
      runner.runDestroy();
    }
  }

  private SpoolDirSource createZipSource() {
    SpoolDirConfigBean conf = new SpoolDirConfigBean();
    conf.dataFormat = DataFormat.TEXT;
    conf.spoolDir = testDir.getAbsolutePath();
    conf.batchSize = 1000;
    conf.overrunLimit = 65;
    conf.poolingTimeoutSecs = 10;
    conf.filePattern = "logArchive*.zip";
    conf.pathMatcherMode = PathMatcherMode.GLOB;
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
    conf.poolingTimeoutSecs = 10;
    conf.filePattern = "logArchive*.tar.gz";
    conf.pathMatcherMode = PathMatcherMode.GLOB;
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
    conf.dataFormatConfig.avroSchemaSource = SOURCE;
    conf.spoolDir = testDir.getAbsolutePath();
    conf.batchSize = 1000;
    conf.overrunLimit = 65;
    conf.poolingTimeoutSecs = 10;
    conf.filePattern = "testAvro*.tar.gz";
    conf.pathMatcherMode = PathMatcherMode.GLOB;
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
    conf.poolingTimeoutSecs = 10;
    conf.filePattern = "testFile*.bz2";
    conf.pathMatcherMode = PathMatcherMode.GLOB;
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
