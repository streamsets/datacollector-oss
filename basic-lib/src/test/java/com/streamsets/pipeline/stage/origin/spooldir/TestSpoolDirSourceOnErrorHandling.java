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
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.config.Compression;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.CsvRecordType;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.config.OnParseError;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.lib.dirspooler.PathMatcherMode;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public class TestSpoolDirSourceOnErrorHandling {

  private String createTestDir() {
    File f = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(f.mkdirs());
    return f.getAbsolutePath();
  }

  private SpoolDirSource createSource() throws Exception {
    String dir = createTestDir();
    File file1 = new File(dir, "file-0.csv").getAbsoluteFile();
    Writer writer = new FileWriter(file1);
    IOUtils.write("a,b\ncccc,dddd\ne,f\n", writer);
    writer.close();
    File file2 = new File(dir, "file-1.csv").getAbsoluteFile();
    writer = new FileWriter(file2);
    IOUtils.write("x,y", writer);
    writer.close();

    SpoolDirConfigBean conf = new SpoolDirConfigBean();
    conf.dataFormat = DataFormat.DELIMITED;
    conf.spoolDir = dir;
    conf.batchSize = 10;
    conf.overrunLimit = 100;
    conf.poolingTimeoutSecs = 1;
    conf.filePattern = "file-[0-9].csv";
    conf.pathMatcherMode = PathMatcherMode.GLOB;
    conf.maxSpoolFiles = 10;
    conf.initialFileToProcess = null;
    conf.dataFormatConfig.compression = Compression.NONE;
    conf.dataFormatConfig.filePatternInArchive = "*";
    conf.errorArchiveDir = null;
    conf.postProcessing = PostProcessingOptions.ARCHIVE;
    conf.archiveDir = dir;
    conf.retentionTimeMins = 10;
    conf.dataFormatConfig.csvFileFormat = CsvMode.RFC4180;
    conf.dataFormatConfig.csvHeader = CsvHeader.NO_HEADER;
    conf.dataFormatConfig.csvMaxObjectLen = 5;
    conf.dataFormatConfig.csvCustomDelimiter = '^';
    conf.dataFormatConfig.csvCustomEscape = '^';
    conf.dataFormatConfig.csvCustomQuote = '^';
    conf.dataFormatConfig.csvRecordType = CsvRecordType.LIST;
    conf.dataFormatConfig.onParseError = OnParseError.ERROR;
    conf.dataFormatConfig.maxStackTraceLines = -1;

    return new SpoolDirSource(conf);
  }

  @Test
  public void testOnErrorDiscardMaxObjectLen() throws Exception {
    SpoolDirSource source = createSource();
    PushSourceRunner runner = new PushSourceRunner.Builder(SpoolDirDSource.class, source).addOutputLane("lane")
                                                          .setOnRecordError(OnRecordError.DISCARD).build();

    List<Record> records = Collections.synchronizedList(new ArrayList<>(10));
    AtomicInteger batchCount = new AtomicInteger(0);

    runner.runInit();
    try {
      runner.runProduce(ImmutableMap.of(Source.POLL_SOURCE_OFFSET_KEY, "file-0.csv::0"), 10, output -> {

        synchronized (batchCount) {
          batchCount.incrementAndGet();

          if (batchCount.get() < 2) {
            Assert.assertEquals("file-0.csv", output.getOffsetEntity());
            Assert.assertEquals("{\"POS\":\"-1\"}", output.getNewOffset());
            Assert.assertEquals(2, output.getRecords().get("lane").size());
            Assert.assertEquals("a", output.getRecords().get("lane").get(0).get("[0]/value").getValueAsString());
            Assert.assertEquals("e", output.getRecords().get("lane").get(1).get("[0]/value").getValueAsString());
            Assert.assertEquals(0, runner.getErrorRecords().size());
          } else {
            synchronized (records) {
              records.addAll(output.getRecords().get("lane"));
            }
            runner.setStop();
          }
        }
      });

      runner.waitOnProduce();

      Assert.assertEquals(2, batchCount.get());
      TestOffsetUtil.compare("file-1.csv::-1", runner.getOffsets());
      Assert.assertEquals(1, records.size());
      Assert.assertEquals("x", records.get(0).get("[0]/value").getValueAsString());
      Assert.assertEquals(0, runner.getErrorRecords().size());

    } finally {
      runner.runDestroy();
    }
  }

  @Ignore
  public void testOnErrorToErrorMaxObjectLen() throws Exception {
    SpoolDirSource source = createSource();
    PushSourceRunner runner = new PushSourceRunner.Builder(SpoolDirDSource.class, source).addOutputLane("lane")
                                                          .setOnRecordError(OnRecordError.TO_ERROR).build();
    List<Record> records = Collections.synchronizedList(new ArrayList<>(10));
    AtomicInteger batchCount = new AtomicInteger(0);
    runner.runInit();

    Offset offset = new Offset("1", "file-0.csv", "0");

    try {
      runner.runProduce(ImmutableMap.of(Source.POLL_SOURCE_OFFSET_KEY, offset.toString()), 10, output -> {
        batchCount.incrementAndGet();

        if (batchCount.get() < 2) {
          Assert.assertEquals("file-0.csv::-1", output.getNewOffset());
          Assert.assertEquals(2, output.getRecords().get("lane").size());
          Assert.assertEquals("a", output.getRecords().get("lane").get(0).get("[0]/value").getValueAsString());
          Assert.assertEquals("e", output.getRecords().get("lane").get(1).get("[0]/value").getValueAsString());
          Assert.assertEquals(0, runner.getErrorRecords().size());
          Assert.assertEquals(1, runner.getErrors().size());

          runner.getErrors().clear();

        } else {
          synchronized (records) {
            records.addAll(output.getRecords().get("lane"));
          }
          runner.setStop();
        }
      });

      runner.waitOnProduce();

      Assert.assertEquals("file-1.csv::-1", runner.getOffsets().get(Source.POLL_SOURCE_OFFSET_KEY));
      Assert.assertEquals(1, records.size());
      Assert.assertEquals("x", records.get(0).get("[0]/value").getValueAsString());
      Assert.assertEquals(0, runner.getErrorRecords().size());
      Assert.assertEquals(1, runner.getErrors().size());

    } finally {
      runner.runDestroy();
    }
  }

  @Test(expected = ExecutionException.class)
  public void testOnErrorLenPipelineMaxObjectLen() throws Exception {
    SpoolDirSource source = createSource();
    PushSourceRunner runner = new PushSourceRunner.Builder(SpoolDirDSource.class, source).addOutputLane("lane")
                                                          .setOnRecordError(OnRecordError.STOP_PIPELINE).build();
    List<Record> records = Collections.synchronizedList(new ArrayList<>(10));
    AtomicInteger batchCount = new AtomicInteger(0);
    runner.runInit();
    try {
      runner.runProduce(ImmutableMap.of(Source.POLL_SOURCE_OFFSET_KEY, "file-0.csv::0"), 10, output -> {
        synchronized (records) {
          records.addAll(output.getRecords().get("lane"));
        }
        batchCount.incrementAndGet();
        runner.setStop();
      });
      runner.waitOnProduce();
    } finally {
      runner.runDestroy();
    }
  }

  private SpoolDirSource createSourceIOEx() throws Exception {
    String dir = createTestDir();
    File file1 = new File(dir, "file-0.json").getAbsoluteFile();
    Writer writer = new FileWriter(file1);
    IOUtils.write("[1,", writer);
    writer.close();
    File file2 = new File(dir, "file-1.json").getAbsoluteFile();
    writer = new FileWriter(file2);
    IOUtils.write("[2]", writer);
    writer.close();

    SpoolDirConfigBean conf = new SpoolDirConfigBean();
    conf.dataFormat = DataFormat.JSON;
    conf.spoolDir = dir;
    conf.batchSize = 10;
    conf.overrunLimit = 100;
    conf.poolingTimeoutSecs = 1;
    conf.filePattern = "file-[0-9].json";
    conf.pathMatcherMode = PathMatcherMode.GLOB;
    conf.maxSpoolFiles = 10;
    conf.initialFileToProcess = null;
    conf.dataFormatConfig.compression = Compression.NONE;
    conf.dataFormatConfig.filePatternInArchive = "*";
    conf.errorArchiveDir = null;
    conf.postProcessing = PostProcessingOptions.ARCHIVE;
    conf.archiveDir = dir;
    conf.retentionTimeMins = 10;
    conf.dataFormatConfig.jsonContent = JsonMode.ARRAY_OBJECTS;
    conf.dataFormatConfig.jsonMaxObjectLen = 100;
    conf.dataFormatConfig.onParseError = OnParseError.ERROR;
    conf.dataFormatConfig.maxStackTraceLines = 0;

    return new SpoolDirSource(conf);
  }

  private SpoolDirSource createSourceDelimitedEx() throws Exception {
    String dir = createTestDir();
    File file = new File(dir, "file-0.csv").getAbsoluteFile();
    Writer writer = new FileWriter(file);
    IOUtils.write("|header1|header2|\n", writer);
    final int sizeOfRecords = 20;
    for (int i = 0; i < sizeOfRecords; i++) {
      IOUtils.write("|value" + i + "1|value" + i + "2|value" + i + "3|\n", writer);
    }
    writer.close();

    SpoolDirConfigBean conf = new SpoolDirConfigBean();
    conf.dataFormat = DataFormat.DELIMITED;

    conf.dataFormatConfig.useCustomDelimiter = true;
    conf.dataFormatConfig.csvFileFormat = CsvMode.CUSTOM;
    conf.dataFormatConfig.csvHeader = CsvHeader.WITH_HEADER;


    conf.spoolDir = dir;
    conf.batchSize = 10;
    conf.overrunLimit = 100;
    conf.poolingTimeoutSecs = 1;
    conf.filePattern = "file-[0-9].csv";
    conf.pathMatcherMode = PathMatcherMode.GLOB;
    conf.maxSpoolFiles = 10;
    conf.initialFileToProcess = null;
    conf.errorArchiveDir = null;
    conf.postProcessing = PostProcessingOptions.ARCHIVE;
    conf.archiveDir = dir;
    conf.retentionTimeMins = 10;
    /*conf.dataFormatConfig.jsonContent = JsonMode.ARRAY_OBJECTS;
    conf.dataFormatConfig.jsonMaxObjectLen = 100;
    */
    conf.dataFormatConfig.onParseError = OnParseError.ERROR;
    conf.dataFormatConfig.maxStackTraceLines = 0;

    return new SpoolDirSource(conf);
  }

  @Test
  public void testOnErrorDiscardIOEx() throws Exception {
    SpoolDirSource source = createSourceIOEx();
    PushSourceRunner runner = new PushSourceRunner.Builder(SpoolDirDSource.class, source).addOutputLane("lane")
                                                          .setOnRecordError(OnRecordError.DISCARD).build();
    List<Record> records = Collections.synchronizedList(new ArrayList<>(10));
    AtomicInteger batchCount = new AtomicInteger(0);
    runner.runInit();
    try {
      runner.runProduce(ImmutableMap.of(Source.POLL_SOURCE_OFFSET_KEY, "file-0.json::0"), 10, output -> {
        synchronized (records) {
          records.addAll(output.getRecords().get("lane"));
        }
        batchCount.incrementAndGet();

        if (batchCount.get() < 2) {
          Assert.assertEquals("file-0.json", output.getOffsetEntity());
          Assert.assertEquals("{\"POS\":\"-1\"}", output.getNewOffset());
          Assert.assertEquals(1, records.size());
          Assert.assertEquals(1, records.get(0).get("").getValueAsInteger());
          Assert.assertEquals(0, runner.getErrorRecords().size());
          records.clear();
        } else {
          runner.setStop();
        }
      });

      runner.waitOnProduce();

      Assert.assertFalse(runner.getOffsets().containsKey("file-0.json"));
      TestOffsetUtil.compare("file-1.json::-1", runner.getOffsets());
      Assert.assertEquals(1, records.size());
      Assert.assertEquals(2, records.get(0).get("").getValueAsInteger());
      Assert.assertEquals(0, runner.getErrorRecords().size());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testOnErrorToErrorIOEx() throws Exception {
    SpoolDirSource source = createSourceIOEx();
    PushSourceRunner runner = new PushSourceRunner.Builder(SpoolDirDSource.class, source).addOutputLane("lane")
                                                          .setOnRecordError(OnRecordError.TO_ERROR).build();
    List<Record> records = Collections.synchronizedList(new ArrayList<>(10));
    AtomicInteger batchCount = new AtomicInteger(0);
    runner.runInit();
    try {
      runner.runProduce(ImmutableMap.of(Source.POLL_SOURCE_OFFSET_KEY, "file-0.json::0"), 10, output -> {
        synchronized (records) {
          records.addAll(output.getRecords().get("lane"));
        }
        batchCount.incrementAndGet();

        if (batchCount.get() < 2) {
          Assert.assertEquals("file-0.json", output.getOffsetEntity());
          Assert.assertEquals("{\"POS\":\"-1\"}", output.getNewOffset());
          Assert.assertEquals(1, records.size());
          Assert.assertEquals(1, records.get(0).get("").getValueAsInteger());
          Assert.assertEquals(0, runner.getErrorRecords().size());
          Assert.assertEquals(1, runner.getErrors().size());

          records.clear();
          runner.clearErrors();
        } else {
          runner.setStop();
        }
      });

      runner.waitOnProduce();

      Assert.assertEquals(2, batchCount.get());
      TestOffsetUtil.compare("file-1.json::-1", runner.getOffsets());
      Assert.assertEquals(1, records.size());
      Assert.assertEquals(2, records.get(0).get("").getValueAsInteger());
      Assert.assertEquals(0, runner.getErrorRecords().size());
      Assert.assertEquals(0, runner.getErrors().size());

    } finally {
      runner.runDestroy();
    }
  }

  //@Test(expected = StageException.class)
  public void testOnErrorPipelineIOEx() throws Exception {
    SpoolDirSource source = createSourceIOEx();
    PushSourceRunner runner = new PushSourceRunner.Builder(SpoolDirDSource.class, source).addOutputLane("lane")
                                                          .setOnRecordError(OnRecordError.STOP_PIPELINE).build();
    runner.runInit();
    try {
      runner.runProduce(ImmutableMap.of(Source.POLL_SOURCE_OFFSET_KEY, "file-0.json::0"), 10, output -> {

      });
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testOnObjectLengthException() throws Exception {
    SpoolDirSource source = createSourceDelimitedEx();
    PushSourceRunner runner = new PushSourceRunner.Builder(SpoolDirDSource.class, source).addOutputLane("lane")
        .setOnRecordError(OnRecordError.TO_ERROR).build();
    List<Record> records = Collections.synchronizedList(new ArrayList<>(10));

    runner.runInit();

    try {
      final int maxBatchSize = 1;
      Offset offset = new Offset("1", "file-0.csv", "0");
      runner.runProduce(ImmutableMap.of(Source.POLL_SOURCE_OFFSET_KEY, offset.toString()), maxBatchSize, output -> {
        synchronized (records) {
          records.addAll(output.getRecords().get("lane"));
        }
        runner.setStop();
      });
      runner.waitOnProduce();

      Assert.assertEquals(0, records.size());
      Assert.assertEquals(maxBatchSize, runner.getErrorRecords().size());
      Assert.assertEquals(0, runner.getErrors().size());
    } catch (Exception ex) {
      ex.getStackTrace();
    } finally {
      runner.runDestroy();
    }
  }

}
