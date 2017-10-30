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

import com.google.common.base.Strings;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.Compression;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.CsvRecordType;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.OnParseError;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.lib.dirspooler.PathMatcherMode;
import com.streamsets.pipeline.lib.dirspooler.SpoolDirRunnable;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class TestCsvSpoolDirSource {
  private static final int threadNumber = 0;
  private static final int batchSize = 10;
  private static final Map<String, Offset> lastSourceOffset = new HashMap<>();

  private String spoolDir;

  private String createTestDir() {
    File f = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(f.mkdirs());
    return f.getAbsolutePath();
  }

  private final static String LINE1 = "A,B";
  private final static String LINE2 = "a,b";
  private final static String LINE3 = "e,f";

  private File createDelimitedFile() throws Exception {
    File f = new File(createTestDir(), "test.log");
    Writer writer = new FileWriter(f);
    writer.write(LINE1 + "\n");
    writer.write(LINE2 + "\n");
    writer.write(LINE3 + "\n");
    writer.close();
    return f;
  }

  private File createCustomDelimitedFile() throws Exception {
    File f = new File(createTestDir(), "test.log");
    Writer writer = new FileWriter(f);
    writer.write("A^!B !^$^A\n");
    writer.close();
    return f;
  }

  private File createSomeRecordsTooLongFile() throws Exception {
    File f = new File(createTestDir(), "test.log");
    Writer writer = new FileWriter(f);
    writer.write("a,b,c,d\n");
    writer.write("e,f,g,h\n");
    writer.write("aaa,bbb,ccc,ddd\n");
    writer.write("i,j,k,l\n");
    writer.write("aa1,bb1,cc1,dd1\n");
    writer.write("aa2,bb2,cc2,dd2\n");
    writer.write("m,n,o,p\n");
    writer.write("q,r,s,t\n");
    writer.write("aa3,bb3,cc3,dd3\n");
    writer.write("aa4,bb5,cc5,dd5\n");
    writer.close();
    return f;
  }

  private File createCommentFile() throws Exception {
    File f = new File(createTestDir(), "test.log");
    Writer writer = new FileWriter(f);
    writer.write("a,b\n");
    writer.write("# This is comment\n");
    writer.write("c,d\n");
    writer.close();
    return f;
  }

  private File createEmptyLineFile() throws Exception {
    File f = new File(createTestDir(), "test.log");
    Writer writer = new FileWriter(f);
    writer.write("a,b\n");
    writer.write("\n");
    writer.write("c,d\n");
    writer.close();
    return f;
  }

  private SpoolDirSource createSource(
      CsvMode mode,
      CsvHeader header,
      char delimiter,
      char escape,
      char quote,
      boolean commentsAllowed,
      char comment,
      boolean ignoreEmptyLines,
      int maxLen,
      CsvRecordType csvRecordType,
      String filePath,
      String pattern) {

    SpoolDirConfigBean conf = new SpoolDirConfigBean();
    conf.dataFormat = DataFormat.DELIMITED;
    conf.dataFormatConfig.charset = "UTF-8";
    conf.dataFormatConfig.removeCtrlChars = false;
    conf.overrunLimit = 100;
    conf.spoolDir = Strings.isNullOrEmpty(filePath) ? createTestDir() : filePath;
    conf.batchSize = 10;
    conf.poolingTimeoutSecs = 1;
    conf.filePattern = Strings.isNullOrEmpty(pattern) ? "file-[0-9].log" : pattern;
    conf.pathMatcherMode = PathMatcherMode.GLOB;
    conf.maxSpoolFiles = 10;
    conf.initialFileToProcess = null;
    conf.dataFormatConfig.compression = Compression.NONE;
    conf.dataFormatConfig.filePatternInArchive = "*";
    conf.errorArchiveDir = null;
    conf.postProcessing = PostProcessingOptions.ARCHIVE;
    conf.archiveDir = createTestDir();
    conf.retentionTimeMins = 10;
    conf.dataFormatConfig.csvFileFormat = mode;
    conf.dataFormatConfig.csvHeader = header;
    conf.dataFormatConfig.csvMaxObjectLen = maxLen;
    conf.dataFormatConfig.csvCustomDelimiter = delimiter;
    conf.dataFormatConfig.csvCustomEscape = escape;
    conf.dataFormatConfig.csvCustomQuote = quote;
    conf.dataFormatConfig.csvRecordType = csvRecordType;
    conf.dataFormatConfig.csvEnableComments = commentsAllowed;
    conf.dataFormatConfig.csvCommentMarker = comment;
    conf.dataFormatConfig.csvIgnoreEmptyLines = ignoreEmptyLines;
    conf.dataFormatConfig.onParseError = OnParseError.ERROR;
    conf.dataFormatConfig.maxStackTraceLines = 0;

    this.spoolDir = conf.spoolDir;
    return new SpoolDirSource(conf);
  }

  @Test
  public void testProduceFullFile() throws Exception {
    SpoolDirSource source = createSource(CsvMode.RFC4180, CsvHeader.NO_HEADER, '|', '\\', '"', false, ' ', true, 5, CsvRecordType.LIST, "", "");
    PushSourceRunner runner = new PushSourceRunner.Builder(SpoolDirDSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
      File testFile = createDelimitedFile();
      SpoolDirRunnable runnable = source.getSpoolDirRunnable(threadNumber, batchSize, lastSourceOffset);
      Assert.assertEquals("-1", runnable.generateBatch(testFile, "0", 10, batchMaker));
      StageRunner.Output output = SourceRunner.getOutput(batchMaker);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(3, records.size());
      Assert.assertEquals("A", records.get(0).get("[0]/value").getValueAsString());
      Assert.assertEquals("B", records.get(0).get("[1]/value").getValueAsString());
      Assert.assertEquals(testFile.getPath(), records.get(0).getHeader().getAttribute(HeaderAttributeConstants.FILE));
      Assert.assertEquals("test.log", records.get(0).getHeader().getAttribute(HeaderAttributeConstants.FILE_NAME));
      Assert.assertEquals("0", records.get(0).getHeader().getAttribute(HeaderAttributeConstants.OFFSET));
      Assert.assertFalse(records.get(0).has("[0]/header"));
      Assert.assertFalse(records.get(0).has("[1]/header"));
      Assert.assertFalse(records.get(0).has("[2]"));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testProduceFullFileWithListMap() throws Exception {
    SpoolDirSource source = createSource(CsvMode.RFC4180, CsvHeader.NO_HEADER, '|', '\\', '"', false, ' ', true, 5, CsvRecordType.LIST_MAP, "", "");
    PushSourceRunner runner = new PushSourceRunner.Builder(SpoolDirDSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
      SpoolDirRunnable runnable = source.getSpoolDirRunnable(threadNumber, batchSize, lastSourceOffset);
      Assert.assertEquals("-1", runnable.generateBatch(createDelimitedFile(), "0", 10, batchMaker));
      StageRunner.Output output = SourceRunner.getOutput(batchMaker);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(3, records.size());
      Assert.assertEquals("A", records.get(0).get("/0").getValueAsString());
      Assert.assertEquals("A", records.get(0).get("[0]").getValueAsString());
      Assert.assertEquals("B", records.get(0).get("/1").getValueAsString());
      Assert.assertEquals("B", records.get(0).get("[1]").getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testProduceLessThanFileIgnoreHeader() throws Exception {
    testProduceLessThanFile(true);
    testProduceLessThanFileWithListMap(true);
  }

  @Test
  public void testProduceLessThanFileWithHeader() throws Exception {
    testProduceLessThanFile(false);
    testProduceLessThanFileWithListMap(false);
  }

  private void testProduceLessThanFile(boolean ignoreHeader) throws Exception {
    SpoolDirSource source = createSource(CsvMode.RFC4180,
                                         (ignoreHeader) ? CsvHeader.IGNORE_HEADER : CsvHeader.WITH_HEADER, '|', '\\',
                                         '"', false, ' ', true, 5, CsvRecordType.LIST, "", "");
    PushSourceRunner runner = new PushSourceRunner.Builder(SpoolDirDSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
      SpoolDirRunnable runnable = source.getSpoolDirRunnable(threadNumber, batchSize, lastSourceOffset);
      String offset = runnable.generateBatch(createDelimitedFile(), "0", 1, batchMaker);
      Assert.assertEquals("8", offset);
      StageRunner.Output output = SourceRunner.getOutput(batchMaker);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(1, records.size());
      Assert.assertEquals("a", records.get(0).get("[0]/value").getValueAsString());
      Assert.assertEquals("b", records.get(0).get("[1]/value").getValueAsString());
      if (ignoreHeader) {
        Assert.assertFalse(records.get(0).has("[0]/header"));
        Assert.assertFalse(records.get(0).has("[1]/header"));
      } else {
        Assert.assertEquals("A", records.get(0).get("[0]/header").getValueAsString());
        Assert.assertEquals("B", records.get(0).get("[1]/header").getValueAsString());
      }

      batchMaker = SourceRunner.createTestBatchMaker("lane");
      offset = runnable.generateBatch(createDelimitedFile(), offset, 1, batchMaker);
      Assert.assertEquals("12", offset);
      output = SourceRunner.getOutput(batchMaker);
      records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(1, records.size());
      Assert.assertEquals("e", records.get(0).get("[0]/value").getValueAsString());
      Assert.assertEquals("f", records.get(0).get("[1]/value").getValueAsString());
      if (ignoreHeader) {
        Assert.assertFalse(records.get(0).has("[0]/header"));
        Assert.assertFalse(records.get(0).has("[1]/header"));
      } else {
        Assert.assertEquals("A", records.get(0).get("[0]/header").getValueAsString());
        Assert.assertEquals("B", records.get(0).get("[1]/header").getValueAsString());
      }

      batchMaker = SourceRunner.createTestBatchMaker("lane");
      offset = runnable.generateBatch(createDelimitedFile(), offset, 1, batchMaker);
      Assert.assertEquals("-1", offset);
      output = SourceRunner.getOutput(batchMaker);
      records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(0, records.size());
    } finally {
      runner.runDestroy();
    }
  }

  private void testProduceLessThanFileWithListMap(boolean ignoreHeader) throws Exception {
    SpoolDirSource source = createSource(CsvMode.RFC4180,
      (ignoreHeader) ? CsvHeader.IGNORE_HEADER : CsvHeader.WITH_HEADER, '|', '\\',
      '"', false, ' ', true, 5, CsvRecordType.LIST_MAP, "", "");
    PushSourceRunner runner = new PushSourceRunner.Builder(SpoolDirDSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
      SpoolDirRunnable runnable = source.getSpoolDirRunnable(threadNumber, batchSize, lastSourceOffset);
      String offset = runnable.generateBatch(createDelimitedFile(), "0", 1, batchMaker);
      Assert.assertEquals("8", offset);
      StageRunner.Output output = SourceRunner.getOutput(batchMaker);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(1, records.size());
      if (ignoreHeader) {
        Assert.assertEquals("a", records.get(0).get("[0]").getValueAsString());
        Assert.assertEquals("a", records.get(0).get("/0").getValueAsString());
        Assert.assertEquals("b", records.get(0).get("[1]").getValueAsString());
        Assert.assertEquals("b", records.get(0).get("/1").getValueAsString());
      } else {
        Assert.assertEquals("a", records.get(0).get("[0]").getValueAsString());
        Assert.assertEquals("a", records.get(0).get("/A").getValueAsString());
        Assert.assertEquals("b", records.get(0).get("[1]").getValueAsString());
        Assert.assertEquals("b", records.get(0).get("/B").getValueAsString());
      }

      batchMaker = SourceRunner.createTestBatchMaker("lane");
      offset = runnable.generateBatch(createDelimitedFile(), offset, 1, batchMaker);
      Assert.assertEquals("12", offset);
      output = SourceRunner.getOutput(batchMaker);
      records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(1, records.size());
      if (ignoreHeader) {
        Assert.assertEquals("e", records.get(0).get("[0]").getValueAsString());
        Assert.assertEquals("e", records.get(0).get("/0").getValueAsString());
        Assert.assertEquals("f", records.get(0).get("[1]").getValueAsString());
        Assert.assertEquals("f", records.get(0).get("/1").getValueAsString());
      } else {
        Assert.assertEquals("e", records.get(0).get("[0]").getValueAsString());
        Assert.assertEquals("e", records.get(0).get("/A").getValueAsString());
        Assert.assertEquals("f", records.get(0).get("[1]").getValueAsString());
        Assert.assertEquals("f", records.get(0).get("/B").getValueAsString());
      }

      batchMaker = SourceRunner.createTestBatchMaker("lane");
      offset = runnable.generateBatch(createDelimitedFile(), offset, 1, batchMaker);
      Assert.assertEquals("-1", offset);
      output = SourceRunner.getOutput(batchMaker);
      records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(0, records.size());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testDelimitedCustom() throws Exception {
    SpoolDirSource source = createSource(CsvMode.CUSTOM, CsvHeader.NO_HEADER, '^', '$', '!', false, ' ', true,20, CsvRecordType.LIST, "", "");
    PushSourceRunner runner = new PushSourceRunner.Builder(SpoolDirDSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
      SpoolDirRunnable runnable = source.getSpoolDirRunnable(threadNumber, batchSize, lastSourceOffset);
      Assert.assertEquals("-1", runnable.generateBatch(createCustomDelimitedFile(), "0", 10, batchMaker));
      StageRunner.Output output = SourceRunner.getOutput(batchMaker);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(1, records.size());
      Assert.assertEquals("A", records.get(0).get("[0]/value").getValueAsString());
      Assert.assertEquals("B ", records.get(0).get("[1]/value").getValueAsString());
      Assert.assertEquals("^A", records.get(0).get("[2]/value").getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }

  @Ignore
  public void testRecordOverrunOnBatchBoundary() throws Exception {
    final File csvFile = createSomeRecordsTooLongFile();
    runRecordOverrunOnBatchBoundaryHelper(csvFile, 3, new int[] {2, 0}, new int[] {1, 3});
    runRecordOverrunOnBatchBoundaryHelper(csvFile, 4, new int[] {3, 2}, new int[] {1, 2});
    runRecordOverrunOnBatchBoundaryHelper(csvFile, 5, new int[] {3, 0}, new int[] {2, 2});
    runRecordOverrunOnBatchBoundaryHelper(csvFile, 6, new int[] {3, 0}, new int[] {3, 0});
  }

  private void runRecordOverrunOnBatchBoundaryHelper(File sourceFile, int batchSize, int[] recordCounts,
      int[] errorCounts) throws Exception {

    if (recordCounts.length != errorCounts.length) {
      throw new IllegalArgumentException("recordCounts and errorCounts must be same length");
    }

    final int maxLen = 8;
    SpoolDirSource source = createSource(CsvMode.CUSTOM, CsvHeader.NO_HEADER, '|', '\\', '"', false, ' ', true, maxLen, CsvRecordType.LIST, sourceFile.getParent(), "*.*");

    PushSourceRunner runner = new PushSourceRunner.Builder(SpoolDirDSource.class, source).addOutputLane("lane")
        .setOnRecordError(OnRecordError.TO_ERROR).build();

    AtomicInteger batchCount = new AtomicInteger();

    runner.runInit();

    try {
      runner.runProduce(new HashMap<>(), 10, output -> {

        List<Record> records = output.getRecords().get("lane");
        int produceNum = batchCount.getAndIncrement();

          if (!output.getNewOffset().endsWith("-1") && produceNum < 99) {
            final int recordCount = recordCounts[produceNum];
            final int errorCount = errorCounts[produceNum];

            Assert.assertNotNull(records);
            Assert.assertEquals(recordCount, records.size());
            Assert.assertEquals(errorCount, runner.getErrors().size());
          } else {
            runner.setStop();
          }
      });

      Assert.assertTrue(batchCount.get() > 0);
      runner.waitOnProduce();

    } finally {
      runner.runDestroy();
    }

  }


  @Test
  public void testDelimitedCustomWithListMap() throws Exception {
    SpoolDirSource source = createSource(CsvMode.CUSTOM, CsvHeader.NO_HEADER, '^', '$', '!', true, ' ', false, 20, CsvRecordType.LIST_MAP, "", "");
    PushSourceRunner runner = new PushSourceRunner.Builder(SpoolDirDSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
      SpoolDirRunnable runnable = source.getSpoolDirRunnable(threadNumber, batchSize, lastSourceOffset);
      Assert.assertEquals("-1", runnable.generateBatch(createCustomDelimitedFile(), "0", 10, batchMaker));
      StageRunner.Output output = SourceRunner.getOutput(batchMaker);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(1, records.size());
      Assert.assertEquals("A", records.get(0).get("/0").getValueAsString());
      Assert.assertEquals("A", records.get(0).get("[0]").getValueAsString());
      Assert.assertEquals("B ", records.get(0).get("/1").getValueAsString());
      Assert.assertEquals("B ", records.get(0).get("[1]").getValueAsString());
      Assert.assertEquals("^A", records.get(0).get("/2").getValueAsString());
      Assert.assertEquals("^A", records.get(0).get("[2]").getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }

  private File createInvalidDataFile(String file) throws Exception {
    File f = new File(file);
    Writer writer = new FileWriter(f);
    writer.write(",\",\"\"a,");
    writer.close();
    return f;
  }

  @Test //this test works for all formats as we are using a WrapperDataParser
  public void testInvalidData() throws Exception {
    SpoolDirSource source = createSource(CsvMode.EXCEL, CsvHeader.NO_HEADER, '^', '$', '!', false, ' ', true, 20, CsvRecordType.LIST, "", "");
    PushSourceRunner runner = new PushSourceRunner.Builder(SpoolDirDSource.class, source).addOutputLane("lane").
        setOnRecordError(OnRecordError.TO_ERROR).build();
    createInvalidDataFile(spoolDir + "/file-0.log");

    final List<Record> records = Collections.synchronizedList(new ArrayList<>(10));

    runner.runInit();

    try {
      runner.runProduce(new HashMap<>(), 10, output -> {
        synchronized (records) {
          records.addAll(output.getRecords().get("lane"));
        }
        runner.setStop();
      });
      runner.waitOnProduce();

      Assert.assertTrue(records.isEmpty());
      Assert.assertFalse(runner.getErrors().isEmpty());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testComment() throws Exception {
    SpoolDirSource source = createSource(CsvMode.CUSTOM, CsvHeader.NO_HEADER, ',', '\\', '"', true, '#', true, 50, CsvRecordType.LIST, "", "");
    PushSourceRunner runner = new PushSourceRunner.Builder(SpoolDirDSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
      SpoolDirRunnable runnable = source.getSpoolDirRunnable(threadNumber, batchSize, lastSourceOffset);
      Assert.assertEquals("-1", runnable.generateBatch(createCommentFile(), "0", 10, batchMaker));
      StageRunner.Output output = SourceRunner.getOutput(batchMaker);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(2, records.size());
      Assert.assertEquals("a", records.get(0).get("[0]/value").getValueAsString());
      Assert.assertEquals("b", records.get(0).get("[1]/value").getValueAsString());
      Assert.assertEquals("c", records.get(1).get("[0]/value").getValueAsString());
      Assert.assertEquals("d", records.get(1).get("[1]/value").getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testEmptyLineIgnore() throws Exception {
    SpoolDirSource source = createSource(CsvMode.CUSTOM, CsvHeader.NO_HEADER, ',', '\\', '"', true, '#', true, 50, CsvRecordType.LIST, "", "");
    PushSourceRunner runner = new PushSourceRunner.Builder(SpoolDirDSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
      SpoolDirRunnable runnable = source.getSpoolDirRunnable(threadNumber, batchSize, lastSourceOffset);
      Assert.assertEquals("-1", runnable.generateBatch(createEmptyLineFile(), "0", 10, batchMaker));
      StageRunner.Output output = SourceRunner.getOutput(batchMaker);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(2, records.size());
      Assert.assertEquals("a", records.get(0).get("[0]/value").getValueAsString());
      Assert.assertEquals("b", records.get(0).get("[1]/value").getValueAsString());
      Assert.assertEquals("c", records.get(1).get("[0]/value").getValueAsString());
      Assert.assertEquals("d", records.get(1).get("[1]/value").getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testEmptyLineNotIgnore() throws Exception {
    SpoolDirSource source = createSource(CsvMode.CUSTOM, CsvHeader.NO_HEADER, ',', '\\', '"', true, '#', false, 50, CsvRecordType.LIST, "", "");
    PushSourceRunner runner = new PushSourceRunner.Builder(SpoolDirDSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
      SpoolDirRunnable runnable = source.getSpoolDirRunnable(threadNumber, batchSize, lastSourceOffset);
      Assert.assertEquals("-1", runnable.generateBatch(createEmptyLineFile(), "0", 10, batchMaker));
      StageRunner.Output output = SourceRunner.getOutput(batchMaker);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(3, records.size());
      Assert.assertEquals("a", records.get(0).get("[0]/value").getValueAsString());
      Assert.assertEquals("b", records.get(0).get("[1]/value").getValueAsString());
      Assert.assertEquals("", records.get(1).get("[0]/value").getValueAsString());
      Assert.assertEquals("c", records.get(2).get("[0]/value").getValueAsString());
      Assert.assertEquals("d", records.get(2).get("[1]/value").getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }

}
