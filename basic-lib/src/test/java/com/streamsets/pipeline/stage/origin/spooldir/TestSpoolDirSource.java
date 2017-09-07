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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.lineage.LineageEvent;
import com.streamsets.pipeline.api.lineage.LineageEventType;
import com.streamsets.pipeline.api.lineage.LineageSpecificAttribute;
import com.streamsets.pipeline.config.Compression;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.config.OnParseError;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.lib.dirspooler.PathMatcherMode;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

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
    PushSourceRunner runner = new PushSourceRunner.Builder(TSpoolDirSource.class, source).addOutputLane("lane").build();
    List<Record> records = Collections.synchronizedList(new ArrayList<>(10));
    AtomicInteger batchCount = new AtomicInteger(0);
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
    Assert.assertEquals("x", source.getFileFromSourceOffset(ImmutableMap.of(Source.POLL_SOURCE_OFFSET_KEY, source.createSourceOffset("x", ""))));
    Assert.assertEquals("0", source.getOffsetFromSourceOffset(null));
    Assert.assertEquals("0", source.getOffsetFromSourceOffset(ImmutableMap.of(Source.POLL_SOURCE_OFFSET_KEY, "x")));
    Assert.assertEquals("x", source.getFileFromSourceOffset(ImmutableMap.of(Source.POLL_SOURCE_OFFSET_KEY, source.createSourceOffset("x", "1"))));
    Assert.assertEquals("1", source.getOffsetFromSourceOffset(ImmutableMap.of(Source.POLL_SOURCE_OFFSET_KEY, source.createSourceOffset("x", "1"))));
  }

  @Test
  public void testOffsetMethods() throws Exception {
    TSpoolDirSource source = createSource(null);
    Assert.assertEquals(NULL_FILE_OFFSET, source.createSourceOffset(null, "0"));
    Assert.assertEquals("file1::0", source.createSourceOffset("file1", "0"));
    Assert.assertEquals("0", source.getOffsetFromSourceOffset(ImmutableMap.of(Source.POLL_SOURCE_OFFSET_KEY, NULL_FILE_OFFSET)));
    Assert.assertNull(source.getFileFromSourceOffset(ImmutableMap.of(Source.POLL_SOURCE_OFFSET_KEY, NULL_FILE_OFFSET)));
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
    TSpoolDirSource sourceWithLateDirectory = new TSpoolDirSource(conf);
    PushSourceRunner runner2 = new PushSourceRunner.Builder(TSpoolDirSource.class, sourceWithLateDirectory).addOutputLane("lane").build();
    AtomicInteger batchCount = new AtomicInteger(0);
    runner2.runInit();

    try {
      runner2.runProduce(new HashMap<>(), 10, output -> {
        batchCount.incrementAndGet();

        if (batchCount.get() == 1) {
          runner2.setStop();
        }
      });

      runner2.waitOnProduce();

      Assert.assertEquals(NULL_FILE_OFFSET, runner2.getOffsets().get(Source.POLL_SOURCE_OFFSET_KEY));

      Assert.assertEquals(1, runner2.getEventRecords().size());
      Assert.assertEquals("no-more-data", runner2.getEventRecords().get(0).getHeader().getAttribute(EventRecord.TYPE));

      Assert.assertTrue(f.mkdirs());

      File file = new File(source.spoolDir, "file-0.log").getAbsoluteFile();
      Files.createFile(file.toPath());

      source.file = file;
      source.offset = 1;
      source.maxBatchSize = 10;

      Thread.sleep(1000);

      PushSourceRunner runner3 = new PushSourceRunner.Builder(TSpoolDirSource.class, source).addOutputLane("lane").build();

      runner3.runInit();

      runner3.runProduce(ImmutableMap.of(Source.POLL_SOURCE_OFFSET_KEY, source.createSourceOffset("file-0.log", "1")), 10, output -> {
        batchCount.incrementAndGet();

        if (batchCount.get() > 1) {
          runner3.setStop();
        }
      });
      runner3.waitOnProduce();

      Assert.assertEquals(source.createSourceOffset("file-0.log", "1"), runner3.getOffsets().get(Source.POLL_SOURCE_OFFSET_KEY));

      Assert.assertEquals(1, runner3.getEventRecords().size());
      Assert.assertEquals("new-file", runner3.getEventRecords().get(0).getHeader().getAttribute(EventRecord.TYPE));

      runner3.runDestroy();

    } finally {
      runner2.runDestroy();
    }
  }

  @Test
  public void testProduceNoInitialFileNoFileInSpoolDirNullOffset() throws Exception {
    TSpoolDirSource source = createSource(null);
    PushSourceRunner runner = new PushSourceRunner.Builder(TSpoolDirSource.class, source).addOutputLane("lane").build();
    runner.runInit();

    try {
      runner.runProduce(new HashMap<>(), 10, output -> {
        runner.setStop();
      });
      runner.waitOnProduce();

      Assert.assertEquals(NULL_FILE_OFFSET, runner.getOffsets().get(Source.POLL_SOURCE_OFFSET_KEY));
      Assert.assertFalse(source.produceCalled);

      Assert.assertEquals(1, runner.getEventRecords().size());
      Assert.assertEquals("no-more-data", runner.getEventRecords().get(0).getHeader().getAttribute(EventRecord.TYPE));

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testProduceNoInitialFileWithFileInSpoolDirNullOffset() throws Exception {
    TSpoolDirSource source = createSource("file-0.log");
    PushSourceRunner runner = new PushSourceRunner.Builder(TSpoolDirSource.class, source).addOutputLane("lane").build();
    File file = new File(source.spoolDir, "file-0.log").getAbsoluteFile();
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

      Assert.assertEquals(source.createSourceOffset("file-0.log", "0"), runner.getOffsets().get(Source.POLL_SOURCE_OFFSET_KEY));
      Assert.assertTrue(source.produceCalled);
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
      runner.runProduce(ImmutableMap.of(Source.POLL_SOURCE_OFFSET_KEY, "file-0.log"), 10, output -> {
        runner.setStop();
      });

      runner.waitOnProduce();

      Assert.assertEquals(source.createSourceOffset("file-0.log", "0"), runner.getOffsets().get(Source.POLL_SOURCE_OFFSET_KEY));
    } finally {
      runner.runDestroy();
    }
  }


  @Test
  public void testProduceNoInitialFileWithFileInSpoolDirNotNullOffset() throws Exception {
    TSpoolDirSource source = createSource("file-0.log");
    PushSourceRunner runner = new PushSourceRunner.Builder(TSpoolDirSource.class, source).addOutputLane("lane").build();
    File file = new File(source.spoolDir, "file-0.log").getAbsoluteFile();
    Files.createFile(file.toPath());
    runner.runInit();
    source.file = file;
    source.offset = 0;
    source.maxBatchSize = 10;
    try {
      runner.runProduce(ImmutableMap.of(Source.POLL_SOURCE_OFFSET_KEY, "file-0.log"), 10, output -> {
        runner.setStop();
      });

      runner.waitOnProduce();

      Assert.assertEquals(source.createSourceOffset("file-0.log", "0"), runner.getOffsets().get(Source.POLL_SOURCE_OFFSET_KEY));
      Assert.assertTrue(source.produceCalled);
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testProduceNoInitialFileWithFileInSpoolDirNonZeroOffset() throws Exception {
    TSpoolDirSource source = createSource(null);
    PushSourceRunner runner = new PushSourceRunner.Builder(TSpoolDirSource.class, source).addOutputLane("lane").build();
    File file = new File(source.spoolDir, "file-0.log").getAbsoluteFile();
    Files.createFile(file.toPath());
    runner.runInit();
    source.file = file;
    source.offset = 1;
    source.maxBatchSize = 10;
    try {
      runner.runProduce(ImmutableMap.of(Source.POLL_SOURCE_OFFSET_KEY, source.createSourceOffset("file-0.log", "1")), 10, output -> {
        runner.setStop();
      });

      runner.waitOnProduce();

      Assert.assertEquals(source.createSourceOffset("file-0.log", "1"), runner.getOffsets().get(Source.POLL_SOURCE_OFFSET_KEY));
      Assert.assertTrue(source.produceCalled);
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testNoMoreFilesEmptyBatch() throws Exception {
    TSpoolDirSource source = createSource(null);
    PushSourceRunner runner = new PushSourceRunner.Builder(TSpoolDirSource.class, source).addOutputLane("lane").build();
    File file = new File(source.spoolDir, "file-0.log").getAbsoluteFile();
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

        if (batchCount.get() == 2) {
          Assert.assertEquals(source.createSourceOffset("file-0.log", "-1"), runner.getOffsets().get(Source.POLL_SOURCE_OFFSET_KEY));
          Assert.assertTrue(source.produceCalled);
          source.produceCalled = false;
        } else if (batchCount.get() == 3) {
          Assert.assertEquals(source.createSourceOffset("file-0.log", "-1"), runner.getOffsets().get(Source.POLL_SOURCE_OFFSET_KEY));
          //Produce will not be called as this file-0.log will not be eligible for produce
          Assert.assertFalse(source.produceCalled);
        } else if (batchCount.get() > 3){
          runner.setStop();
        }
      });

      runner.waitOnProduce();

      Assert.assertEquals(4, batchCount.get());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testAdvanceToNextSpoolFile() throws Exception {
    TSpoolDirSource source = createSource(null);
    PushSourceRunner runner = new PushSourceRunner.Builder(TSpoolDirSource.class, source).addOutputLane("lane").build();
    File file1 = new File(source.spoolDir, "file-0.log").getAbsoluteFile();
    Files.createFile(file1.toPath());
    File file2 = new File(source.spoolDir, "file-1.log").getAbsoluteFile();
    Files.createFile(file2.toPath());

    AtomicInteger batchCount = new AtomicInteger(0);
    runner.runInit();
    source.file = file1;
    source.offset = 0;
    source.maxBatchSize = 10;

    try {
      runner.runProduce(ImmutableMap.of(Source.POLL_SOURCE_OFFSET_KEY, source.createSourceOffset("file-0.log", "0")), 10, output -> {
        batchCount.incrementAndGet();

        if (batchCount.get() == 1) {
          Assert.assertEquals(source.createSourceOffset("file-0.log", "0"), output.getNewOffset()/*runner.getOffsets().get(Source.POLL_SOURCE_OFFSET_KEY)Assert*/);
          Assert.assertTrue(source.produceCalled);

          Assert.assertEquals(1, runner.getEventRecords().size());
          Assert.assertEquals("new-file", runner.getEventRecords().get(0).getHeader().getAttribute(EventRecord.TYPE));

          source.produceCalled = false;
          source.offsetIncrement = -1;
        } else if (batchCount.get() == 2) {
          Assert.assertEquals(source.createSourceOffset("file-0.log", "-1"), output.getNewOffset());
          Assert.assertTrue(source.produceCalled);
          Assert.assertEquals(2, runner.getEventRecords().size());
          Assert.assertEquals("new-file", runner.getEventRecords().get(0).getHeader().getAttribute(EventRecord.TYPE));

          Assert.assertEquals("finished-file", runner.getEventRecords().get(1).getHeader().getAttribute(EventRecord.TYPE));
          Assert.assertEquals(0, runner.getEventRecords().get(1).get("/error-count").getValueAsInteger());
          Assert.assertEquals(0, runner.getEventRecords().get(1).get("/record-count").getValueAsInteger());

          source.file = file2;

        } else if (batchCount.get() == 4) {
          source.produceCalled = false;
          source.offset = 0;
          source.offsetIncrement = 0;
          runner.setStop();
        } else if (batchCount.get() > 4) {
          runner.setStop();
        }
      });

      runner.waitOnProduce();

      Assert.assertEquals(4, batchCount.get());

      Assert.assertEquals(source.createSourceOffset("file-1.log", "-1"), runner.getOffsets().get(Source.POLL_SOURCE_OFFSET_KEY));
      Assert.assertFalse(source.produceCalled);

      // 2 each of new-file and finished-file and 1 no-more-data
      Assert.assertEquals(5, runner.getEventRecords().size());

      // check for LineageEvents.
      List<LineageEvent> events = runner.getLineageEvents();
      Assert.assertEquals(2, events.size());
      Assert.assertEquals(LineageEventType.ENTITY_READ, events.get(0).getEventType());
      Assert.assertEquals(LineageEventType.ENTITY_READ, events.get(1).getEventType());

      Assert.assertTrue(events.get(0).getSpecificAttribute(LineageSpecificAttribute.ENTITY_NAME).contains("file-0.log"));
      Assert.assertTrue(events.get(1).getSpecificAttribute(LineageSpecificAttribute.ENTITY_NAME).contains("file-1.log"));
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
    PushSourceRunner runner = new PushSourceRunner.Builder(SpoolDirSource.class, source)
      .setOnRecordError(OnRecordError.TO_ERROR)
      .addOutputLane("lane")
      .build();
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

      // Verify proper record
      Assert.assertNotNull(records);
      Assert.assertEquals(1, records.size());

      // And error record
      List<Record> errorRecords = runner.getErrorRecords();
      Assert.assertEquals(1, errorRecords.size());

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
      try (FileOutputStream outputStream = new FileOutputStream(current)) {
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
    PushSourceRunner runner = new PushSourceRunner.Builder(SpoolDirSource.class, source)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .addOutputLane("lane")
        .build();

    AtomicInteger batchCount = new AtomicInteger(0);
    runner.runInit();

    Assert.assertEquals(0, runner.getErrors().size());


    try {
      runner.runProduce(new HashMap<>(), 10, output -> {
        int i = batchCount.getAndIncrement();

        if (i < 8) {
          List<Record> records = output.getRecords().get("lane");
          Assert.assertNotNull(records);
          Assert.assertTrue(!records.isEmpty());
          Assert.assertEquals(2, records.size());

          Assert.assertEquals(Utils.format("file-{}.log", i),
              records.get(0).getHeader().getAttribute(HeaderAttributeConstants.FILE_NAME)
          );

          try {
            Assert.assertEquals(
                String.valueOf(Files.getLastModifiedTime(Paths.get(f.getAbsolutePath(), Utils.format("file-{}.log", i))).toMillis()),
                records.get(0).getHeader().getAttribute(HeaderAttributeConstants.LAST_MODIFIED_TIME)
            );
          } catch (IOException ex) {
            Assert.fail(ex.toString());
          }
          Assert.assertEquals("a-" + i, records.get(0).get("/A").getValueAsString());
          Assert.assertEquals("b-" + i, records.get(0).get("/B").getValueAsString());

          Assert.assertEquals("a", records.get(1).get("/A").getValueAsString());
          Assert.assertEquals("b", records.get(1).get("/B").getValueAsString());

          // And error record
          List<Record> errorRecords = runner.getErrorRecords();
          Assert.assertEquals(0, errorRecords.size());

        } else if (i < 9) {
          List<Record> records = output.getRecords().get("lane");
          Assert.assertNotNull(records);
          Assert.assertTrue(!records.isEmpty());
          Assert.assertEquals(2, records.size());

          Assert.assertEquals("a.log", records.get(0).getHeader().getAttribute(HeaderAttributeConstants.FILE_NAME));
          Assert.assertEquals("Gollum", records.get(0).get("/A").getValueAsString());
          Assert.assertEquals("Sauron", records.get(0).get("/B").getValueAsString());

          Assert.assertEquals("Aragorn", records.get(1).get("/A").getValueAsString());
          Assert.assertEquals("Boromir", records.get(1).get("/B").getValueAsString());
        } else if (i < 10) {
          List<Record> records = output.getRecords().get("lane");
          Assert.assertTrue(records.isEmpty());

          // And error record
          records = runner.getErrorRecords();
          Assert.assertEquals(0, records.size());
        } else if (i < 11) {
          // And a bunch of event records...
          // new-file event, finished-file event for each file.
          // file-0.log through file-7.log and a.log  (9 files)
          // two no-more-data events.
          Assert.assertEquals(20, runner.getEventRecords().size());
          Map<String, Integer> map = new HashMap<>();
          for(Record rec : runner.getEventRecords()) {
            if(map.get(rec.getHeader().getAttribute(EventRecord.TYPE)) != null) {
              map.put(rec.getHeader().getAttribute(EventRecord.TYPE),
                  map.get(rec.getHeader().getAttribute(EventRecord.TYPE)) + 1);
            } else {
              map.put(rec.getHeader().getAttribute(EventRecord.TYPE), 1);
            }
          }

          Assert.assertNotNull(map.get("new-file"));
          Assert.assertNotNull(map.get("finished-file"));
          Assert.assertNotNull(map.get("no-more-data"));

          int numEvents = map.get("new-file");
          Assert.assertEquals(9, numEvents);

          numEvents = map.get("finished-file");
          Assert.assertEquals(9, numEvents);

          numEvents = map.get("no-more-data");
          Assert.assertEquals(2, numEvents);
        } else {
          runner.setStop();
        }
      });


      runner.waitOnProduce();

      Assert.assertEquals(12, batchCount.get());

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
    PushSourceRunner runner = new PushSourceRunner.Builder(SpoolDirSource.class, source)
      .setPreview(preview)
      .setOnRecordError(OnRecordError.TO_ERROR)
      .addOutputLane("lane")
      .build();
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

      // Verify proper record
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
