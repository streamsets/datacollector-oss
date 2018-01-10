/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.hdfs;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public class TestHdfsSource {
  private static final Logger LOG = LoggerFactory.getLogger(TestHdfsSource.class);
  private final int NUM_RECORDS = 10;
  private final int NUM_FILES = 10;
  private final String FILENAME_TEMPLATE = "test-%s.json";
  private final String TARGET = "target";
  private String TEST_FOLDER_ROOT;

  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder(new File(String.format("./%s", TARGET)));

  @Before
  public void setUp() throws Exception {
    TEST_FOLDER_ROOT = testFolder.getRoot().getAbsolutePath().replace(String.format("./%s", TARGET), TARGET);
    for (int i = 0; i < NUM_FILES; i++) {
      writeTestData(i);
    }
  }

  private void writeTestData(int i) {
    final String fileName = String.format(FILENAME_TEMPLATE, i);
    try {
      File file = testFolder.newFile(fileName);
      FileOutputStream outputStream = new FileOutputStream(file);

      for (int index = NUM_RECORDS * i; index <NUM_RECORDS * (i+1); index++) {
        final String jsonString = String.format("{\"a\":%s}\n", index);
        outputStream.write(jsonString.getBytes());
      }

    } catch (IOException ex) {
      LOG.error("failed to create test files");
    }
  }

  @Test
  public void testEmptyDirectory() {
    HdfsSource source = new TestHdfsSourceBuilder()
        .dirPathTemplate("")
        .dataFormat(DataFormat.JSON)
        .build();

    PushSourceRunner runner = new PushSourceRunner.Builder(HdfsDSource.class, source)
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .addOutputLane("lane")
        .build();

    try {
      runner.runInit();
      Assert.fail("Should throw an exception if the directory does not exist");
    } catch (StageException ex) {
      // expected
      Assert.assertTrue(ex.getMessage().contains("HADOOPFS_02"));
    }
  }

  @Test
  public void testInvalidDirectory() throws Exception {
    HdfsSource source = new TestHdfsSourceBuilder()
        .dirPathTemplate("invlaidfaljtwlektj")
        .poolingTimeoutSecs(1)
        .firstFile(TEST_FOLDER_ROOT + "/test-0.json")
        .dataFormat(DataFormat.JSON)
        .build();

    PushSourceRunner runner = new PushSourceRunner.Builder(HdfsDSource.class, source)
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .addOutputLane("lane")
        .build();

    AtomicInteger batchCount = new AtomicInteger(0);
    final List<Record> records = Collections.synchronizedList(new ArrayList<>(NUM_FILES * NUM_RECORDS));
    final int maxBatchSize = NUM_RECORDS;

    runner.runInit();

    try {
      runner.runProduce(new HashMap<>(), maxBatchSize, output -> {
        batchCount.incrementAndGet();

        synchronized (records) {
          records.addAll(output.getRecords().get("lane"));
        }

        if (batchCount.get() > 10) {
          runner.setStop();
        }
      });

      runner.waitOnProduce();
      Assert.assertTrue(batchCount.get() > 1);
      Assert.assertEquals(0, records.size());
    } catch (Exception ex) {
      System.err.println(ex.toString());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testGeneralSource() throws Exception {
    HdfsSource source = new TestHdfsSourceBuilder()
        .dirPathTemplate(TEST_FOLDER_ROOT)
        .dataFormat(DataFormat.JSON)
        .build();

    PushSourceRunner runner = new PushSourceRunner.Builder(HdfsDSource.class, source)
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .addOutputLane("lane")
        .build();

    AtomicInteger batchCount = new AtomicInteger(0);
    final List<Record> records = Collections.synchronizedList(new ArrayList<>(NUM_FILES * NUM_RECORDS));
    final int maxBatchSize = NUM_RECORDS;

    runner.runInit();

    try {
      runner.runProduce(new HashMap<>(), maxBatchSize, output -> {
        batchCount.incrementAndGet();

        synchronized (records) {
          records.addAll(output.getRecords().get("lane"));
        }

        if (records.size() >= NUM_FILES * NUM_RECORDS || batchCount.get() > 11) {
          runner.setStop();
        }
      });

      runner.waitOnProduce();
      Assert.assertTrue(batchCount.get() > 1);
      Assert.assertEquals(NUM_FILES * NUM_RECORDS, records.size());
      Assert.assertEquals(2, runner.getOffsets().size());
    } catch (Exception ex) {
      System.err.println(ex.toString());
      Assert.fail("test failed");
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testHandleLastOffset() {
    HdfsSource source = new TestHdfsSourceBuilder()
        .dirPathTemplate(TEST_FOLDER_ROOT)
        .dataFormat(DataFormat.JSON)
        .build();

    Map<String, String> lastOffset = ImmutableMap.of(HdfsSource.OFFSET_VERSION, "1",
        "file-5.json", "0",
        "file-1.json", "0",
        "file-2.json", "0",
        "file-6.json", "0"
    );

    String firstFile = source.handleLastOffsets(lastOffset);

    Assert.assertEquals("file-1.json", firstFile);
  }

  @Test
  public void testWithInitialOffset() throws Exception {
    HdfsSource source = new TestHdfsSourceBuilder()
        .dirPathTemplate(TEST_FOLDER_ROOT)
        .dataFormat(DataFormat.JSON)
        .build();

    PushSourceRunner runner = new PushSourceRunner.Builder(HdfsDSource.class, source)
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .addOutputLane("lane")
        .build();

    AtomicInteger batchCount = new AtomicInteger(0);
    final List<Record> records = Collections.synchronizedList(new ArrayList<>(NUM_FILES * NUM_RECORDS));
    final int maxBatchSize = NUM_RECORDS;

    runner.runInit();

    final String initialFileName = TEST_FOLDER_ROOT + "/" + String.format(FILENAME_TEMPLATE, NUM_FILES-2);
    Map<String, String> initialOffset = ImmutableMap.of(initialFileName, "0");

    try {
      runner.runProduce(initialOffset, maxBatchSize, output -> {
        batchCount.incrementAndGet();

        synchronized (records) {
          records.addAll(output.getRecords().get("lane"));
        }

        runner.setStop();
      });
      runner.waitOnProduce();
      Assert.assertEquals(1, batchCount.get());
      Assert.assertEquals(NUM_RECORDS, records.size());
      Assert.assertTrue("Offset does not contain " + initialFileName, runner.getOffsets().containsKey(initialFileName));
    } finally {
      runner.runDestroy();
    }
  }

  @Test(expected = ExecutionException.class)
  public void testDataParserFailure() throws Exception {
    // create corrupted json file
    final String fileName = "0.json";
    String dirpath = "";
    try {
      File dir = testFolder.newFolder();

      dirpath = dir.getAbsolutePath();
      File file = new File(dir.getAbsoluteFile() + "/0.json");
      file.createNewFile();
      FileOutputStream outputStream = new FileOutputStream(file);

      for (int index =0; index < NUM_RECORDS; index++) {
        final String jsonString = String.format("test\n");
        outputStream.write(jsonString.getBytes());
      }
    } catch (IOException ex) {
      LOG.error("failed to setup test file: {}", ex.toString(), ex);
      Assert.fail("failed to setup test file");
    }

    HdfsSource source = new TestHdfsSourceBuilder()
        .dirPathTemplate(dirpath)
        .dataFormat(DataFormat.JSON)
        .build();

    PushSourceRunner runner = new PushSourceRunner.Builder(HdfsDSource.class, source)
        .setOnRecordError(OnRecordError.DISCARD)
        .addOutputLane("lane")
        .build();

    AtomicInteger batchCount = new AtomicInteger(0);
    final List<Record> records = Collections.synchronizedList(new ArrayList<>(NUM_FILES * NUM_RECORDS));
    final int maxBatchSize = NUM_RECORDS;

    runner.runInit();

    try {
      runner.runProduce(ImmutableMap.of(TEST_FOLDER_ROOT + "/" + fileName, "0"), maxBatchSize, output -> {
        batchCount.incrementAndGet();

        synchronized (records) {
          records.addAll(output.getRecords().get("lane"));
        }

        if (batchCount.get() > 10) {
          runner.setStop();
        }
      });

      runner.waitOnProduce();
      Assert.assertEquals("0", runner.getOffsets().get(fileName));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testEmptyBatchWithInitialOffset() throws Exception {
    // setup the sample data
    HdfsSource source = new TestHdfsSourceBuilder()
        .dirPathTemplate(TEST_FOLDER_ROOT)
        .dataFormat(DataFormat.JSON)
        .poolingTimeoutSecs(1)
        .numberOfThreads(5)
        .build();

    PushSourceRunner runner = new PushSourceRunner.Builder(HdfsDSource.class, source)
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .addOutputLane("lane")
        .build();

    AtomicInteger batchCount = new AtomicInteger(0);
    final List<Record> records = Collections.synchronizedList(new ArrayList<>(NUM_FILES * NUM_RECORDS));
    final int maxBatchSize = NUM_RECORDS;

    runner.runInit();

    final String initialFileName = TEST_FOLDER_ROOT + "/" + String.format(FILENAME_TEMPLATE, NUM_FILES - 2);
    // start with 5th line of the file
    Map<String, String> initialOffset = ImmutableMap.of(initialFileName, "45");

    try {
      runner.runProduce(initialOffset, maxBatchSize, output -> {
        batchCount.incrementAndGet();

        synchronized (records) {
          records.addAll(output.getRecords().get("lane"));
        }

        if (batchCount.get() > 4) {
          runner.setStop();
        }
      });
      runner.waitOnProduce();

      Assert.assertEquals(15, records.size());
      String file = TEST_FOLDER_ROOT + "/" + "test-9.json";
      Assert.assertTrue("Offset does not contain " + file, runner.getOffsets().containsKey(file));
      Assert.assertTrue("offset should be -1: " + runner.getOffsets().get(file), runner.getOffsets().get(file).equals("-1"));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testWithEndOfInitialOffset() throws Exception {
    // setup the sample data
    HdfsSource source = new TestHdfsSourceBuilder()
        .dirPathTemplate(TEST_FOLDER_ROOT)
        .dataFormat(DataFormat.JSON)
        .poolingTimeoutSecs(1)
        .numberOfThreads(1)
        .build();

    PushSourceRunner runner = new PushSourceRunner.Builder(HdfsDSource.class, source)
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .addOutputLane("lane")
        .build();

    AtomicInteger batchCount = new AtomicInteger(0);
    final List<Record> records = Collections.synchronizedList(new ArrayList<>(NUM_FILES * NUM_RECORDS));
    final int maxBatchSize = NUM_RECORDS;

    runner.runInit();

    final String initialFileName = String.format(FILENAME_TEMPLATE, NUM_FILES-2);
    Map<String, String> initialOffset = ImmutableMap.of(TEST_FOLDER_ROOT + "/" + initialFileName, "-1");

    try {
      runner.runProduce(initialOffset, maxBatchSize, output -> {
        batchCount.incrementAndGet();

        synchronized (records) {
          records.addAll(output.getRecords().get("lane"));
        }

        if (batchCount.get() > 1) {
          runner.setStop();
        }
      });
      runner.waitOnProduce();

      Assert.assertEquals(NUM_RECORDS, records.size());
      String file = TEST_FOLDER_ROOT + "/" + "test-9.json";
      Assert.assertTrue("Offset does not contain " + file, runner.getOffsets().containsKey(file));
      Assert.assertTrue("offset should be -1: " + runner.getOffsets().get(file), runner.getOffsets().get(file).equals("-1"));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testWithDifferentBatch() throws Exception {
    // setup the sample data
    HdfsSource source = new TestHdfsSourceBuilder()
        .dirPathTemplate(TEST_FOLDER_ROOT)
        .dataFormat(DataFormat.JSON)
        .poolingTimeoutSecs(1)
        .maxBatchSize(3)
        .numberOfThreads(5)
        .build();

    PushSourceRunner runner = new PushSourceRunner.Builder(HdfsDSource.class, source)
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .addOutputLane("lane")
        .build();

    AtomicInteger batchCount = new AtomicInteger(0);
    final List<Record> records = Collections.synchronizedList(new ArrayList<>(NUM_FILES * NUM_RECORDS));
    final int maxBatchSize = NUM_RECORDS;

    runner.runInit();

    try {
      runner.runProduce(new HashMap<>(), maxBatchSize, output -> {
        batchCount.incrementAndGet();

        synchronized (records) {
          records.addAll(output.getRecords().get("lane"));
        }

        if (batchCount.get() > 10) {
          runner.setStop();
        }
      });
      runner.waitOnProduce();

      Assert.assertEquals(NUM_RECORDS * NUM_RECORDS, records.size());
      String file = TEST_FOLDER_ROOT + "/test-9.json";
      Assert.assertTrue("Offset does not contain " + file, runner.getOffsets().containsKey(file));
      Assert.assertTrue("offset should be -1: " + runner.getOffsets().get(file), runner.getOffsets().get(file).equals("-1"));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testWithFirstFile() throws Exception {
    HdfsSource source = new TestHdfsSourceBuilder()
        .dirPathTemplate(TEST_FOLDER_ROOT)
        .firstFile(TEST_FOLDER_ROOT + "/test-9.json")
        .dataFormat(DataFormat.JSON)
        .poolingTimeoutSecs(1)
        .maxBatchSize(3)
        .numberOfThreads(5)
        .build();

    PushSourceRunner runner = new PushSourceRunner.Builder(HdfsDSource.class, source)
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .addOutputLane("lane")
        .build();

    AtomicInteger batchCount = new AtomicInteger(0);
    final List<Record> records = Collections.synchronizedList(new ArrayList<>(NUM_FILES * NUM_RECORDS));
    final int maxBatchSize = NUM_RECORDS;

    runner.runInit();

    try {
      runner.runProduce(new HashMap<>(), maxBatchSize, output -> {
        batchCount.incrementAndGet();

        synchronized (records) {
          records.addAll(output.getRecords().get("lane"));
        }

        if (batchCount.get() > 3) {
          runner.setStop();
        }
      });
      runner.waitOnProduce();

      Assert.assertEquals(NUM_RECORDS, records.size());
      String file = TEST_FOLDER_ROOT + "/test-9.json";
      Assert.assertTrue("Offset does not contain " + file, runner.getOffsets().containsKey(file));
      Assert.assertTrue("offset should be -1: " + runner.getOffsets().get(file), runner.getOffsets().get(file).equals("-1"));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testWithMultipleInitialOffset() throws Exception {
    final int numofThreads = 1;
    HdfsSource source = new TestHdfsSourceBuilder()
        .dirPathTemplate(TEST_FOLDER_ROOT)
        .dataFormat(DataFormat.JSON)
        .poolingTimeoutSecs(1)
        .maxBatchSize(3)
        .numberOfThreads(numofThreads)
        .build();

    PushSourceRunner runner = new PushSourceRunner.Builder(HdfsDSource.class, source)
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .addOutputLane("lane")
        .build();

    AtomicInteger batchCount = new AtomicInteger(0);
    final List<Record> records = Collections.synchronizedList(new ArrayList<>(NUM_FILES * NUM_RECORDS));
    final int maxBatchSize = NUM_RECORDS;

    runner.runInit();

    Map<String, String> initialOffset = ImmutableMap.of(TEST_FOLDER_ROOT + "/test-0.json", "-1", TEST_FOLDER_ROOT + "/test-1.json", "45", TEST_FOLDER_ROOT + "/test-2.json", "-1");

    try {
      runner.runProduce(initialOffset, maxBatchSize, output -> {
        batchCount.incrementAndGet();

        synchronized (records) {
          records.addAll(output.getRecords().get("lane"));
        }

        if (batchCount.get() > 10) {
          runner.setStop();
        }
      });
      runner.waitOnProduce();

      Assert.assertEquals(75, records.size());
      String file = TEST_FOLDER_ROOT + "/test-9.json";
      Assert.assertTrue("Offset does not contain " + file, runner.getOffsets().containsKey(file));
      Assert.assertTrue("offset should be -1: " + runner.getOffsets().get(file), runner.getOffsets().get(file).equals("-1"));
      Assert.assertEquals(numofThreads+1, runner.getOffsets().size());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testWithGlobPath() throws Exception {
    // setup the sample data
    final List<String> folderPaths = ImmutableList.of("2018-02-01", "2018-02-02", "2018-02-03");

    int i = 0;
    for (String folderPath : folderPaths) {
      testFolder.newFolder(folderPath);
      File file = testFolder.newFile(folderPath + "/test.json");
      FileOutputStream outputStream = new FileOutputStream(file);

      for (int index = NUM_RECORDS * i; index < NUM_RECORDS * (i + 1); index++) {
        final String jsonString = String.format("{\"a\":%s}\n", index);
        outputStream.write(jsonString.getBytes());
      }
      i++;
    }

    HdfsSource source = new TestHdfsSourceBuilder()
        .dirPathTemplate(TEST_FOLDER_ROOT + "/*-*-*")
        .dataFormat(DataFormat.JSON)
        .poolingTimeoutSecs(1)
        .maxBatchSize(100)
        .numberOfThreads(3)
        .build();

    PushSourceRunner runner = new PushSourceRunner.Builder(HdfsDSource.class, source)
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .addOutputLane("lane")
        .build();

    AtomicInteger batchCount = new AtomicInteger(0);
    final List<Record> records = Collections.synchronizedList(new ArrayList<>(NUM_FILES * NUM_RECORDS));
    final int maxBatchSize = NUM_RECORDS;

    runner.runInit();

    try {
      runner.runProduce(new HashMap<>(), maxBatchSize, output -> {
        batchCount.incrementAndGet();

        synchronized (records) {
          records.addAll(output.getRecords().get("lane"));
        }

        if (batchCount.get() > 5) {
          runner.setStop();
        }
      });
      runner.waitOnProduce();

      Assert.assertEquals(folderPaths.size() * NUM_RECORDS, records.size());
      String file = TEST_FOLDER_ROOT + "/2018-02-03/test.json";
      Assert.assertTrue("Offset does not contain " + file, runner.getOffsets().containsKey(file));
      Assert.assertTrue("offset should be -1: " + runner.getOffsets().get(file), runner.getOffsets().get(file).equals("-1"));
    } finally {
      runner.runDestroy();
    }
  }
}
