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
package com.streamsets.pipeline.stage.destination.hdfs;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.lineage.LineageEvent;
import com.streamsets.pipeline.api.lineage.LineageEventType;
import com.streamsets.pipeline.api.lineage.LineageSpecificAttribute;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.lib.hdfs.common.Errors;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.stage.destination.hdfs.util.HdfsTargetUtil;
import com.streamsets.pipeline.stage.destination.hdfs.writer.ActiveRecordWriters;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TestHdfsTarget {
  private String testDir;

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    File dir = new File("target/TestHdfsTarget", testName.getMethodName()).getAbsoluteFile();
    if(dir.exists()) {
      FileUtils.deleteDirectory(dir);
    }
    Assert.assertTrue(dir.mkdirs());
    testDir = dir.getAbsolutePath();
  }

  private String getTestDir() {
    return testDir;
  }

  @Test
  public void testTarget() throws Exception {
    HdfsTarget hdfsTarget = HdfsTargetUtil.newBuilder()
      .dirPathTemplate(getTestDir() + "/hdfs/${YYYY()}${MM()}${DD()}${hh()}${mm()}${record:value('/a')}")
      .timeDriver("${record:value('/time')}")
      .lateRecordsLimit("${30 * MINUTES}")
      .lateRecordsAction(LateRecordsAction.SEND_TO_LATE_RECORDS_FILE)
      .build();

    TargetRunner runner = new TargetRunner.Builder(HdfsDTarget.class, hdfsTarget)
      .setOnRecordError(OnRecordError.STOP_PIPELINE)
      .build();

    runner.runInit();
    List<Record> records = new ArrayList<>();
    Record record = RecordCreator.create();
    Map<String, Field> map = new HashMap<>();
    map.put("time", Field.createDatetime(new Date()));
    map.put("a", Field.create("x"));
    record.set(Field.create(map));
    records.add(record);

    record = RecordCreator.create();
    map.put("a", Field.create("y"));
    record.set(Field.create(map));
    records.add(record);

    record = RecordCreator.create();
    map = new HashMap<>();
    map.put("time", Field.createDatetime(new Date(System.currentTimeMillis() - 1 * 60 * 1000)));
    map.put("a", Field.create("x"));
    record.set(Field.create(map));
    records.add(record);

    record = RecordCreator.create();
    map = new HashMap<>();
    map.put("time", Field.createDatetime(new Date(System.currentTimeMillis() - 2 * 60 * 1000)));
    map.put("a", Field.create("x"));
    record.set(Field.create(map));
    records.add(record);

    runner.runWrite(records);
    runner.runDestroy();

    Assert.assertEquals(4, runner.getEventRecords().size());
    Iterator<EventRecord> eventRecordIterator = runner.getEventRecords().iterator();

    while (eventRecordIterator.hasNext()) {
      Record eventRecord = eventRecordIterator.next();

      String type = eventRecord.getHeader().getAttribute("sdc.event.type");
      Assert.assertEquals("file-closed", type);

      Assert.assertTrue(eventRecord.has("/filepath"));
      Assert.assertTrue(eventRecord.has("/filename"));
      Assert.assertTrue(eventRecord.has("/length"));
    }
  }

  @Test
  public void testOnlyConfDirectory() throws Exception {
    // Create custom core-site.xml
    Configuration configuration = new Configuration();
    configuration.clear();
    configuration.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, "file:///");
    FileOutputStream configOut = FileUtils.openOutputStream(new File(getTestDir() + "/conf-dir/core-site.xml"));
    configuration.writeXml(configOut);
    configOut.close();

    HdfsTarget hdfsTarget = HdfsTargetUtil.newBuilder()
      .hdfsUri("")
      .hdfsConfDir(getTestDir() + "/conf-dir/")
      .build();

    TargetRunner runner = new TargetRunner.Builder(HdfsDTarget.class, hdfsTarget)
      .setOnRecordError(OnRecordError.STOP_PIPELINE)
      .build();

    runner.runInit();

    // The configuration object should have the FS config from core-site.xml
    Assert.assertEquals("file:///", hdfsTarget.getHdfsConfiguration().get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY));

    runner.runDestroy();
  }

  @Test
  public void testNoUriOrConfDirectory() throws Exception {
    HdfsTarget hdfsTarget = HdfsTargetUtil.newBuilder()
        .hdfsUri("")
        .hdfsConfDir("")
        .build();

    TargetRunner runner = new TargetRunner.Builder(HdfsDTarget.class, hdfsTarget)
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .build();

    List<Stage.ConfigIssue> configIssues = runner.runValidateConfigs();
    Assert.assertEquals(1, configIssues.size());
    Assert.assertTrue(configIssues.get(0).toString().contains(Errors.HADOOPFS_61.name()));
  }

  @Test
  public void testCutoffLimitUnitConversion() throws Exception {
    HdfsTarget hdfsTarget = HdfsTargetUtil.newBuilder()
      .maxFileSize(1)
      .lateRecordsAction(LateRecordsAction.SEND_TO_LATE_RECORDS_FILE)
      .dirPathTemplate(getTestDir() + "/hdfs/${YYYY()}${MM()}${DD()}${hh()}${mm()}${record:value('/a')}")
      .lateRecordsDirPathTemplate(getTestDir() + "/hdfs/${YYYY()}")
      .build();

    TargetRunner runner = new TargetRunner.Builder(HdfsDTarget.class, hdfsTarget)
      .setOnRecordError(OnRecordError.STOP_PIPELINE)
      .build();

    runner.runInit();
    try {
      Assert.assertEquals(1024 * 1024,
        ((HdfsTarget) runner.getStage()).getCurrentWriters().getWriterManager().getCutOffSizeBytes());
      Assert.assertEquals(1024 * 1024,
        ((HdfsTarget) runner.getStage()).getLateWriters().getWriterManager().getCutOffSizeBytes());
    } finally {
      runner.runDestroy();
    }
  }

  @Ignore
  @Test
  public void testEmptyBatch() throws Exception {
    HdfsTarget hdfsTarget = HdfsTargetUtil.newBuilder()
      .maxFileSize(1)
      .maxRecordsPerFile(1)
      .dirPathTemplate(getTestDir() + "/hdfs/${YYYY()}${MM()}${DD()}${hh()}${mm()}")
      .build();

    TargetRunner runner = new TargetRunner.Builder(HdfsDTarget.class, hdfsTarget)
      .setOnRecordError(OnRecordError.STOP_PIPELINE)
      .build();

    runner.runInit();
    try {
      ActiveRecordWriters activeWriters = ((HdfsTarget)runner.getStage()).getCurrentWriters();

      Assert.assertEquals(0, activeWriters.getActiveWritersCount());
      runner.runWrite(ImmutableList.of(RecordCreator.create()));
      Assert.assertEquals(1, activeWriters.getActiveWritersCount());
      Thread.sleep(2100);
      runner.runWrite(Collections.<Record>emptyList());
      Assert.assertEquals(0, activeWriters.getActiveWritersCount());
    } finally {
      runner.runDestroy();
    }
  }

  /**
    If directory path is relative path, it raises an issue with error code HADOOPFS_40
   */
  @Test
  public void testInvalidDirValidation() throws Exception {
    HdfsTarget hdfsTarget = HdfsTargetUtil.newBuilder()
      .dirPathTemplate("invalid-directory")
      .build();

    TargetRunner runner = new TargetRunner.Builder(HdfsDTarget.class, hdfsTarget)
      .setOnRecordError(OnRecordError.STOP_PIPELINE)
      .build();

    List<Stage.ConfigIssue> configIssues = runner.runValidateConfigs();
    Assert.assertEquals(1, configIssues.size());
    Assert.assertTrue(configIssues.get(0).toString().contains(Errors.HADOOPFS_40.name()));
  }

  /**
    If late record directory is "SEND TO ERROR", we don't do validation on late record directory.
    This test should't raise an config issue.
  */
  @Test
  public void testNoLateRecordsDirValidation() throws Exception {
    HdfsTarget hdfsTarget = HdfsTargetUtil.newBuilder()
      .dirPathTemplate(getTestDir() + "/hdfs/${YYYY()}${MM()}${DD()}${hh()}${mm()}")
      .lateRecordsAction(LateRecordsAction.SEND_TO_ERROR)
      .lateRecordsDirPathTemplate("relative-and-thus-invalid")
      .build();

    TargetRunner runner = new TargetRunner.Builder(HdfsDTarget.class, hdfsTarget)
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .build();

    Assert.assertTrue(runner.runValidateConfigs().isEmpty());
  }

  /**
    If late record action is SEND_TO_LATE_RECORDS_FILE, we should do validation on the directory path.
  */
  @Test
  public void testLateRecordsDirValidation() throws Exception {
    HdfsTarget hdfsTarget = HdfsTargetUtil.newBuilder()
      .dirPathTemplate(getTestDir() + "/hdfs/${YYYY()}${MM()}${DD()}${hh()}${mm()}")
      .lateRecordsAction(LateRecordsAction.SEND_TO_LATE_RECORDS_FILE)
      .lateRecordsDirPathTemplate(getTestDir() + "/late/${TEST}") // Unknown constant
      .build();

    TargetRunner runner = new TargetRunner.Builder(HdfsDTarget.class, hdfsTarget)
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .build();

    List<Stage.ConfigIssue> configIssues = runner.runValidateConfigs();
    Assert.assertEquals(1, configIssues.size());
    // since ${TEST} cannot be resolved, this test should raise ELException
    Assert.assertTrue(configIssues.get(0).toString().contains(Errors.HADOOPFS_20.name()));
  }

  /**
    Test validation on both output and late record directory. Testing ELs and relative path, and
    both should fail.
  */
  @Test
  public void testDirValidationFailure() throws Exception {
    HdfsTarget hdfsTarget = HdfsTargetUtil.newBuilder()
      .dirPathTemplate(getTestDir() + "${TEST}")
      .lateRecordsAction(LateRecordsAction.SEND_TO_LATE_RECORDS_FILE)
      .lateRecordsDirPathTemplate("relative-and-thus-invalid")
      .build();

    TargetRunner runner = new TargetRunner.Builder(HdfsDTarget.class, hdfsTarget)
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .build();

    List<Stage.ConfigIssue> configIssues = runner.runValidateConfigs();
    Assert.assertEquals(2, configIssues.size());
    // output directory should cause HADOOPFS_20 since we didn't set constant value for ${TEST}
    Assert.assertTrue(configIssues.get(0).toString().contains(Errors.HADOOPFS_20.name()));
    // late record directory should cause HADOOPFS_40 since it is relative path
    Assert.assertTrue(configIssues.get(1).toString().contains(Errors.HADOOPFS_40.name()));
  }

  @Test
  public void testClusterModeHadoopConfDirAbsPath() {
    HdfsTarget hdfsTarget = HdfsTargetUtil.newBuilder()
      .hdfsConfDir(testDir)
      .build();

    TargetRunner runner = new TargetRunner.Builder(HdfsDTarget.class, hdfsTarget)
      .setOnRecordError(OnRecordError.STOP_PIPELINE)
      .setExecutionMode(ExecutionMode.CLUSTER_BATCH)
      .build();

    try {
      runner.runInit();
      Assert.fail(Utils.format("Expected StageException as absolute hdfsConfDir path '{}' is specified in cluster mode",
        testDir));
    } catch (StageException e) {
      Assert.assertTrue(e.getMessage().contains("HADOOPFS_45"));
    }
  }

  @Test
  public void testIdleTimeoutEventRecordsLineageEvents() throws Exception {
    HdfsTarget hdfsTarget = HdfsTargetUtil.newBuilder()
        .dirPathTemplate(getTestDir() + "/hdfs/${YYYY()}${MM()}${DD()}")
        .idleTimeout("1")
        .build();

    TargetRunner runner = new TargetRunner.Builder(HdfsDTarget.class, hdfsTarget)
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .build();

    runner.runInit();
    List<Record> records = new ArrayList<>();
    Record record = RecordCreator.create();
    Map<String, Field> map = new HashMap<>();
    map.put("time", Field.createDatetime(new Date()));
    map.put("a", Field.create("x"));
    record.set(Field.create(map));
    records.add(record);
    runner.runWrite(ImmutableList.copyOf(new Record[]{record}));

    // Idle for 1500 milliseconds - should create a new file.
    Thread.sleep(1500);
    record = RecordCreator.create();
    map.put("a", Field.create("y"));
    record.set(Field.create(map));
    records.add(record);
    runner.runWrite(ImmutableList.copyOf(new Record[]{record}));

    // Idle for 500 milliseconds - no new file
    Thread.sleep(500);
    record = RecordCreator.create();
    map = new HashMap<>();
    map.put("time", Field.createDatetime(new Date(System.currentTimeMillis() - 1 * 60 * 1000)));
    map.put("a", Field.create("x"));
    record.set(Field.create(map));
    records.add(record);
    runner.runWrite(ImmutableList.copyOf(new Record[]{record}));

    // Idle for 1500 milliseconds - one more file.
    Thread.sleep(1500);
    record = RecordCreator.create();
    map = new HashMap<>();
    map.put("time", Field.createDatetime(new Date(System.currentTimeMillis() - 2 * 60 * 1000)));
    map.put("a", Field.create("x"));
    record.set(Field.create(map));
    records.add(record);
    runner.runWrite(ImmutableList.copyOf(new Record[]{record}));

    runner.runDestroy();

    File[] list = (new File(getTestDir() + "/hdfs/").listFiles())[0].listFiles();
    for (File f: list) {
      System.out.print(f.getName());
    }

    List<LineageEvent> lineageEvents = runner.getLineageEvents();
    Assert.assertEquals(3, lineageEvents.size());
    for(LineageEvent event : lineageEvents) {
      Assert.assertEquals(LineageEventType.ENTITY_CREATED.name(), event.getEventType().name());
      boolean matched = false;
      for(File f : list) {
        if (f.getAbsolutePath().equals(event.getSpecificAttribute(LineageSpecificAttribute.ENTITY_NAME))) {
          matched = true;
          break;
        }
      }
      if (!matched) {
        Assert.fail("LineageEvent: ENTITY_NAME is not in the list of files.");
      }
    }

    Assert.assertEquals(3, list.length);
    Assert.assertEquals(3, runner.getEventRecords().size());
  }

  /**
   * Verifies normal behavior when target directory is in the header.
   */
  @Test
  public void testDirectoryTemplateInHeader() throws Exception {
    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.jsonMode = JsonMode.MULTIPLE_OBJECTS;

    HdfsTarget hdfsTarget = HdfsTargetUtil.newBuilder()
      .dataGeneratorFormatConfig(dataGeneratorFormatConfig)
      .dirPathTemplateInHeader(true)
      .dirPathTemplate(null)
      .dataForamt(DataFormat.JSON)
      .build();

    TargetRunner runner = new TargetRunner.Builder(HdfsDTarget.class, hdfsTarget)
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .build();
    runner.runInit();

    Record record = RecordCreator.create();
    record.getHeader().setAttribute(HdfsTarget.TARGET_DIRECTORY_HEADER, getTestDir() + "/hdfs/");
    Map<String, Field> map = new HashMap<>();
    map.put("a", Field.create("x"));
    record.set(Field.create(map));

    runner.runWrite(ImmutableList.copyOf(new Record[]{record}));

    runner.runDestroy();

    File[] list = new File(getTestDir() + "/hdfs/").listFiles();
    Assert.assertEquals(1, list.length);
    Assert.assertEquals("{\"a\":\"x\"}", FileUtils.readFileToString(list[0], Charset.defaultCharset()));
    Assert.assertEquals(1, runner.getEventRecords().size());
  }

  /**
   * Verifies normal behavior when target directory is in the header.
   */
  @Test
  public void testHdfsDelimitedDataFormat() throws Exception {
    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.csvFileFormat = CsvMode.CSV;
    dataGeneratorFormatConfig.csvReplaceNewLines = true;
    dataGeneratorFormatConfig.csvReplaceNewLinesString = " ";
    dataGeneratorFormatConfig.csvHeader = CsvHeader.NO_HEADER;

    HdfsTarget hdfsTarget = HdfsTargetUtil.newBuilder()
      .dataGeneratorFormatConfig(dataGeneratorFormatConfig)
      .dirPathTemplateInHeader(true)
      .dataForamt(DataFormat.DELIMITED)
      .build();

    TargetRunner runner = new TargetRunner.Builder(HdfsDTarget.class, hdfsTarget)
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .build();
    runner.runInit();

    Record record = RecordCreator.create();
    record.getHeader().setAttribute(HdfsTarget.TARGET_DIRECTORY_HEADER, getTestDir() + "/hdfs/");
    LinkedHashMap<String, Field> list = new LinkedHashMap<>();
    list.put("x", Field.create("x\nz"));
    list.put("y", Field.create("y"));
    record.set(Field.createListMap(list));

    runner.runWrite(ImmutableList.copyOf(new Record[]{record}));

    runner.runDestroy();

    File[] fileList = new File(getTestDir() + "/hdfs/").listFiles();
    Assert.assertEquals(1, fileList.length);
    Assert.assertEquals("x z,y\r\n", FileUtils.readFileToString(fileList[0], Charset.defaultCharset()));
  }

  /**
   * Records without expected header needs to be propagated to error output.
   */
  @Test
  public void testDirectoryTemplateInHeaderMissingHeader() throws Exception {
    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.jsonMode = JsonMode.MULTIPLE_OBJECTS;

    HdfsTarget hdfsTarget = HdfsTargetUtil.newBuilder()
      .dataGeneratorFormatConfig(dataGeneratorFormatConfig)
      .dirPathTemplateInHeader(true)
      .dirPathTemplate(null)
      .dataForamt(DataFormat.JSON)
      .build();

    TargetRunner runner = new TargetRunner.Builder(HdfsDTarget.class, hdfsTarget)
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .build();
    runner.runInit();

    Record record = RecordCreator.create();
    Map<String, Field> map = new HashMap<>();
    map.put("a", Field.create("x"));
    record.set(Field.create(map));

    runner.runWrite(ImmutableList.copyOf(new Record[]{record}));
    runner.runDestroy();

    List<Record> errorRecords = runner.getErrorRecords();
    Assert.assertNotNull(errorRecords);
    Assert.assertEquals(1, errorRecords.size());
  }

  @Test
  public void testForcedRollOutByHeaderWithInvalidHeaderConfiguration() throws Exception {
    for(String headerValue : Arrays.asList("", null)) {
      HdfsTarget hdfsTarget = HdfsTargetUtil.newBuilder()
        .rollIfHeader(true)
        .rollHeaderName(headerValue)
        .build();

      TargetRunner runner = new TargetRunner.Builder(HdfsDTarget.class, hdfsTarget)
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .build();

      List<Stage.ConfigIssue> configIssues = runner.runValidateConfigs();
      Assert.assertEquals(1, configIssues.size());
      Assert.assertTrue(configIssues.get(0).toString().contains(Errors.HADOOPFS_51.name()));
    }
  }

  @Test
  public void testForcedRollOutByHeader() throws Exception {
    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.jsonMode = JsonMode.MULTIPLE_OBJECTS;

    HdfsTarget hdfsTarget = HdfsTargetUtil.newBuilder()
      .dirPathTemplate(getTestDir() + "/hdfs/")
      .dataGeneratorFormatConfig(dataGeneratorFormatConfig)
      .dataForamt(DataFormat.JSON)
      .rollIfHeader(true)
      .rollHeaderName("roll")
      .build();

    TargetRunner runner = new TargetRunner.Builder(HdfsDTarget.class, hdfsTarget)
      .setOnRecordError(OnRecordError.STOP_PIPELINE)
      .build();

    runner.runInit();

    Record record = RecordCreator.create();
    Map<String, Field> map = new HashMap<>();
    map.put("a", Field.create("x"));
    record.set(Field.create(map));

    // First write should open a new file
    runner.runWrite(ImmutableList.copyOf(new Record[]{record}));

    // Second write should also open a new file (forcibly)
    record.getHeader().setAttribute("roll", "true");
    runner.runWrite(ImmutableList.copyOf(new Record[]{record}));

    runner.runDestroy();

    File[] list = new File(getTestDir() + "/hdfs/").listFiles();
    Assert.assertEquals(2, list.length);
    Assert.assertEquals(2, runner.getEventRecords().size());

    for(File file : list) {
      Assert.assertFalse("The file wasn't renamed after close: " + file.getName(), file.getName().contains("_tmp_"));
      Assert.assertEquals("{\"a\":\"x\"}", FileUtils.readFileToString(file, Charset.defaultCharset()));
    }
  }

  // SDC-3418
  @Test
  public void testDoNotCreateEmptyDirectoryOnInit() throws Exception {
    HdfsTarget hdfsTarget = HdfsTargetUtil.newBuilder()
      .dirPathTemplate(getTestDir() + "/hdfs/${record:attribute('key')}/a/b/c}")
      .timeDriver("${time:now()}")
      .lateRecordsAction(LateRecordsAction.SEND_TO_LATE_RECORDS_FILE)
      .build();

    TargetRunner runner = new TargetRunner.Builder(HdfsDTarget.class, hdfsTarget)
      .setOnRecordError(OnRecordError.STOP_PIPELINE)
      .build();

    runner.runInit();

    File targetDirectory = new File(getTestDir() + "/hdfs/a/b/c");
    Assert.assertFalse(targetDirectory.exists());
  }
}
