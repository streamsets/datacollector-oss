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
package com.streamsets.pipeline.stage.destination.hdfs;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.stage.destination.hdfs.util.HdfsTargetUtil;
import com.streamsets.pipeline.stage.destination.hdfs.writer.ActiveRecordWriters;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

public class TestHdfsTarget {
  private String testDir;

  @Before
  public void setUp() {
    File dir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(dir.mkdirs());
    testDir = dir.getAbsolutePath();
  }

  @After
  public void after() {
    FileUtils.deleteQuietly(new File(testDir));
  }

  private String getTestDir() {
    return testDir;
  }

  @Test
  public void testTarget() throws Exception {

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();

    HdfsTarget hdfsTarget = HdfsTargetUtil.createHdfsTarget(
        "file:///",
        "foo",
        false,
        null,
        new HashMap<String, String>(),
        "foo",
        "UTC",
        getTestDir() + "/hdfs/${YYYY()}${MM()}${DD()}${hh()}${mm()}${record:value('/a')}",
        HdfsFileType.TEXT,
        "${uuid()}",
        CompressionMode.NONE,
        HdfsSequenceFileCompressionType.BLOCK,
        5,
        0,
        "${record:value('/time')}",
        "${30 * MINUTES}",
        LateRecordsAction.SEND_TO_LATE_RECORDS_FILE,
        "",
        DataFormat.SDC_JSON,
        dataGeneratorFormatConfig,
        null
    );

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
  }

  @Test
  public void testOnlyConfDirectory() throws Exception {
    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();

    // Create core-site.xml
    Configuration configuration = new Configuration();
    configuration.clear();
    configuration.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, "file:///");
    FileOutputStream configOut = FileUtils.openOutputStream(new File(getTestDir() + "/conf-dir/core-site.xml"));
    configuration.writeXml(configOut);
    configOut.close();

    HdfsTarget hdfsTarget = HdfsTargetUtil.createHdfsTarget(
        "",
        "foo",
        false,
        getTestDir() + "/conf-dir/",
        new HashMap<String, String>(),
        "foo",
        "UTC",
        getTestDir() + "/hdfs/${YYYY()}${MM()}${DD()}${hh()}${mm()}${record:value('/a')}",
        HdfsFileType.TEXT,
        "${uuid()}",
        CompressionMode.NONE,
        HdfsSequenceFileCompressionType.BLOCK,
        5,
        0,
        "${record:value('/time')}",
        "${30 * MINUTES}",
        LateRecordsAction.SEND_TO_LATE_RECORDS_FILE,
        "",
        DataFormat.SDC_JSON,
        dataGeneratorFormatConfig,
        null
    );

    TargetRunner runner = new TargetRunner.Builder(HdfsDTarget.class, hdfsTarget)
      .setOnRecordError(OnRecordError.STOP_PIPELINE)
      .build();

    runner.runInit();

    // The configuration object should have the FS config from core-site.xml
    Assert.assertEquals("file:///", hdfsTarget.getHdfsConfiguration().get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY));

    runner.runDestroy();
  }

  @Test
  public void testCutoffLimitUnitConversion() throws Exception {
    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();

    HdfsTarget hdfsTarget = HdfsTargetUtil.createHdfsTarget(
      "file:///",
      "foo",
      false,
      null,
      new HashMap<String, String>(),
      "foo",
      "UTC",
      getTestDir() + "/hdfs/${YYYY()}${MM()}${DD()}${hh()}${mm()}${record:value('/a')}",
      HdfsFileType.TEXT,
      "${uuid()}",
      CompressionMode.NONE,
      HdfsSequenceFileCompressionType.BLOCK,
      1,
      1,
      "${record:value('/time')}",
      "${30 * MINUTES}",
      LateRecordsAction.SEND_TO_LATE_RECORDS_FILE,
      getTestDir() + "/hdfs/${YYYY()}",
      DataFormat.SDC_JSON,
      dataGeneratorFormatConfig,
      null
    );

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
    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();

    HdfsTarget hdfsTarget = HdfsTargetUtil.createHdfsTarget(
        "file:///",
        "foo",
        false,
        null,
        new HashMap<String, String>(),
        "foo",
        "UTC",
        getTestDir() + "/hdfs/${YYYY()}${MM()}${DD()}${hh()}${mm()}${ss()}",
        HdfsFileType.TEXT,
        "${uuid()}",
        CompressionMode.NONE,
        HdfsSequenceFileCompressionType.BLOCK,
        1,
        1,
        "${time:now()}",
        "${1 * SECONDS}",
        LateRecordsAction.SEND_TO_ERROR,
        "",
        DataFormat.SDC_JSON,
        dataGeneratorFormatConfig,
        null
    );

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
    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();

    HdfsTarget hdfsTarget = HdfsTargetUtil.createHdfsTarget(
        "file:///",
        "foo",
        false,
        null,
        new HashMap<String, String>(),
        "foo",
        "UTC",
        "nonabsolutedir",   // relative path
        HdfsFileType.TEXT,
        "${uuid()}",
        CompressionMode.NONE,
        HdfsSequenceFileCompressionType.BLOCK,
        1,
        1,
        "${time:now()}",
        "${1 * SECONDS}",
        LateRecordsAction.SEND_TO_ERROR,
        "",
        DataFormat.SDC_JSON,
        dataGeneratorFormatConfig,
        null
    );

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
    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();

    HdfsTarget hdfsTarget = HdfsTargetUtil.createHdfsTarget(
        "file:///",
        "foo",
        false,
        null,
        new HashMap<String, String>(),
        "foo",
        "UTC",
        getTestDir() + "/hdfs/${YYYY()}${MM()}${DD()}${hh()}${mm()}${record:value('/a')}",
        HdfsFileType.TEXT,
        "${uuid()}",
        CompressionMode.NONE,
        HdfsSequenceFileCompressionType.BLOCK,
        5,
        0,
        "${record:value('/time')}",
        "${30 * MINUTES}",
        LateRecordsAction.SEND_TO_ERROR, // action should be SEND_TO_ERROR to skip validation
        "relative_path", // lateRecordsDirPathTemplate is relative path
        DataFormat.SDC_JSON,
        dataGeneratorFormatConfig,
        null
    );

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
    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();

    HdfsTarget hdfsTarget = HdfsTargetUtil.createHdfsTarget(
        "file:///",
        "foo",
        false,
        null,
        new HashMap<String, String>(),
        "foo",
        "UTC",
        getTestDir() + "/hdfs",
        HdfsFileType.TEXT,
        "${uuid()}",
        CompressionMode.NONE,
        HdfsSequenceFileCompressionType.BLOCK,
        5,
        0,
        "${record:value('/time')}",
        "${30 * MINUTES}",
        LateRecordsAction.SEND_TO_LATE_RECORDS_FILE, // action should be SEND_TO_LATE_RECORDS_FILE
        getTestDir() + "/late_record/${TEST}", // lateRecordsDirPathTemplate contains constant that we don't know
        DataFormat.SDC_JSON,
        dataGeneratorFormatConfig,
        null
    );

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
    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();


    HdfsTarget hdfsTarget = HdfsTargetUtil.createHdfsTarget(
        "file:///",
        "foo",
        false,
        null,
        new HashMap<String, String>(),
        "foo",
        "UTC",
        getTestDir() + "${TEST}", // output dir contains constant that we don't know
        HdfsFileType.TEXT,
        "${uuid()}",
        CompressionMode.NONE,
        HdfsSequenceFileCompressionType.BLOCK,
        5,
        0,
        "${record:value('/time')}",
        "${30 * MINUTES}",
        LateRecordsAction.SEND_TO_LATE_RECORDS_FILE, // action should be SEND_TO_LATE_RECORDS_FILE
        "relative/late_record",  // creating this dir should fail
        DataFormat.SDC_JSON,
        dataGeneratorFormatConfig,
        null
    );

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

  /**
    If dirPathTemplate(output directory) and late record directory contain Els,
    we first convert Els to constant values, and do the path validation.
  */
  @Test
  public void testDirWithELsValidation() throws Exception {
    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();

    HdfsTarget hdfsTarget = HdfsTargetUtil.createHdfsTarget(
        "file:///",
        "foo",
        false,
        null,
        new HashMap<String, String>(),
        "foo",
        "UTC",
        getTestDir() + "/hdfs/${YYYY()}-${MM()}/out",
        HdfsFileType.TEXT,
        "${uuid()}",
        CompressionMode.NONE,
        HdfsSequenceFileCompressionType.BLOCK,
        5,
        0,
        "${record:value('/time')}",
        "${30 * MINUTES}",
        LateRecordsAction.SEND_TO_LATE_RECORDS_FILE, // action should be SEND_TO_LATE_RECORDS_FILE
        getTestDir() + "/late_record/${YYYY()}-${MM()}", // lateRecordsDirPathTemplate contains Els
        DataFormat.SDC_JSON,
        dataGeneratorFormatConfig,
        null
    );

    TargetRunner runner = new TargetRunner.Builder(HdfsDTarget.class, hdfsTarget)
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .build();

    Assert.assertTrue(runner.runValidateConfigs().isEmpty());
    // Check if output dir and late record dir are created with the right path
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM");
    String date_format = dateFormat.format(new Date());
    File outDir = new File(testDir + "/hdfs/" + date_format + "/out");
    File lateRecordDir = new File(testDir + "/late_record/" + date_format);
    // Check if the output dir is created
    Assert.assertTrue(outDir.exists());
    // Check if the late record dir is created
    Assert.assertTrue(lateRecordDir.exists());
  }

  @Test
  public void testClusterModeHadoopConfDirAbsPath() {

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();

    HdfsTarget hdfsTarget = HdfsTargetUtil.createHdfsTarget(
      "file:///",
      "foo",
      false,
      testDir,
      new HashMap<String, String>(),
      "foo",
      "UTC",
      getTestDir() + "/hdfs/${YYYY()}${MM()}${DD()}${hh()}${mm()}${record:value('/a')}",
      HdfsFileType.TEXT,
      "${uuid()}",
      CompressionMode.NONE,
      HdfsSequenceFileCompressionType.BLOCK,
      5,
      0,
      "${record:value('/time')}",
      "${30 * MINUTES}",
      LateRecordsAction.SEND_TO_ERROR,
      "",
      DataFormat.SDC_JSON,
      dataGeneratorFormatConfig,
      null
    );

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
  public void testIdleTimeout() throws Exception {

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();

    HdfsTarget hdfsTarget = HdfsTargetUtil.createHdfsTarget(
        "file:///",
        "foo",
        false,
        null,
        new HashMap<String, String>(),
        "foo",
        "UTC",
        getTestDir() + "/hdfs/${YYYY()}${MM()}${DD()}",
        HdfsFileType.TEXT,
        "${uuid()}",
        CompressionMode.NONE,
        HdfsSequenceFileCompressionType.BLOCK,
        5,
        0,
        "${record:value('/time')}",
        "${30 * MINUTES}",
        LateRecordsAction.SEND_TO_LATE_RECORDS_FILE,
        "",
        DataFormat.SDC_JSON,
        dataGeneratorFormatConfig,
        "1"
    );

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
    Assert.assertEquals(3, list.length);
  }
}
