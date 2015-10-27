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
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.stage.destination.hdfs.util.HdfsTargetUtil;
import com.streamsets.pipeline.stage.destination.hdfs.writer.ActiveRecordWriters;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class TestHdfsTarget {
  private static String testDir;

  @BeforeClass
  public static void setUpClass() {
    File dir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(dir.mkdirs());
    testDir = dir.getAbsolutePath();
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
        dataGeneratorFormatConfig
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
      dataGeneratorFormatConfig
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
        dataGeneratorFormatConfig
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
        "nonabsolutedir",
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
        dataGeneratorFormatConfig
    );

    TargetRunner runner = new TargetRunner.Builder(HdfsDTarget.class, hdfsTarget)
      .setOnRecordError(OnRecordError.STOP_PIPELINE)
      .build();

    Assert.assertFalse(runner.runValidateConfigs().isEmpty());
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
      dataGeneratorFormatConfig
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
}
