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
package com.streamsets.pipeline.stage.processor.hive;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.BaseHiveIT;
import com.streamsets.pipeline.stage.HiveMetadataProcessorBuilder;
import com.streamsets.pipeline.stage.PartitionConfigBuilder;
import com.streamsets.pipeline.stage.lib.hive.HiveMetastoreUtil;
import com.streamsets.pipeline.stage.lib.hive.typesupport.HiveType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class HiveMetadataProcessorIT extends BaseHiveIT {

  private ProcessorRunner getProcessorRunner(HiveMetadataProcessor processor) {
    return new ProcessorRunner.Builder(HiveMetadataDProcessor.class, processor)
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .addOutputLane("hdfs")
        .addOutputLane("hive")
        .build();
  }

  @Test
  public void testInitialization() throws Exception {
    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder().build();
    ProcessorRunner runner = getProcessorRunner(processor);

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    Assert.assertEquals(0, issues.size());
  }

  @Test
  public void testAllOutputsOnCompletelyColdStart() throws Exception {
    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder().build();
    ProcessorRunner runner = getProcessorRunner(processor);
    runner.runInit();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("name", Field.create(Field.Type.STRING, "Jarcec"));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));

    StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
    Assert.assertEquals(2, output.getRecords().get("hive").size());
    Assert.assertEquals(1, output.getRecords().get("hdfs").size());

    // Metadata records

    Record newTableRecord = output.getRecords().get("hive").get(0);
    Assert.assertNotNull(newTableRecord);
    Assert.assertEquals(1, newTableRecord.get("/version").getValueAsInteger());
    Assert.assertEquals("tbl", newTableRecord.get("/table").getValueAsString());
    Assert.assertEquals("TABLE", newTableRecord.get("/type").getValueAsString());

    Record newPartitionRecord = output.getRecords().get("hive").get(1);
    Assert.assertNotNull(newPartitionRecord);
    Assert.assertEquals(1, newPartitionRecord.get("/version").getValueAsInteger());
    Assert.assertEquals("tbl", newPartitionRecord.get("/table").getValueAsString());
    Assert.assertEquals("PARTITION", newPartitionRecord.get("/type").getValueAsString());

    // HDFS record

    Record hdfsRecord = output.getRecords().get("hdfs").get(0);
    Assert.assertNotNull(hdfsRecord);
    // The record should have "roll" set to true
    Assert.assertNotNull(hdfsRecord.getHeader().getAttribute("roll"));
    // Target directory with correct path
    Assert.assertEquals(
        "/user/hive/warehouse/tbl/dt=secret-value",
        hdfsRecord.getHeader().getAttribute("targetDirectory")
    );

    // Sending the same record second time should not generate any metadata outputs
    output = runner.runProcess(ImmutableList.of(record));
    Assert.assertEquals(0, output.getRecords().get("hive").size());
    Assert.assertEquals(1, output.getRecords().get("hdfs").size());
  }

  @Test
  public void testTableAlreadyExistsInHive() throws Exception {
    executeUpdate("CREATE TABLE `tbl` (name string) PARTITIONED BY (dt string) STORED AS AVRO");

    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder().build();

    ProcessorRunner runner = getProcessorRunner(processor);
    runner.runInit();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("name", Field.create(Field.Type.STRING, "Jarcec"));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));

    // First run should generate only one metadata record (new partition)
    StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
    Assert.assertEquals(1, output.getRecords().get("hive").size());
    Assert.assertEquals(1, output.getRecords().get("hdfs").size());

    // Sending the same record second time should not generate any metadata outputs
    output = runner.runProcess(ImmutableList.of(record));
    Assert.assertEquals(0, output.getRecords().get("hive").size());
    Assert.assertEquals(1, output.getRecords().get("hdfs").size());
  }


  @Test
  public void testTableAndPartitionAlreadyExistsInHive() throws Exception {
    executeUpdate("CREATE TABLE `tbl` (name string) PARTITIONED BY (dt string) STORED AS AVRO");
    executeUpdate("ALTER TABLE `tbl` ADD PARTITION (dt = 'secret-value')");

    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder().build();
    ProcessorRunner runner = getProcessorRunner(processor);
    runner.runInit();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("name", Field.create(Field.Type.STRING, "Jarcec"));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));

    // Since both table and partition exists, no metadata requests should be generated
    StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
    Assert.assertEquals(0, output.getRecords().get("hive").size());
    Assert.assertEquals(1, output.getRecords().get("hdfs").size());

    // Sending the same record second time should not generate any metadata outputs
    output = runner.runProcess(ImmutableList.of(record));
    Assert.assertEquals(0, output.getRecords().get("hive").size());
    Assert.assertEquals(1, output.getRecords().get("hdfs").size());
  }

  @Test
  public void testSubpartitions() throws Exception {
    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
        .partitions(new PartitionConfigBuilder()
            .addPartition("year", HiveType.STRING, "${YYYY()}")
            .addPartition("month", HiveType.STRING, "${MM()}")
            .addPartition("day", HiveType.STRING, "${DD()}")
            .build()
        )
        .build();

    ProcessorRunner runner = getProcessorRunner(processor);
    runner.runInit();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("name", Field.create(Field.Type.STRING, "Junko"));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));

    StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
    Assert.assertEquals(2, output.getRecords().get("hive").size());
    Assert.assertEquals(1, output.getRecords().get("hdfs").size());

    Calendar cal = Calendar.getInstance();
    cal.setTime(new Date(System.currentTimeMillis()));
    String year = String.valueOf(cal.get(Calendar.YEAR));
    String month = String.valueOf(Utils.intToPaddedString(cal.get(Calendar.MONTH) + 1, 2));
    String day =  String.valueOf(Utils.intToPaddedString(cal.get(Calendar.DAY_OF_MONTH), 2));

    // HDFS record
    Record hdfsRecord = output.getRecords().get("hdfs").get(0);
    Assert.assertNotNull(hdfsRecord);
    // The record should have "roll" set to true
    Assert.assertNotNull(hdfsRecord.getHeader().getAttribute("roll"));
    // Target directory with correct path
    Assert.assertEquals(
        Utils.format("/user/hive/warehouse/tbl/year={}/month={}/day={}", year, month, day),
        hdfsRecord.getHeader().getAttribute("targetDirectory")
    );
  }

}
