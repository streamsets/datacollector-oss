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
package com.streamsets.pipeline.stage.processor.hive;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.BaseHiveIT;
import com.streamsets.pipeline.stage.HiveMetadataProcessorBuilder;
import com.streamsets.pipeline.stage.PartitionConfigBuilder;
import com.streamsets.pipeline.stage.lib.hive.HiveMetastoreUtil;
import com.streamsets.pipeline.stage.lib.hive.typesupport.HiveType;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

@Ignore
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
    Assert.assertEquals(2, newTableRecord.get("/version").getValueAsInteger());
    Assert.assertEquals("tbl", newTableRecord.get("/table").getValueAsString());
    Assert.assertEquals("TABLE", newTableRecord.get("/type").getValueAsString());

    Record newPartitionRecord = output.getRecords().get("hive").get(1);
    Assert.assertNotNull(newPartitionRecord);
    Assert.assertEquals(2, newPartitionRecord.get("/version").getValueAsInteger());
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
    final TimeZone timezone = TimeZone.getTimeZone("UTC");

    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
        .partitions(new PartitionConfigBuilder()
            .addPartition("year", HiveType.STRING, "${YYYY()}")
            .addPartition("month", HiveType.STRING, "${MM()}")
            .addPartition("day", HiveType.STRING, "${DD()}")
            .build()
        )
        .timeZone(timezone)
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

    Calendar cal = Calendar.getInstance(timezone);
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

  @Test
  public void testSubTimeZonepartitions() throws Exception {
    final TimeZone timeZone = TimeZone.getTimeZone("US/Pacific");
    final TimeZone targetTimeZone = TimeZone.getTimeZone("US/Eastern");

    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
        .partitions(new PartitionConfigBuilder()
            .addPartition("year", HiveType.STRING, "${YYYY()}")
            .addPartition("month", HiveType.STRING, "${MM()}")
            .addPartition("day", HiveType.STRING, "${DD()}")
            .addPartition("hour", HiveType.STRING, "${hh()}")
            .addPartition("minute", HiveType.STRING, "${mm()}")
            .addPartition("second", HiveType.STRING, "${ss()}")
            .build()
        )
        .timeZone(targetTimeZone)
        .timeDriver("${record:value('/timestamp')}")
        .build();

    ProcessorRunner runner = getProcessorRunner(processor);
    runner.runInit();

    Calendar cal = Calendar.getInstance(timeZone);
    cal.setTime(new Date());

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("name", Field.create(Field.Type.STRING, "StreamSets"));
    map.put("timestamp", Field.create(Field.Type.DATE, cal.getTime()));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));

    StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
    Assert.assertEquals(2, output.getRecords().get("hive").size());
    Assert.assertEquals(1, output.getRecords().get("hdfs").size());

    // Set the target timezone
    cal.setTimeZone(targetTimeZone);

    String year = String.valueOf(cal.get(Calendar.YEAR));
    String month = String.valueOf(Utils.intToPaddedString(cal.get(Calendar.MONTH) + 1, 2));
    String day =  String.valueOf(Utils.intToPaddedString(cal.get(Calendar.DAY_OF_MONTH), 2));
    String hour = String.valueOf(Utils.intToPaddedString(cal.get(Calendar.HOUR_OF_DAY), 2));
    String minute = String.valueOf(Utils.intToPaddedString(cal.get(Calendar.MINUTE), 2));
    String second = String.valueOf(Utils.intToPaddedString(cal.get(Calendar.SECOND), 2));

    // HDFS record
    Record hdfsRecord = output.getRecords().get("hdfs").get(0);
    Assert.assertNotNull(hdfsRecord);
    // The record should have "roll" set to true
    Assert.assertNotNull(hdfsRecord.getHeader().getAttribute("roll"));
    // Target directory with correct path
    Assert.assertEquals(
        Utils.format("/user/hive/warehouse/tbl/year={}/month={}/day={}/hour={}/minute={}/second={}", year, month, day, hour, minute, second),
        hdfsRecord.getHeader().getAttribute("targetDirectory")
    );
  }

  @Test
  public void testNoPartitions() throws Exception {
    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
        .partitions(new PartitionConfigBuilder().build())
        .build();

    ProcessorRunner runner = getProcessorRunner(processor);
    runner.runInit();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("name", Field.create(Field.Type.STRING, "Junko"));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));

    StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
    // There should be only one metadata record, which is new partition info
    Assert.assertEquals(1, output.getRecords().get("hive").size());
    Assert.assertEquals(1, output.getRecords().get("hdfs").size());

    Record hiveRecord = output.getRecords().get("hive").get(0);
    LinkedHashMap<String, Field> meteadata = hiveRecord.get().getValueAsListMap();
    Assert.assertEquals(
        "/user/hive/warehouse/tbl",
        meteadata.get(HiveMetastoreUtil.LOCATION_FIELD).getValueAsString()
    );
    Assert.assertFalse(
        "Partition filed should be set in Metadata Record for non-partitioned table",
        meteadata.containsKey(HiveMetastoreUtil.PARTITION_FIELD)
    );

    Record hdfsRecord = output.getRecords().get("hdfs").get(0);
    Assert.assertEquals(
        "/user/hive/warehouse/tbl",
        hdfsRecord.getHeader().getAttribute("targetDirectory")
    );
  }

  @Test
  public void testUppercaseELsAndColumnName() throws Exception {
    /*
      Database name EL: ${record:attribute('DATABASE')} mapped to "default"
      Table mame EL   : ${record:attribute('TABLE_NAME')} mapped to "lowercase"

      Partition:
         Name    : UPPER_CASE => should be stored as "upper_case"
         Value EL: ${record:attribute('PARTITION_FIELD')} -> mapped to "some-value"

      Table Column :
         Name : COLUMN1 => should be stored as "column1"
     */
    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
        .database("${record:attribute('DATABASE')}")
        .table("${record:attribute('TABLE_NAME')}")
        .partitions(new PartitionConfigBuilder()
            .addPartition("UPPER_CASE", HiveType.STRING, "${record:attribute('PARTITION_FIELD')}")
            .build()
        )
        .build();

    ProcessorRunner runner = getProcessorRunner(processor);
    runner.runInit();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("COLUMN1", Field.create(Field.Type.STRING, "upper case column"));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    record.getHeader().setAttribute("DATABASE", "default");
    record.getHeader().setAttribute("TABLE_NAME", "lowercase");
    record.getHeader().setAttribute("PARTITION_FIELD", "some-value");
    ELVars elVars = runner.getContext().createELVars();
    RecordEL.setRecordInContext(elVars, record);

    StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
    // There should be two metadata records, both schema change and new partition
    Assert.assertEquals(2, output.getRecords().get("hive").size());
    Assert.assertEquals(1, output.getRecords().get("hdfs").size());


    Record schemaChangeRecord = output.getRecords().get("hive").get(0);
    LinkedHashMap<String, Field> metadata1 = schemaChangeRecord.get().getValueAsListMap();
    // Check if path to table is right.
    Assert.assertEquals(
        "/user/hive/warehouse/lowercase",
        metadata1.get(HiveMetastoreUtil.LOCATION_FIELD).getValueAsString()
    );

    // Check if partition name is lowercase in schema change record
    Map<String, Field> partitions = metadata1.get(HiveMetastoreUtil.PARTITION_FIELD).getValueAsListMap();
    Assert.assertEquals(partitions.get("0").getValueAsMap().get("name").getValueAsString(), "upper_case");
    // Check if column name is lowercase in schema change record
    Map<String, Field> columns = metadata1.get(HiveMetastoreUtil.COLUMNS_FIELD).getValueAsListMap();
    Assert.assertEquals(columns.get("0").getValueAsMap().get("name").getValueAsString(), "column1");


    Record newPartitionRecord = output.getRecords().get("hive").get(0);
    LinkedHashMap<String, Field> metadata2 = newPartitionRecord.get().getValueAsListMap();
    // Check if path to table is right.
    Assert.assertEquals(
        "/user/hive/warehouse/lowercase",
        metadata2.get(HiveMetastoreUtil.LOCATION_FIELD).getValueAsString()
    );
    // Check if partition name is lowercase in new partition record
    Map<String, Field> pValues = metadata2.get(HiveMetastoreUtil.PARTITION_FIELD).getValueAsListMap();
    Assert.assertEquals(pValues.get("0").getValueAsMap().get("name").getValueAsString(), "upper_case");

    Record hdfsRecord = output.getRecords().get("hdfs").get(0);
    Assert.assertEquals(
        "/user/hive/warehouse/lowercase/upper_case=some-value",
        hdfsRecord.getHeader().getAttribute("targetDirectory")
    );
  }

  @Test
  public void testTimeAllTheWayToMillisecond() throws Exception {
    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
      .partitions(new PartitionConfigBuilder()
        .addPartition("dt", HiveType.STRING, "${ss()}.${SSS()}")
        .build()
      )
      .external(true)
      .timeDriver("${time:now()}")
      .partitionPathTemplate("${ss()}.${SSS()}")
      .tablePathTemplate("/table")
      .build();
    ProcessorRunner runner = getProcessorRunner(processor);
    runner.runInit();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("name", Field.create(Field.Type.STRING, "SDC"));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));

    StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
    Assert.assertEquals(2, output.getRecords().get("hive").size());
    Assert.assertEquals(1, output.getRecords().get("hdfs").size());

    // Look at generated partition

    Record newPartitionRecord = output.getRecords().get("hive").get(1);
    Assert.assertNotNull(newPartitionRecord);
    Assert.assertEquals(2, newPartitionRecord.get("/version").getValueAsInteger());
    Assert.assertEquals("PARTITION", newPartitionRecord.get("/type").getValueAsString());

    String partitionValue = newPartitionRecord.get("/partitions[0]/value").getValueAsString();
    Assert.assertNotNull(partitionValue);
    String expectedPath = "/table/" + partitionValue;

    Assert.assertEquals(expectedPath, newPartitionRecord.get("/location").getValueAsString());
  }

}
