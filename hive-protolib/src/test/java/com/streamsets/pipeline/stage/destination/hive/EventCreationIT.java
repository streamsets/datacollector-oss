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
package com.streamsets.pipeline.stage.destination.hive;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.stage.BaseHiveIT;
import com.streamsets.pipeline.stage.HiveMetastoreTargetBuilder;
import com.streamsets.pipeline.stage.lib.hive.HiveMetastoreUtil;
import com.streamsets.pipeline.stage.lib.hive.typesupport.HiveType;
import com.streamsets.pipeline.stage.lib.hive.typesupport.HiveTypeInfo;
import com.streamsets.pipeline.stage.processor.hive.HMPDataFormat;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Ignore
public class EventCreationIT extends BaseHiveIT {

  @SuppressWarnings("unchecked")
  public List<Record> runNewTableRecord() throws Exception {
    HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder().build();

    TargetRunner runner = new TargetRunner.Builder(HiveMetastoreTarget.class, hiveTarget)
      .setOnRecordError(OnRecordError.STOP_PIPELINE)
      .build();
    runner.runInit();

    LinkedHashMap<String, HiveTypeInfo> columns = new LinkedHashMap<>();
    columns.put("name", HiveType.STRING.getSupport().generateHiveTypeInfoFromResultSet("STRING"));
    columns.put("surname", HiveType.STRING.getSupport().generateHiveTypeInfoFromResultSet("STRING"));

    LinkedHashMap<String, HiveTypeInfo> partitions = new LinkedHashMap<>();
    partitions.put("dt", HiveType.STRING.getSupport().generateHiveTypeInfoFromResultSet("STRING"));

    Field newTableField = HiveMetastoreUtil.newSchemaMetadataFieldBuilder(
        "default",
        "tbl",
        columns,
        partitions,
        true,
        BaseHiveIT.getDefaultWareHouseDir(),
        HiveMetastoreUtil.generateAvroSchema(columns, "tbl"),
        HMPDataFormat.AVRO
    );

    Record record = RecordCreator.create();
    record.set(newTableField);
    Assert.assertTrue(HiveMetastoreUtil.isSchemaChangeRecord(record));

    runner.runWrite(ImmutableList.of(record));

    assertTableStructure("default.tbl",
      ImmutablePair.of("tbl.name", Types.VARCHAR),
      ImmutablePair.of("tbl.surname", Types.VARCHAR),
      ImmutablePair.of("tbl.dt", Types.VARCHAR)
    );

    try {
      return runner.getEventRecords();
    } finally {
      runner.runDestroy();
    }
  }

  public List<Record> runNewPartitionRecord() throws Exception {
    HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder().build();

    TargetRunner runner = new TargetRunner.Builder(HiveMetastoreTarget.class, hiveTarget)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();


    Assert.assertEquals("There should be no error records", 0, runner.getErrorRecords().size());
    LinkedHashMap<String, String> partitionVals = new LinkedHashMap<String, String>();
    partitionVals.put("dt", "2016");

    Field newPartitionField = HiveMetastoreUtil.newPartitionMetadataFieldBuilder(
        "default",
        "tbl",
        partitionVals,
        "/user/hive/warehouse/tbl/dt=2016",
        HMPDataFormat.AVRO
    );
    Record record = RecordCreator.create();
    record.set(newPartitionField);
    runner.runWrite(ImmutableList.of(record));

    try {
      return runner.getEventRecords();
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testEventOnNewTable() throws Exception {
    List<Record> events = runNewTableRecord();

    Assert.assertNotNull(events);
    Assert.assertEquals(1, events.size());

    Record event = events.get(0);
    Assert.assertEquals("new-table", event.getHeader().getAttribute(EventRecord.TYPE));
    Assert.assertEquals("`default`.`tbl`", event.get("/table").getValueAsString());

    // Validate proper columns
    LinkedHashMap<String, Field> columns = event.get("/columns").getValueAsListMap();
    Assert.assertNotNull(columns);
    for(Map.Entry<String, Field> entry : columns.entrySet()) {
      System.out.println("FUCK: " + entry.getKey() + " => " + entry.getValue());
    }
    Assert.assertEquals(2, columns.size());
    Assert.assertTrue(columns.containsKey("name"));
    Assert.assertTrue(columns.containsKey("surname"));
    Assert.assertEquals("STRING", columns.get("name").getValueAsString());
    Assert.assertEquals("STRING", columns.get("surname").getValueAsString());

    // Validate proper partitions
    LinkedHashMap<String, Field> partitions = event.get("/partitions").getValueAsListMap();
    Assert.assertNotNull(partitions);
    Assert.assertEquals(1, partitions.size());
    Assert.assertTrue(partitions.containsKey("dt"));
    Assert.assertEquals("STRING", partitions.get("dt").getValueAsString());

  }

  @Test
  public void testNoRecordOnExistingTable() throws Exception {
    executeUpdate("CREATE TABLE `default`.`tbl` (name STRING, surname STRING) PARTITIONED BY(dt STRING) STORED AS AVRO");
    List<Record> events = runNewTableRecord();

    Assert.assertNotNull(events);
    Assert.assertEquals(0, events.size());
  }

  @Test
  public void testEventOnNewColumn() throws Exception {
    executeUpdate("CREATE TABLE `default`.`tbl` (name STRING) PARTITIONED BY(dt STRING) STORED AS AVRO");
    List<Record> events = runNewTableRecord();

    Assert.assertNotNull(events);
    Assert.assertEquals(1, events.size());
    Assert.assertEquals("new-columns", events.get(0).getHeader().getAttribute(EventRecord.TYPE));
    Assert.assertEquals("`default`.`tbl`", events.get(0).get("/table").getValueAsString());
  }

  @Test
  public void testEventOnNewPartition() throws Exception {
    executeUpdate("CREATE TABLE `default`.`tbl` (name STRING, surname STRING) PARTITIONED BY(dt STRING) STORED AS AVRO");
    List<Record> events = runNewPartitionRecord();

    Assert.assertNotNull(events);
    Assert.assertEquals(1, events.size());
    Assert.assertEquals("new-partition", events.get(0).getHeader().getAttribute(EventRecord.TYPE));
    Assert.assertEquals("`default`.`tbl`", events.get(0).get("/table").getValueAsString());
  }

  @Test
  public void testNoEventOnExistingPartition() throws Exception {
    executeUpdate("CREATE TABLE `default`.`tbl` (name STRING, surname STRING) PARTITIONED BY(dt STRING) STORED AS AVRO");
    executeUpdate("ALTER TABLE `default`.`tbl` ADD PARTITION(dt = '2016')");
    List<Record> events = runNewPartitionRecord();

    Assert.assertNotNull(events);
    Assert.assertEquals(0, events.size());
  }

}
