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
package com.streamsets.pipeline.stage.it;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.stage.HiveMetadataProcessorBuilder;
import com.streamsets.pipeline.stage.HiveMetastoreTargetBuilder;
import com.streamsets.pipeline.stage.PartitionConfigBuilder;
import com.streamsets.pipeline.stage.destination.hive.HiveMetastoreTarget;
import com.streamsets.pipeline.stage.lib.hive.Errors;
import com.streamsets.pipeline.stage.lib.hive.typesupport.HiveType;
import com.streamsets.pipeline.stage.processor.hive.HMPDataFormat;
import com.streamsets.pipeline.stage.processor.hive.HiveMetadataProcessor;
import com.streamsets.pipeline.stage.processor.hive.PartitionConfig;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Validates what happens on each drift type (columns added, removed, changed).
 */
@SuppressWarnings("unchecked")
@Ignore
public class DriftIT extends  BaseHiveMetadataPropagationIT {

  @Before
  public void createTestTable() throws Exception {
    executeUpdate("CREATE TABLE `tbl` (id int) PARTITIONED BY (dt string) STORED AS AVRO");
    executeUpdate("CREATE TABLE `tbl_no_partition` (city string) STORED AS AVRO");
    executeUpdate("CREATE TABLE `tbl_partition` (city string) partitioned by (dt1 String, dt2 String) STORED AS AVRO");
    executeUpdate("CREATE TABLE `multiple` (id int, value string) PARTITIONED BY (dt string) STORED AS AVRO");
    executeUpdate("CREATE EXTERNAL TABLE `ext_table` (id int, value string) STORED AS AVRO LOCATION '/user/hive/external'");
    executeUpdate("CREATE EXTERNAL TABLE `ext_table_location` " +
        "(id int, value string) partitioned by (dt String) STORED AS AVRO LOCATION '/user/hive/external'");
    executeUpdate("CREATE  TABLE `tbl_csv` (id int, value string) partitioned by (dt String)" +
        " ROW FORMAT DELIMITED " +
        " FIELDS TERMINATED BY ',' " +
        " STORED AS TEXTFILE ");

  }

  @Test
  public void testNewColumn() throws Exception {
    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
        .build();
    HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder()
        .build();

    List<Record> records = new LinkedList<>();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("id", Field.create(Field.Type.INTEGER, 1));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    records.add(record);

    map = new LinkedHashMap<>();
    map.put("id", Field.create(Field.Type.INTEGER, 2));
    map.put("new_column", Field.create(Field.Type.STRING, "new value"));
    record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    records.add(record);

    processRecords(processor, hiveTarget, records);

    assertQueryResult("select * from tbl order by id", new QueryValidator() {
      @Override
      public void validateResultSet(ResultSet rs) throws Exception {
        assertResultSetStructure(rs,
            ImmutablePair.of("tbl.id", Types.INTEGER),
            ImmutablePair.of("tbl.new_column", Types.VARCHAR),
            ImmutablePair.of("tbl.dt", Types.VARCHAR)
        );

        Assert.assertTrue("Table tbl doesn't contain any rows", rs.next());
        Assert.assertEquals(1, rs.getLong(1));
        Assert.assertEquals(null, rs.getString(2));

        Assert.assertTrue("Unexpected number of rows", rs.next());
        Assert.assertEquals(2, rs.getLong(1));
        Assert.assertEquals("new value", rs.getString(2));

        Assert.assertFalse("Unexpected number of rows", rs.next());
      }
    });
  }

  @Test
  public void testReorderedColumns() throws Exception {
    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
      .table("reorder")
      .partitions(Collections.<PartitionConfig>emptyList())
      .build();
    HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder()
      .build();

    List<Record> records = new LinkedList<>();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("first", Field.create(Field.Type.INTEGER, 1));
    map.put("second", Field.create(Field.Type.INTEGER, 1));
    map.put("third", Field.create(Field.Type.INTEGER, 1));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    records.add(record);

    map = new LinkedHashMap<>();
    map.put("third", Field.create(Field.Type.INTEGER, 1));
    map.put("second", Field.create(Field.Type.INTEGER, 1));
    map.put("first", Field.create(Field.Type.INTEGER, 1));
    record = RecordCreator.create("s", "s:2");
    record.set(Field.create(map));
    records.add(record);

    processRecords(processor, hiveTarget, records);

    assertTableStructure("default.reorder",
      ImmutablePair.of("reorder.first", Types.INTEGER),
      ImmutablePair.of("reorder.second", Types.INTEGER),
      ImmutablePair.of("reorder.third", Types.INTEGER)
    );
  }

  @Test
  public void testRenameColumn() throws Exception {
    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
        .build();
    HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder()
        .build();

    List<Record> records = new LinkedList<>();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("id", Field.create(Field.Type.INTEGER, 1));
    map.put("old_column", Field.create(Field.Type.STRING, "old_value"));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    records.add(record);

    map = new LinkedHashMap<>();
    map.put("id", Field.create(Field.Type.INTEGER, 2));
    map.put("new_column", Field.create(Field.Type.STRING, "new_value"));
    record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    records.add(record);

    processRecords(processor, hiveTarget, records);

    assertQueryResult("select * from tbl order by id", new QueryValidator() {
      @Override
      public void validateResultSet(ResultSet rs) throws Exception {
        assertResultSetStructure(rs,
            ImmutablePair.of("tbl.id", Types.INTEGER),
            ImmutablePair.of("tbl.old_column", Types.VARCHAR),
            ImmutablePair.of("tbl.new_column", Types.VARCHAR),
            ImmutablePair.of("tbl.dt", Types.VARCHAR)
        );

        Assert.assertTrue("Table tbl doesn't contain any rows", rs.next());
        Assert.assertEquals(1, rs.getLong(1));
        Assert.assertEquals("old_value", rs.getString(2));
        Assert.assertEquals(null, rs.getString(3));


        Assert.assertTrue("Unexpected number of rows", rs.next());
        Assert.assertEquals(2, rs.getLong(1));
        Assert.assertEquals(null, rs.getString(2));
        Assert.assertEquals("new_value", rs.getString(3));

        Assert.assertFalse("Unexpected number of rows", rs.next());
      }
    });
  }

  @Test
  public void testNewColumnInTheMiddle() throws Exception {
    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
        .table("multiple")
        .build();
    HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder()
        .build();

    List<Record> records = new LinkedList<>();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("id", Field.create(Field.Type.INTEGER, 1));
    map.put("value", Field.create(Field.Type.STRING, "exists"));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    records.add(record);

    map = new LinkedHashMap<>();
    map.put("id", Field.create(Field.Type.INTEGER, 2));
    map.put("new_column", Field.create(Field.Type.STRING, "new value"));
    map.put("value", Field.create(Field.Type.STRING, "exists"));
    record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    records.add(record);

    processRecords(processor, hiveTarget, records);

    assertQueryResult("select * from multiple order by id", new QueryValidator() {
      @Override
      public void validateResultSet(ResultSet rs) throws Exception {
        assertResultSetStructure(rs,
            ImmutablePair.of("multiple.id", Types.INTEGER),
            ImmutablePair.of("multiple.value", Types.VARCHAR),
            ImmutablePair.of("multiple.new_column", Types.VARCHAR),
            ImmutablePair.of("multiple.dt", Types.VARCHAR)
        );

        Assert.assertTrue("Table tbl doesn't contain any rows", rs.next());
        Assert.assertEquals(1, rs.getLong(1));
        Assert.assertEquals("exists", rs.getString(2));
        Assert.assertEquals(null, rs.getString(3));

        Assert.assertTrue("Unexpected number of rows", rs.next());
        Assert.assertEquals(2, rs.getLong(1));
        Assert.assertEquals("exists", rs.getString(2));
        Assert.assertEquals("new value", rs.getString(3));

        Assert.assertFalse("Unexpected number of rows", rs.next());
      }
    });
  }

  @Test
  public void testRemovedColumn() throws Exception {
    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
        .build();
    HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder()
        .build();

    List<Record> records = new LinkedList<>();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("id", Field.create(Field.Type.INTEGER, 1));
    map.put("removed", Field.create(Field.Type.STRING, "value"));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    records.add(record);

    map = new LinkedHashMap<>();
    map.put("id", Field.create(Field.Type.INTEGER, 2));
    record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    records.add(record);

    processRecords(processor, hiveTarget, records);

    assertQueryResult("select * from tbl order by id", new QueryValidator() {
      @Override
      public void validateResultSet(ResultSet rs) throws Exception {
        assertResultSetStructure(rs,
            ImmutablePair.of("tbl.id", Types.INTEGER),
            ImmutablePair.of("tbl.removed", Types.VARCHAR),
            ImmutablePair.of("tbl.dt", Types.VARCHAR)
        );

        Assert.assertTrue("Table tbl doesn't contain any rows", rs.next());
        Assert.assertEquals(1, rs.getLong(1));
        Assert.assertEquals("value", rs.getString(2));

        Assert.assertTrue("Unexpected number of rows", rs.next());
        Assert.assertEquals(2, rs.getLong(1));
        Assert.assertEquals(null, rs.getString(2));

        Assert.assertFalse("Unexpected number of rows", rs.next());
      }
    });
  }


  @Test
  public void testChangedColumnType() throws Exception {
    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
        .build();
    HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder()
        .build();

    List<Record> records = new LinkedList<>();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("id", Field.create(Field.Type.INTEGER, 1));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    records.add(record);

    map = new LinkedHashMap<>();
    map.put("id", Field.create(Field.Type.STRING, "2"));
    record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    records.add(record);

    try {
      processRecords(processor, hiveTarget, records);
      Assert.fail("Column type change haven't resulted in exception");
    } catch (StageException e) {
      Assert.assertEquals(Errors.HIVE_21, e.getErrorCode());
    }
  }

  @Test
  public void testChangedColumnTypeDecimal() throws Exception {
    executeUpdate("CREATE TABLE `decimal` (dec decimal(2, 1)) PARTITIONED BY (dt string) STORED AS AVRO");

    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
        .table("decimal")
        .decimalConfig(3, 2)
        .build();
    HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder()
        .build();

    List<Record> records = new LinkedList<>();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("dec", Field.create(Field.Type.DECIMAL, BigDecimal.valueOf(2.2)));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    records.add(record);

    try {
      processRecords(processor, hiveTarget, records);
      Assert.fail("Column type change haven't resulted in exception");
    } catch (StageException e) {
      Assert.assertEquals(Errors.HIVE_21, e.getErrorCode());
      Assert.assertTrue(e.getMessage().contains("Expected: DECIMAL(2,1), Actual: DECIMAL(3,2)"));
    }
  }

  @Test
  public void testDifferentColumnCase() throws Exception {
    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
        .build();
    HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder()
        .build();
    List<Record> records = new LinkedList<>();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("ID", Field.create(Field.Type.INTEGER, 1));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    records.add(record);
    processRecords(processor, hiveTarget, records);

    assertQueryResult("select * from tbl order by id", new QueryValidator() {
      @Override
      public void validateResultSet(ResultSet rs) throws Exception {
        assertResultSetStructure(rs,
            ImmutablePair.of("tbl.id", Types.INTEGER),
            ImmutablePair.of("tbl.dt", Types.VARCHAR)
        );

        Assert.assertTrue("Table tbl doesn't contain any rows", rs.next());
        Assert.assertEquals(1, rs.getInt(1));
        Assert.assertFalse("Unexpected number of rows", rs.next());
      }
    });
  }

  @Test
  public void testAddColumnToNonPartitionedTableInternal() throws Exception {
    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
        .table("tbl_no_partition")
        .partitions(new PartitionConfigBuilder().build())
        .build();

    HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder()
        .build();
    List<Record> records = new LinkedList<>();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("city", Field.create("San Francisco"));
    map.put("state", Field.create("California"));
    Record record = RecordCreator.create();
    record.set(Field.create(map));
    records.add(record);
    processRecords(processor, hiveTarget, records);

    assertQueryResult("select * from tbl_no_partition", new QueryValidator() {
      @Override
      public void validateResultSet(ResultSet rs) throws Exception {
        assertResultSetStructure(rs,
            ImmutablePair.of("tbl_no_partition.city", Types.VARCHAR),
            ImmutablePair.of("tbl_no_partition.state", Types.VARCHAR)
        );

        Assert.assertTrue("Table tbl_no_partition doesn't contain any rows", rs.next());
        Assert.assertEquals("San Francisco", rs.getString(1));
        Assert.assertEquals("California", rs.getString(2));
        Assert.assertFalse("Unexpected number of rows", rs.next());
      }
    });
  }

  @Test
  public void testAddColumnToNonPartitionedTableExternal() throws Exception {
    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
        .table("ext_table")
        .partitions(new PartitionConfigBuilder().build())
        .external(true)
        .tablePathTemplate("/user/hive/external")
        .build();

    HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder()
        .build();
    List<Record> records = new LinkedList<>();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("id", Field.create(123));
    map.put("value", Field.create("testtest"));
    Record record = RecordCreator.create();
    record.set(Field.create(map));
    records.add(record);
    processRecords(processor, hiveTarget, records);

    assertQueryResult("select * from ext_table", new QueryValidator() {
      @Override
      public void validateResultSet(ResultSet rs) throws Exception {
        assertResultSetStructure(rs,
            ImmutablePair.of("ext_table.id", Types.INTEGER),
            ImmutablePair.of("ext_table.value", Types.VARCHAR)
        );
        Assert.assertTrue("Table ext_table doesn't contain any rows", rs.next());
        Assert.assertEquals(123, rs.getInt(1));
        Assert.assertEquals("testtest", rs.getString(2));
        Assert.assertFalse("Unexpected number of rows", rs.next());
      }
    });
  }

  @Test
  public void testAddPartitionToNonPartitionedTable() throws Exception {
    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
        .table("tbl_no_partition")
        .build();

    HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder()
        .build();
    List<Record> records = new LinkedList<>();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("city", Field.create("San Jose"));
    Record record = RecordCreator.create();
    record.set(Field.create(map));
    records.add(record);

    try {
      processRecords(processor, hiveTarget, records);
      Assert.fail("Adding a partition to non-partitioned table should fail");
    } catch (StageException e) {
      Assert.assertEquals(e.getErrorCode(), Errors.HIVE_27);
    }

    assertQueryResult("select * from tbl_no_partition", new QueryValidator() {
      @Override
      public void validateResultSet(ResultSet rs) throws Exception {
        // Table structure should not be altered
        assertResultSetStructure(rs,
            ImmutablePair.of("tbl_no_partition.city", Types.VARCHAR)
        );
        // Alter Table query failed, so no data should be added to the table
        Assert.assertFalse("Table tbl_no_partition should not contain rows", rs.next());
      }
    });
  }


  public void testPartitionMismatch(List<PartitionConfig> partitionConfigList) throws Exception {
    HiveMetadataProcessorBuilder processorBuilder = new HiveMetadataProcessorBuilder()
        .table("tbl_partition");
    if (!partitionConfigList.isEmpty()) {
      processorBuilder.partitions(partitionConfigList);
    }
    HiveMetadataProcessor processor = processorBuilder.build();

    HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder()
        .build();
    List<Record> records = new LinkedList<>();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("city", Field.create("San Jose"));
    Record record = RecordCreator.create();
    record.set(Field.create(map));
    records.add(record);

    try {
      processRecords(processor, hiveTarget, records);
      Assert.fail("Specifying no partitions to partitioned table should fail");
    } catch (StageException e) {
      Assert.assertEquals("Error codes mismatch", Errors.HIVE_27, e.getErrorCode());
    }

    assertQueryResult("select * from tbl_partition", new QueryValidator() {
      @Override
      public void validateResultSet(ResultSet rs) throws Exception {
        // Table structure should not be altered
        assertResultSetStructure(
            rs,
            ImmutablePair.of("tbl_partition.city", Types.VARCHAR),
            ImmutablePair.of("tbl_partition.dt1", Types.VARCHAR),
            ImmutablePair.of("tbl_partition.dt2", Types.VARCHAR)
        );
        // Alter Table query failed, so no data should be added to the table
        Assert.assertFalse("Table tbl_partition should not contain rows", rs.next());
      }
    });
  }

  @Test
  public void testNoPartitionsToPartitionedTable() throws Exception {
    testPartitionMismatch(new PartitionConfigBuilder().build());
  }

  //Delete partition
  @Test
  public void testLessPartitionsMismatch() throws Exception {
    testPartitionMismatch(new PartitionConfigBuilder().addPartition("dt1", HiveType.STRING, "2016").build());
  }

  //Add partition
  @Test
  public void testMorePartitionsMismatch() throws Exception {
    testPartitionMismatch(
        new PartitionConfigBuilder()
            .addPartition("dt1", HiveType.STRING, "2016")
            .addPartition("dt2", HiveType.STRING, "2017")
            .addPartition("dt3", HiveType.STRING, "2018")
            .build()
    );
  }

  @Test
  public void testChangePartitionType() throws Exception {
    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
        .table("tbl_partition")
        .partitions(
            new PartitionConfigBuilder()
                .addPartition("dt1", HiveType.STRING, "2016")
                .addPartition("dt2", HiveType.BIGINT, "2017")
                .build()
        )
        .build();

    HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder()
        .build();
    List<Record> records = new LinkedList<>();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("id", Field.create(123));
    map.put("value", Field.create("testtest"));
    Record record = RecordCreator.create();
    record.set(Field.create(map));
    records.add(record);
    try {
      processRecords(processor, hiveTarget, records);
      Assert.fail("Partition type mismatch should cause a failure");
    } catch (StageException e) {
      Assert.assertEquals("Error codes should match", Errors.HIVE_28, e.getErrorCode());
    }
  }

  @Test
  public void testPartitionLocationMismatch() throws Exception {
    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
        .table("ext_table_location")
        .partitions(
            new PartitionConfigBuilder()
                .addPartition("dt", HiveType.STRING, "2016")
                .build()
        )
        .external(true)
        .tablePathTemplate("/user/hive/external")
        .partitionPathTemplate("location1")
        .build();
    HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder().build();

    List<Record> records = new LinkedList<>();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("id", Field.create(123));
    map.put("value", Field.create("testtest"));
    Record record = RecordCreator.create();
    record.set(Field.create(map));
    records.add(record);

    //Added file in the location1
    processRecords(processor, hiveTarget, records);

    assertQueryResult("desc formatted ext_table_location partition(dt='2016')", new QueryValidator() {
      @Override
      public void validateResultSet(ResultSet rs) throws Exception {
        // Table structure should not be altered
        while (rs.next()) {
          String col_name = rs.getString(1);
          if (col_name != null && col_name.equals("Location:")) {
            Assert.assertEquals("/user/hive/external/location1", rs.getString(2));
          }

        }
      }
    });

    processor = new HiveMetadataProcessorBuilder()
        .table("ext_table_location")
        .partitions(
            new PartitionConfigBuilder()
                .addPartition("dt", HiveType.STRING, "2016")
                .build()
        )
        .external(true)
        .tablePathTemplate("/user/hive/external")
        .partitionPathTemplate("location2")
        .build();
    try {
      processRecords(processor, hiveTarget, records);
    } catch (StageException e) {
      Assert.assertEquals("Errors Codes did not match", Errors.HIVE_31, e.getErrorCode());
    }
    assertQueryResult("desc formatted ext_table_location partition(dt='2016')", new QueryValidator() {
      @Override
      public void validateResultSet(ResultSet rs) throws Exception {
        // Table structure should not be altered
        while (rs.next()) {
          String col_name = rs.getString(1);
          if (col_name != null && col_name.equals("Location:")) {
            Assert.assertEquals("/user/hive/external/location1", rs.getString(2));
          }
        }
      }
    });
  }

  @Test
  public void testNonAvroTable() throws Exception {
    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
        .table("tbl_csv")
        .partitions(
            new PartitionConfigBuilder()
                .addPartition("dt", HiveType.STRING, "2016")
                .build()
        )
        .build();

    HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder()
        .build();
    List<Record> records = new LinkedList<>();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("id", Field.create(123));
    map.put("value", Field.create("testtest"));
    Record record = RecordCreator.create();
    record.set(Field.create(map));
    records.add(record);

    try {
      processRecords(processor, hiveTarget, records);
      Assert.fail("Non Avro tables should not be processed");
    } catch (StageException e) {
      Assert.assertEquals("Error codes should match", Errors.HIVE_32, e.getErrorCode());
    }

  }

  @Test
  public void testNonParquetTable() throws Exception {
    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
        .table("tbl_csv")
        .partitions(
            new PartitionConfigBuilder()
                .addPartition("dt", HiveType.STRING, "2016")
                .build()
        )
        .dataFormat(HMPDataFormat.PARQUET)
        .build();

    HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder()
        .build();
    List<Record> records = new LinkedList<>();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("id", Field.create(123));
    map.put("value", Field.create("testtest"));
    Record record = RecordCreator.create();
    record.set(Field.create(map));
    records.add(record);

    try {
      processRecords(processor, hiveTarget, records);
      Assert.fail("Non Avro tables should not be processed");
    } catch (StageException e) {
      Assert.assertEquals("Error codes should match", Errors.HIVE_32, e.getErrorCode());
    }
  }

  @Test // SDC-5273
  public void testDriftWithMultipleOpenPartitions() throws Exception {
    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
        .table("drift_open_partitions")
         .partitions(
            new PartitionConfigBuilder()
                .addPartition("dt", HiveType.STRING, "${record:value('/partition')}")
                .build()
        )
        .build();
    HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder()
        .build();

    List<Record> records = new LinkedList<>();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("id", Field.create(Field.Type.INTEGER, 1));
    map.put("partition", Field.create(Field.Type.STRING, "1"));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    records.add(record);

    map = new LinkedHashMap<>();
    map.put("id", Field.create(Field.Type.INTEGER, 2));
    map.put("partition", Field.create(Field.Type.STRING, "2"));
    record = RecordCreator.create("s", "s:2");
    record.set(Field.create(map));
    records.add(record);

    map = new LinkedHashMap<>();
    map.put("id", Field.create(Field.Type.INTEGER, 3));
    map.put("partition", Field.create(Field.Type.STRING, "3"));
    record = RecordCreator.create("s", "s:3");
    record.set(Field.create(map));
    records.add(record);

    map = new LinkedHashMap<>();
    map.put("id", Field.create(Field.Type.INTEGER, 4));
    map.put("partition", Field.create(Field.Type.STRING, "1"));
    map.put("new_column", Field.create(Field.Type.STRING, "4"));
    record = RecordCreator.create("s", "s:4");
    record.set(Field.create(map));
    records.add(record);

    map = new LinkedHashMap<>();
    map.put("id", Field.create(Field.Type.INTEGER, 5));
    map.put("partition", Field.create(Field.Type.STRING, "2"));
    map.put("new_column", Field.create(Field.Type.STRING, "5"));
    record = RecordCreator.create("s", "s:5");
    record.set(Field.create(map));
    records.add(record);

    map = new LinkedHashMap<>();
    map.put("id", Field.create(Field.Type.INTEGER, 6));
    map.put("partition", Field.create(Field.Type.STRING, "3"));
    map.put("new_column", Field.create(Field.Type.STRING, "6"));
    record = RecordCreator.create("s", "s:6");
    record.set(Field.create(map));
    records.add(record);

    processRecords(processor, hiveTarget, records);

    // No error records should be generated
    Assert.assertEquals(0, getErrorRecord(Stage.METADATA_PROCESSOR).size());
    Assert.assertEquals(0, getErrorRecord(Stage.METASTORE_TARGET).size());
    Assert.assertEquals(0, getErrorRecord(Stage.HDFS_TARGET).size());

    assertQueryResult("select * from drift_open_partitions order by id", new QueryValidator() {
      @Override
      public void validateResultSet(ResultSet rs) throws Exception {
        assertResultSetStructure(rs,
            ImmutablePair.of("drift_open_partitions.id", Types.INTEGER),
            ImmutablePair.of("drift_open_partitions.partition", Types.VARCHAR),
            ImmutablePair.of("drift_open_partitions.new_column", Types.VARCHAR),
            ImmutablePair.of("drift_open_partitions.dt", Types.VARCHAR)
        );

        Assert.assertTrue("Table tbl doesn't contain any rows", rs.next());
        Assert.assertEquals(1, rs.getLong(1));
        Assert.assertEquals("1", rs.getString(2));
        Assert.assertEquals(null, rs.getString(3));
        Assert.assertEquals("1", rs.getString(4));

        Assert.assertTrue("Unexpected number of rows", rs.next());
        Assert.assertEquals(2, rs.getLong(1));
        Assert.assertEquals("2", rs.getString(2));
        Assert.assertEquals(null, rs.getString(3));
        Assert.assertEquals("2", rs.getString(4));

        Assert.assertTrue("Unexpected number of rows", rs.next());
        Assert.assertEquals(3, rs.getLong(1));
        Assert.assertEquals("3", rs.getString(2));
        Assert.assertEquals(null, rs.getString(3));
        Assert.assertEquals("3", rs.getString(4));

        Assert.assertTrue("Unexpected number of rows", rs.next());
        Assert.assertEquals(4, rs.getLong(1));
        Assert.assertEquals("1", rs.getString(2));
        Assert.assertEquals("4", rs.getString(3));
        Assert.assertEquals("1", rs.getString(4));

        Assert.assertTrue("Unexpected number of rows", rs.next());
        Assert.assertEquals(5, rs.getLong(1));
        Assert.assertEquals("2", rs.getString(2));
        Assert.assertEquals("5", rs.getString(3));
        Assert.assertEquals("2", rs.getString(4));

        Assert.assertTrue("Unexpected number of rows", rs.next());
        Assert.assertEquals(6, rs.getLong(1));
        Assert.assertEquals("3", rs.getString(2));
        Assert.assertEquals("6", rs.getString(3));
        Assert.assertEquals("3", rs.getString(4));

        Assert.assertFalse("Unexpected number of rows", rs.next());
      }
    });
  }
}
