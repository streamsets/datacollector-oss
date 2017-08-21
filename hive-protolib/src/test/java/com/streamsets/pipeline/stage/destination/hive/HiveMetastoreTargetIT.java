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
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.stage.BaseHiveIT;
import com.streamsets.pipeline.stage.HiveMetastoreTargetBuilder;
import com.streamsets.pipeline.stage.lib.hive.Errors;
import com.streamsets.pipeline.stage.lib.hive.HiveMetastoreUtil;
import com.streamsets.pipeline.stage.lib.hive.typesupport.HiveType;
import com.streamsets.pipeline.stage.lib.hive.typesupport.HiveTypeInfo;
import com.streamsets.pipeline.stage.processor.hive.HMPDataFormat;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.List;

@Ignore
public class HiveMetastoreTargetIT extends BaseHiveIT {

  private static Logger LOG = LoggerFactory.getLogger(BaseHiveIT.class);

  @Test
  public void testInitialization() throws Exception {
    HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder().build();

    TargetRunner runner = new TargetRunner.Builder(HiveMetastoreTarget.class, hiveTarget)
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    Assert.assertEquals(0, issues.size());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCreateNonExistingTable() throws Exception {
    HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder().build();

    TargetRunner runner = new TargetRunner.Builder(HiveMetastoreTarget.class, hiveTarget)
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .build();
    runner.runInit();

    LinkedHashMap<String, HiveTypeInfo> columns = new LinkedHashMap<>();
    columns.put("name", HiveType.STRING.getSupport().generateHiveTypeInfoFromResultSet("STRING"));

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
    runner.runDestroy();

    assertTableStructure("default.tbl",
        ImmutablePair.of("tbl.name", Types.VARCHAR),
        ImmutablePair.of("tbl.dt", Types.VARCHAR)
    );
  }

  @Test
  public void testNonPartitionedInfoToPartitionedTable() throws Exception {
    HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder().build();

    TargetRunner runner = new TargetRunner.Builder(HiveMetastoreTarget.class, hiveTarget)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();

    LinkedHashMap<String, HiveTypeInfo> columns = new LinkedHashMap<>();
    columns.put("name", HiveType.STRING.getSupport().generateHiveTypeInfoFromResultSet("STRING"));

    Field newTableField = HiveMetastoreUtil.newSchemaMetadataFieldBuilder(
        "default",
        "tbl",
        columns,
        null,
        true,
        BaseHiveIT.getDefaultWareHouseDir(),
        HiveMetastoreUtil.generateAvroSchema(columns, "tbl"),
        HMPDataFormat.AVRO
    );

    Record record = RecordCreator.create();
    record.set(newTableField);
    Assert.assertTrue(HiveMetastoreUtil.isSchemaChangeRecord(record));

    runner.runWrite(ImmutableList.of(record));
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
    record = RecordCreator.create();
    record.set(newPartitionField);
    runner.runWrite(ImmutableList.of(record));
    Assert.assertEquals("There should be one error record", 1, runner.getErrorRecords().size());
    Record errorRecord = runner.getErrorRecords().get(0);
    Assert.assertEquals(errorRecord.getHeader().getErrorCode(), Errors.HIVE_27.name());
  }

  @Test
  public void testPartitionMismatch() throws Exception {
    HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder().build();

    TargetRunner runner = new TargetRunner.Builder(HiveMetastoreTarget.class, hiveTarget)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();

    LinkedHashMap<String, HiveTypeInfo> columns = new LinkedHashMap<>();
    columns.put("name", HiveType.STRING.getSupport().generateHiveTypeInfoFromResultSet("STRING"));

    LinkedHashMap<String, HiveTypeInfo> partitions = new LinkedHashMap<>();
    partitions.put("dt1", HiveType.STRING.getSupport().generateHiveTypeInfoFromResultSet("STRING"));
    partitions.put("dt2", HiveType.STRING.getSupport().generateHiveTypeInfoFromResultSet("STRING"));


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
    Assert.assertEquals("There should be no error records", 0, runner.getErrorRecords().size());

    //More Partitions (3) than configured
    LinkedHashMap<String, String> partitionVals = new LinkedHashMap<>();
    partitionVals.put("dt1", "2016");
    partitionVals.put("dt2", "2017");
    partitionVals.put("dt3", "2018");

    Field newPartitionField1 = HiveMetastoreUtil.newPartitionMetadataFieldBuilder(
        "default",
        "tbl",
        partitionVals,
        "/user/hive/warehouse/tbl/dt1=2016/dt2=2017/dt3=2018",
        HMPDataFormat.AVRO
    );
    record = RecordCreator.create();
    record.set(newPartitionField1);
    runner.runWrite(ImmutableList.of(record));
    Assert.assertEquals("There should be one error record", 1, runner.getErrorRecords().size());
    Record errorRecord = runner.getErrorRecords().get(0);
    Assert.assertEquals(errorRecord.getHeader().getErrorCode(), Errors.HIVE_27.name());

    //Resetting the runner
    runner.getErrorRecords().clear();
    //Remove 3 partition names, less number of partitions than configured.
    partitionVals.remove("dt2");
    partitionVals.remove("dt3");

    Field newPartitionField2 = HiveMetastoreUtil.newPartitionMetadataFieldBuilder(
        "default",
        "tbl",
        partitionVals,
        "/user/hive/warehouse/tbl/dt1=2016/dt2=2017/dt3=2018",
        HMPDataFormat.AVRO
    );

    record = RecordCreator.create();
    record.set(newPartitionField2);
    runner.runWrite(ImmutableList.of(record));
    Assert.assertEquals("There should be one error record", 1, runner.getErrorRecords().size());
    errorRecord = runner.getErrorRecords().get(0);
    Assert.assertEquals(errorRecord.getHeader().getErrorCode(), Errors.HIVE_27.name());
  }

  @Test
  public void testPartitionLocationMismatch() throws Exception {
    HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder().build();

    TargetRunner runner = new TargetRunner.Builder(HiveMetastoreTarget.class, hiveTarget)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();

    LinkedHashMap<String, HiveTypeInfo> columns = new LinkedHashMap<>();
    columns.put("name", HiveType.STRING.getSupport().generateHiveTypeInfoFromResultSet("STRING"));

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

    Assert.assertEquals("There should be no error records", 0, runner.getErrorRecords().size());

    LinkedHashMap<String, String> partitionVals = new LinkedHashMap<>();
    partitionVals.put("dt", "2016");

    Field newPartitionField1 = HiveMetastoreUtil.newPartitionMetadataFieldBuilder(
        "default",
        "tbl",
        partitionVals,
        "/user/hive/external/tbl/location1",
        HMPDataFormat.AVRO
    );
    record = RecordCreator.create();
    record.set(newPartitionField1);
    runner.runWrite(ImmutableList.of(record));
    Assert.assertEquals("There should be no error records", 0, runner.getErrorRecords().size());

    Field newPartitionField2 = HiveMetastoreUtil.newPartitionMetadataFieldBuilder(
        "default",
        "tbl",
        partitionVals,
        "/user/hive/external/tbl/location2",
        HMPDataFormat.AVRO
    );

    record = RecordCreator.create();
    record.set(newPartitionField2);
    runner.runWrite(ImmutableList.of(record));
    Assert.assertEquals("There should be no error records", 1, runner.getErrorRecords().size());
    Record errorRecord = runner.getErrorRecords().get(0);
    Assert.assertEquals("Error codes mismatch", Errors.HIVE_31.name(), errorRecord.getHeader().getErrorCode());
  }

  @Test
  public void testNonAvroTable() throws Exception {
    executeUpdate("CREATE  TABLE `tbl_csv` (id int, value string) partitioned by (dt String)" +
        " ROW FORMAT DELIMITED " +
        " FIELDS TERMINATED BY ',' " +
        " STORED AS TEXTFILE ");
    HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder().build();

    TargetRunner runner = new TargetRunner.Builder(HiveMetastoreTarget.class, hiveTarget)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();

    LinkedHashMap<String, HiveTypeInfo> columns = new LinkedHashMap<>();
    columns.put("name", HiveType.STRING.getSupport().generateHiveTypeInfoFromResultSet("STRING"));

    LinkedHashMap<String, HiveTypeInfo> partitions = new LinkedHashMap<>();
    partitions.put("dt", HiveType.STRING.getSupport().generateHiveTypeInfoFromResultSet("STRING"));

    Field newTableField = HiveMetastoreUtil.newSchemaMetadataFieldBuilder(
        "default",
        "tbl_csv",
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

    Assert.assertEquals("There should be one error record", 1L, runner.getErrorRecords().size());
    Record errorRecord = runner.getErrorRecords().get(0);
    Assert.assertEquals("Error codes mismatch", Errors.HIVE_32.name(), errorRecord.getHeader().getErrorCode());
  }

  @Test
  public void testNonParquetTable() throws Exception {
    executeUpdate("CREATE  TABLE `tbl_csv` (id int, value string) partitioned by (dt String)" +
        " ROW FORMAT DELIMITED " +
        " FIELDS TERMINATED BY ',' " +
        " STORED AS TEXTFILE ");
    HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder().build();

    TargetRunner runner = new TargetRunner.Builder(HiveMetastoreTarget.class, hiveTarget)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();

    LinkedHashMap<String, HiveTypeInfo> columns = new LinkedHashMap<>();
    columns.put("name", HiveType.STRING.getSupport().generateHiveTypeInfoFromResultSet("STRING"));

    LinkedHashMap<String, HiveTypeInfo> partitions = new LinkedHashMap<>();
    partitions.put("dt", HiveType.STRING.getSupport().generateHiveTypeInfoFromResultSet("STRING"));

    Field newTableField = HiveMetastoreUtil.newSchemaMetadataFieldBuilder(
        "default",
        "tbl_csv",
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

    Assert.assertEquals("There should be one error record", 1L, runner.getErrorRecords().size());
    Record errorRecord = runner.getErrorRecords().get(0);
    Assert.assertEquals("Error codes mismatch", Errors.HIVE_32.name(), errorRecord.getHeader().getErrorCode());
  }

  @Test
  public void testUnsupportedDataFormat() throws Exception {
    executeUpdate("CREATE  TABLE `tbl_csv` (id int, value string) partitioned by (dt String)" +
        " ROW FORMAT DELIMITED " +
        " FIELDS TERMINATED BY ',' " +
        " STORED AS TEXTFILE ");
    HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder().build();

    TargetRunner runner = new TargetRunner.Builder(HiveMetastoreTarget.class, hiveTarget)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();

    LinkedHashMap<String, HiveTypeInfo> columns = new LinkedHashMap<>();
    columns.put("name", HiveType.STRING.getSupport().generateHiveTypeInfoFromResultSet("STRING"));

    LinkedHashMap<String, HiveTypeInfo> partitions = new LinkedHashMap<>();
    partitions.put("dt", HiveType.STRING.getSupport().generateHiveTypeInfoFromResultSet("STRING"));

    Field newTableField = HiveMetastoreUtil.newSchemaMetadataFieldBuilder(
        "default",
        "tbl_csv",
        columns,
        partitions,
        true,
        BaseHiveIT.getDefaultWareHouseDir(),
        HiveMetastoreUtil.generateAvroSchema(columns, "tbl"),
        HMPDataFormat.AVRO
    );

    // change the dataFormat
    newTableField.getValueAsMap().put(HiveMetastoreUtil.DATA_FORMAT, Field.create("Text"));

    Record record = RecordCreator.create();
    record.set(newTableField);
    Assert.assertTrue(HiveMetastoreUtil.isSchemaChangeRecord(record));

    runner.runWrite(ImmutableList.of(record));

    Assert.assertEquals("There should be one error record", 1L, runner.getErrorRecords().size());
    Record errorRecord = runner.getErrorRecords().get(0);
    Assert.assertEquals("Error codes mismatch", Errors.HIVE_37.name(), errorRecord.getHeader().getErrorCode());
  }
}

