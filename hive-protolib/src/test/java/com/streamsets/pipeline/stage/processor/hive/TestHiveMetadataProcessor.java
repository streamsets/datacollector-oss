/**
 * Copyright 2016 StreamSets Inc.
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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.lib.util.SdcAvroTestUtil;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.stage.BaseHiveIT;
import com.streamsets.pipeline.stage.HiveMetadataProcessorBuilder;
import com.streamsets.pipeline.stage.PartitionConfigBuilder;
import com.streamsets.pipeline.stage.lib.hive.HiveConfigBean;
import com.streamsets.pipeline.stage.lib.hive.Errors;
import com.streamsets.pipeline.stage.lib.hive.HiveMetastoreUtil;
import com.streamsets.pipeline.stage.lib.hive.TestHiveMetastoreUtil;
import com.streamsets.pipeline.stage.lib.hive.cache.*;
import com.streamsets.pipeline.stage.lib.hive.typesupport.DecimalHiveTypeSupport;
import com.streamsets.pipeline.stage.lib.hive.typesupport.HiveType;
import com.streamsets.pipeline.stage.lib.hive.typesupport.HiveTypeInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.api.support.membermodification.MemberMatcher;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.*;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.security.*")
@PrepareForTest({
    HiveConfigBean.class,
    HiveMetadataProcessor.class,
    BaseHiveIT.class,
    HMSCache.class
})
public class TestHiveMetadataProcessor {

  static final String dbName = "testDB";
  static final String tableName = "testTable";
  static final String location = "/user/hive/warehouse/testDB.db/testTable";

  static final LinkedHashMap<String, HiveTypeInfo> SAMPLE_RECORD1
      = new LinkedHashMap<>(ImmutableMap.of(
      "column1",
      TestHiveMetastoreUtil.generatePrimitiveTypeInfo(HiveType.STRING),
      "column2",
      TestHiveMetastoreUtil.generatePrimitiveTypeInfo(HiveType.INT)
  )
  );

  static final LinkedHashMap<String, HiveTypeInfo> SAMPLE_RECORD2
      = new LinkedHashMap<>(ImmutableMap.of(
      "column1",
      TestHiveMetastoreUtil.generatePrimitiveTypeInfo(HiveType.BOOLEAN),
      "column2",
      TestHiveMetastoreUtil.generatePrimitiveTypeInfo(HiveType.DOUBLE)
  )
  );

  static final LinkedHashMap<String, HiveTypeInfo> SAMPLE_RECORD3
      = new LinkedHashMap<>(ImmutableMap.of(
      "first",
      TestHiveMetastoreUtil.generatePrimitiveTypeInfo(HiveType.STRING),
      "second",
      TestHiveMetastoreUtil.generatePrimitiveTypeInfo(HiveType.INT)
  )
  );

  static final LinkedHashMap<String, HiveTypeInfo> DECIMAL_RECORD1
      = new LinkedHashMap<>(ImmutableMap.of(
      "decimal_val",
      TestHiveMetastoreUtil.generateDecimalTypeInfo(5, 10),
      "long_val",
      TestHiveMetastoreUtil.generatePrimitiveTypeInfo(HiveType.BIGINT)
  )
  );

  static final LinkedHashMap<String, HiveTypeInfo> DECIMAL_RECORD2
      = new LinkedHashMap<>(ImmutableMap.of(
      "decimal_val",
      TestHiveMetastoreUtil.generateDecimalTypeInfo(10, 12),
      "long_value",
      TestHiveMetastoreUtil.generatePrimitiveTypeInfo(HiveType.BIGINT)
  )
  );

  static final LinkedHashMap<String, HiveTypeInfo> SAMPLE_PARTITION
      = new LinkedHashMap<>(ImmutableMap.of(
      "dt",
      TestHiveMetastoreUtil.generatePrimitiveTypeInfo(HiveType.STRING),
      "state",
      TestHiveMetastoreUtil.generatePrimitiveTypeInfo(HiveType.STRING)
  )
  );

  @Before
  public void setup(){
    // do not resolve JDBC URL
    PowerMockito.suppress(
        MemberMatcher.method(
            HiveMetastoreUtil.class,
            "resolveJDBCUrl",
            ELEval.class,
            String.class,
            Record.class)
    );
    // do not run Hive queries
    PowerMockito.suppress(
        MemberMatcher.method(
            HMSCache.class,
            "getOrLoad",
            HMSCacheType.class,
            String.class,
            String.class,
            UserGroupInformation.class
        ));

    // Do not create issues
    PowerMockito.suppress(
        MemberMatcher.method(
            HiveConfigBean.class,
            "init",
            Stage.Context.class,
            String.class,
            List.class
        )
    );
    PowerMockito.replace(
        MemberMatcher.method(
            BaseHiveIT.class,
            "getHiveConfigBean"
        )
    ).with(new InvocationHandler() {
      @Override
      public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        HiveConfigBean bean = BaseHiveIT.getHiveConfigBean();
        bean.setConfiguration(new Configuration());
        return bean;
      }
    });
  }

  /**
   * Utility function to compare the generated Metadata Record with input.
   * This is used for testing both column list and partition list.
   * @param generated: List of name-Field pairs obtained from generated Metadata Record.
   * @param original: Original structure obtained from incoming record.
   */
  private void assertMetadataRecordNewSchemaList(
      LinkedHashMap<String, Field> generated,
      LinkedHashMap<String, HiveTypeInfo> original
  ) {
    for(Map.Entry<String, Field> entry: generated.entrySet()) {
      LinkedHashMap<String, Field> elem = entry.getValue().getValueAsListMap();
      // Both partition and column fields in metadata record has the same key "name"
      String colName = elem.get(HiveMetastoreUtil.COLUMN_NAME).getValueAsString();
      Assert.assertTrue(original.containsKey(colName));
      Assert.assertEquals(
          original.get(colName).getHiveType().toString(),
          elem.get(HiveMetastoreUtil.TYPE_INFO).getValueAsMap().get(HiveMetastoreUtil.TYPE).getValueAsString()
      );
      // Check extraInfo for both decimal and primitive types
      Map<String, Field> extraInfo
          = elem.get(HiveMetastoreUtil.TYPE_INFO).getValueAsMap().get(HiveMetastoreUtil.EXTRA_INFO).getValueAsMap();

      if (original.get(colName).getHiveType() == HiveType.DECIMAL){
        DecimalHiveTypeSupport.DecimalTypeInfo expectedDecimal
            = (DecimalHiveTypeSupport.DecimalTypeInfo)original.get(colName);

        Assert.assertFalse(extraInfo.isEmpty());
        Assert.assertEquals(
            extraInfo.get(DecimalHiveTypeSupport.PRECISION).getValueAsInteger(),
            expectedDecimal.getPrecision()
        );
        Assert.assertEquals(
            extraInfo.get(DecimalHiveTypeSupport.SCALE).getValueAsInteger(),
            expectedDecimal.getScale()
        );
      } else {
        // Type is not decimal, so extraInfo should be empty
        Assert.assertTrue(extraInfo.isEmpty());
      }
    }
  }

  /**
   * Utility function to compare the generated Metadata Record with input.
   * This is used for testing partition name and value.
   * @param partitionList: List of partition name-value pairs obtained from generated Metadata Record.
   * @param expected: Original structure obtained from incoming record.
   */
  private void assertMetadataRecordPartitionList(
      LinkedHashMap<String, Field> partitionList,
      LinkedHashMap<String, String> expected
  ) {
    // Check the detail of partition list
    Iterator<Field> it = partitionList.values().iterator();
    for (Map.Entry<String, String> entry: expected.entrySet()) {
      if (!it.hasNext()) {
        Assert.fail("Number of partition info in metadata record is wrong");
      }
      Field field = it.next();
      LinkedHashMap<String, Field> partition = field.getValueAsListMap();
      Assert.assertEquals(
          partition.get(HiveMetastoreUtil.PARTITION_NAME).getValueAsString(),
          entry.getKey());
      Assert.assertEquals(
          partition.get(HiveMetastoreUtil.PARTITION_VALUE).getValueAsString(),
          entry.getValue());
    }
  }

  /**
   * Utility function to test if the generated record has correct field and values.
   *
   * @param record: generated metadata record
   * @param originalColumn : Table column name-HiveTypeInfo contained in incoming record
   * @param originalPartition: Partition name-HiveTypeInfo contained in incoming record
   * @throws Exception
   */
  private void checkRecordForColumnsAndPartitionList (
      Record record,
      LinkedHashMap<String, HiveTypeInfo>  originalColumn,
      LinkedHashMap<String, HiveTypeInfo>  originalPartition) throws Exception
  {
    Assert.assertEquals(
        HiveMetastoreUtil.getDatabaseName(record),
        dbName);
    Assert.assertEquals(
        HiveMetastoreUtil.getTableName(record),
        tableName);
    Assert.assertEquals(
        HiveMetastoreUtil.getLocation(record),
        location);

    // Metadata Record Type must be TABLE
    Assert.assertEquals(
        record.get(HiveMetastoreUtil.SEP + HiveMetastoreUtil.METADATA_RECORD_TYPE).getValueAsString(),
        HiveMetastoreUtil.MetadataRecordType.TABLE.name());
    Assert.assertNotNull(HiveMetastoreUtil.getAvroSchema(record));

    // Check the detail of column list and test if it contains correct type
    LinkedHashMap<String, Field> columnList
        = record.get(HiveMetastoreUtil.SEP + HiveMetastoreUtil.COLUMNS_FIELD).getValueAsListMap();
    assertMetadataRecordNewSchemaList(columnList, originalColumn);

    if (originalPartition  != null) {
      // Check the detail of partition list
      LinkedHashMap<String, Field> partitionList
          = record.get(HiveMetastoreUtil.SEP + HiveMetastoreUtil.PARTITION_FIELD).getValueAsListMap();
      assertMetadataRecordNewSchemaList(partitionList, originalPartition);
    }
  }

  /**
   * Utility function to build a ProcessRunner
   * @param processor Processor to build a ProcessRunner
   * @return Generated ProcessRunner
   */
  ProcessorRunner getProcessRunner(Processor processor) {
    return new ProcessorRunner.Builder(HiveMetadataDProcessor.class, processor)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .addOutputLane("hdfs")
        .addOutputLane("hive")
        .build();
  }

  @Test
  public void testDetectSchemaChangeNoDiff() throws Exception {
    // Compare SAMPLE_RECORD1 and SAMPLE_RECORD1 (same record)
    TypeInfoCacheSupport.TypeInfo typeInfo
        = new TypeInfoCacheSupport.TypeInfo(SAMPLE_RECORD1, SAMPLE_PARTITION);

    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder().build();
    boolean result = processor.detectSchemaChange(SAMPLE_RECORD1, typeInfo);
    Assert.assertFalse(result);
  }

  @Test
  public void testDetectSchemaChangeCompatible1() throws Exception {
    // Compare SAMPLE_RECORD1 and SAMPLE_RECORD3
    TypeInfoCacheSupport.TypeInfo typeInfo
        = new TypeInfoCacheSupport.TypeInfo(SAMPLE_RECORD1, SAMPLE_PARTITION);

    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder().build();
    boolean result = processor.detectSchemaChange(SAMPLE_RECORD3, typeInfo);
    Assert.assertTrue(result);
  }

  @Test
  public void testDetectSchemaChangeCompatible2() throws Exception {
    // Compare SAMPLE_RECORD2 and SAMPLE_RECORD3
    TypeInfoCacheSupport.TypeInfo typeInfo
        = new TypeInfoCacheSupport.TypeInfo(SAMPLE_RECORD2, SAMPLE_PARTITION);

    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder().build();
    boolean result = processor.detectSchemaChange(SAMPLE_RECORD3, typeInfo);
    Assert.assertTrue(result);
  }

  @Test
  public void testDetectSchemaChangeInCompatibleTypeChange() throws Exception {
    // Compare SAMPLE_RECORD1 and SAMPLE_RECORD2 (Same column name and different type)
    // This should throw StageException

    TypeInfoCacheSupport.TypeInfo typeInfo
        = new  TypeInfoCacheSupport.TypeInfo(SAMPLE_RECORD1, SAMPLE_PARTITION);

    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder().build();
    try {
      processor.detectSchemaChange(SAMPLE_RECORD2, typeInfo);
      Assert.fail("Incompatible schema change");
    } catch (StageException e) {
      Assert.assertEquals("Schema change for Incompatible type should receive StageException",
          Errors.HIVE_21,
          e.getErrorCode());
    }
  }

  @Test
  public void testDetectSchemaChangeInCompatibleDecimals() throws Exception {
    // Compare DECIMAL_RECORD1 and DECIMAL_RECORD2. Incompatible due to different scale and precision
    // This should throw StageException

    TypeInfoCacheSupport.TypeInfo typeInfo
        = new  TypeInfoCacheSupport.TypeInfo(DECIMAL_RECORD1, SAMPLE_PARTITION);

    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder().build();
    try {
      processor.detectSchemaChange(DECIMAL_RECORD2, typeInfo);
      Assert.fail("Incompatible schema change");
    } catch (StageException e) {
      Assert.assertEquals("Schema change for Incompatible type should receive",
          Errors.HIVE_21,
          e.getErrorCode());
    }
  }

  @Test
  public void testGenerateSchemaChangeRecord() throws Exception {
    // Test for the contents of metadata record
    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder().build();

    ProcessorRunner runner = getProcessRunner(processor);
    runner.runInit();

    Record record = processor.generateSchemaChangeRecord(
        dbName,
        tableName,
        SAMPLE_RECORD1,
        SAMPLE_PARTITION,
        location,
        HiveMetastoreUtil.generateAvroSchema( // Using this function since this isn't to test contents of avro schema
            SAMPLE_RECORD1,
            HiveMetastoreUtil.getQualifiedTableName(dbName, tableName)
        )
    );
    checkRecordForColumnsAndPartitionList(record, SAMPLE_RECORD1, SAMPLE_PARTITION);
  }

  @Test
  public void testGenerateSchemaChangeRecordExtraInfo() throws Exception {
    // Test for metadata record that includes decimal values

    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder().build();
    ProcessorRunner runner = getProcessRunner(processor);
    runner.runInit();

    Record record = processor.generateSchemaChangeRecord(
        dbName,
        tableName,
        DECIMAL_RECORD1,
        DECIMAL_RECORD2,   // partition values are decimals
        location,
        HiveMetastoreUtil.generateAvroSchema( // Using this function since this isn't to test contents of avro schema
            DECIMAL_RECORD1,
            HiveMetastoreUtil.getQualifiedTableName(dbName, tableName)
        )
    );
    checkRecordForColumnsAndPartitionList(record, DECIMAL_RECORD1, DECIMAL_RECORD2);
    runner.runDestroy();
  }

  @Test
  public void testGenerateNewPartitionRecord() throws Exception {

    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder().build();
    ProcessorRunner runner = getProcessRunner(processor);
    runner.runInit();

    LinkedHashMap<String, String> sampleValues = new LinkedHashMap<>();
    sampleValues.put("dt", "2016-06-09");
    sampleValues.put("country", "US");
    sampleValues.put("state", "CA");
    Record record = null;

    try {
      record = processor.generateNewPartitionRecord(
          dbName,
          tableName,
          sampleValues,
          location
      );
    } catch (StageException e) {
      Assert.fail("Should not receive StageException");
    }

    Assert.assertEquals(
        HiveMetastoreUtil.getDatabaseName(record),
        dbName);
    Assert.assertEquals(
        HiveMetastoreUtil.getTableName(record),
        tableName);
    Assert.assertEquals(
        HiveMetastoreUtil.getLocation(record),
        location);
    Assert.assertEquals(
        record.get(HiveMetastoreUtil.SEP + HiveMetastoreUtil.METADATA_RECORD_TYPE).getValueAsString(),
        HiveMetastoreUtil.MetadataRecordType.PARTITION.name());

    LinkedHashMap<String, Field> partitions
        = record.get(HiveMetastoreUtil.SEP + HiveMetastoreUtil.PARTITION_FIELD).getValueAsListMap();
    assertMetadataRecordPartitionList(partitions, sampleValues);
    runner.runDestroy();
  }

  @Test
  public void testGetPartitionValueFromRecord() throws Exception {
    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
        .partitions(new PartitionConfigBuilder()
            .addPartition("year", HiveType.STRING, "${YYYY()}")
            .addPartition("month", HiveType.STRING, "${MM()}")
            .addPartition("day", HiveType.STRING, "${DD()}")
            .build()
        )
        .build();
    ProcessorRunner runner = getProcessRunner(processor);
    runner.runInit();

    Map<String, Field> map = new LinkedHashMap<>();
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    ELVars elVars = runner.getContext().createELVars();
    RecordEL.setRecordInContext(elVars, record);

    LinkedHashMap<String, String> values = null;

    Calendar cal = Calendar.getInstance();
    cal.setTime(new Date(System.currentTimeMillis()));

    TimeEL.setCalendarInContext(elVars, cal);
    TimeNowEL.setTimeNowInContext(elVars, new Date(System.currentTimeMillis()));
    String year = String.valueOf(cal.get(Calendar.YEAR));
    String month = String.valueOf(Utils.intToPaddedString(cal.get(Calendar.MONTH) + 1, 2));
    String day =  String.valueOf(Utils.intToPaddedString(cal.get(Calendar.DAY_OF_MONTH), 2));

    try {
      values = processor.getPartitionValuesFromRecord(record);
    } catch (StageException e) {
      Assert.fail("getPartitionValuesFromRecord should not raise StageException");
    }
    Assert.assertEquals(
        "Number of partition name-value pair is wrong",
        3,
        values.size()
    );
    String path = HiveMetastoreUtil.generatePartitionPath(values);
    Assert.assertEquals(Utils.format("/year={}/month={}/day={}", year, month, day), path);
    runner.runDestroy();
  }

  @Test
  public void testRecordHeaderToHDFSRoll() throws Exception {
    Record record = RecordCreator.create();
    String sample = "sample record";
    record.set(Field.create(sample));
    String targetDir = "/user/hive/warehouse/table/dt=2016-05-24/country=US";

    HiveMetadataProcessor.updateRecordForHDFS(record, true, SdcAvroTestUtil.AVRO_SCHEMA, targetDir);
    Assert.assertEquals(record.getHeader().getAttribute(HiveMetadataProcessor.HDFS_HEADER_ROLL), "true");
    Assert.assertEquals(record.getHeader().getAttribute(
        HiveMetadataProcessor.HDFS_HEADER_AVROSCHEMA),
        SdcAvroTestUtil.AVRO_SCHEMA);
    Assert.assertEquals(record.getHeader().getAttribute(
        HiveMetadataProcessor.HDFS_HEADER_TARGET_DIRECTORY),
        targetDir);
  }

  @Test
  public void testRecordHeaderToHDFSNoRoll() throws Exception {
    Record record = RecordCreator.create();
    String sample = "sample record";
    record.set(Field.create(sample));
    String targetDir = "/user/hive/warehouse/table/dt=2016/state=CA";

    HiveMetadataProcessor.updateRecordForHDFS(record, false, SdcAvroTestUtil.AVRO_SCHEMA, targetDir);
    Assert.assertNull(record.getHeader().getAttribute(HiveMetadataProcessor.HDFS_HEADER_ROLL));
    Assert.assertEquals(record.getHeader().getAttribute(
        HiveMetadataProcessor.HDFS_HEADER_AVROSCHEMA),
        SdcAvroTestUtil.AVRO_SCHEMA);
    Assert.assertEquals(record.getHeader().getAttribute(
        HiveMetadataProcessor.HDFS_HEADER_TARGET_DIRECTORY),
        targetDir);
  }

  @Test
  public void testScaleAndPrecision() throws Exception {
    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder().decimalDefaultsConfig(39, 39).build();
    ProcessorRunner runner = getProcessRunner(processor);
    runner.runInit();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("name", Field.create(Field.Type.STRING, "default database"));
    map.put("decimal", Field.create(1.5));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    runner.runProcess(ImmutableList.of(record));
    Assert.assertEquals(runner.getErrorRecords().size(), 1);
    Record errorRecord = runner.getErrorRecords().get(0);

    Assert.assertEquals(
        com.streamsets.pipeline.stage.processor.hive.Errors.HIVE_METADATA_08.name(),
        errorRecord.getHeader().getErrorCode()
    );
    runner.runDestroy();

    processor = new HiveMetadataProcessorBuilder().decimalDefaultsConfig(37, 39).build();
    runner = getProcessRunner(processor);
    runner.runInit();
    runner.runProcess(ImmutableList.of(record));
    Assert.assertEquals(runner.getErrorRecords().size(), 1);
    errorRecord = runner.getErrorRecords().get(0);
    Assert.assertEquals(
        com.streamsets.pipeline.stage.processor.hive.Errors.HIVE_METADATA_08.name(),
        errorRecord.getHeader().getErrorCode()
    );
    runner.runDestroy();

    processor = new HiveMetadataProcessorBuilder().decimalDefaultsConfig(5, 2).build();
    runner = getProcessRunner(processor);
    runner.runInit();
    runner.runProcess(ImmutableList.of(record));
    Assert.assertEquals(runner.getErrorRecords().size(), 1);
    errorRecord = runner.getErrorRecords().get(0);
    Assert.assertEquals(
        com.streamsets.pipeline.stage.processor.hive.Errors.HIVE_METADATA_09.name(),
        errorRecord.getHeader().getErrorCode()
    );
    runner.runDestroy();

    processor = new HiveMetadataProcessorBuilder().decimalDefaultsConfig(1, 1).build();
    runner = getProcessRunner(processor);
    runner.runInit();
    runner.runProcess(ImmutableList.of(record));
    Assert.assertEquals(runner.getErrorRecords().size(), 0);
    runner.runDestroy();
  }

  @Test
  public void testExternalTableDirectoryPathDefault() throws Exception {
    /* database : default
       table    : tbl
       path template : /user/hive/warehouse
       partition template: secret-value
       Expected directory path = /user/hive/some_directory/tbl/secret-value
    */
    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
        .database("")
        .external(true)
        .tablePathTemplate("/user/hive/some_directory/tbl")
        .partitionPathTemplate("secret-value")
        .build();
    ProcessorRunner runner = getProcessRunner(processor);
    runner.runInit();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("name", Field.create(Field.Type.STRING, "default database"));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));

    StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
    Assert.assertEquals(2, output.getRecords().get("hive").size());
    Assert.assertEquals(1, output.getRecords().get("hdfs").size());

    Record hdfsRecord = output.getRecords().get("hdfs").get(0);
    Assert.assertNotNull(hdfsRecord);
    // Target directory with correct path
    Assert.assertEquals(
        "/user/hive/some_directory/tbl/secret-value",
        hdfsRecord.getHeader().getAttribute("targetDirectory")
    );
  }

  @Test
  public void testExternalTableDirectoryPath() throws Exception {
    /* database : testDB
       table    : tbl
       path template : /user/hive/some_directory
       partition path: ${YYYY()}-${MM()}-${DD()}
       Expected directory path
            : /user/hive/some_directory/testDB.db/tbl/<year>-<month>-<date>
    */
    String tableTemplate = "/user/hive/some_directory/testDB.db/tbl";
    String partitionTemplate = "${YYYY()}-${MM()}-${DD()}";
    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
        .database("testDB")
        .external(true)
        .tablePathTemplate(tableTemplate)
        .partitionPathTemplate(partitionTemplate)
        .build();
    ProcessorRunner runner = getProcessRunner(processor);
    runner.runInit();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("name", Field.create(Field.Type.STRING, "default database"));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    ELVars elVars = runner.getContext().createELVars();
    RecordEL.setRecordInContext(elVars, record);

    StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
    Assert.assertEquals(2, output.getRecords().get("hive").size());
    Assert.assertEquals(1, output.getRecords().get("hdfs").size());

    Record hdfsRecord = output.getRecords().get("hdfs").get(0);
    Assert.assertNotNull(hdfsRecord);

    // Obtain Today's date
    Calendar cal = Calendar.getInstance();
    cal.setTime(new Date(System.currentTimeMillis()));
    TimeEL.setCalendarInContext(elVars, cal);
    TimeNowEL.setTimeNowInContext(elVars, new Date(System.currentTimeMillis()));
    String year = String.valueOf(cal.get(Calendar.YEAR));
    String month = String.valueOf(Utils.intToPaddedString(cal.get(Calendar.MONTH) + 1, 2));
    String day =  String.valueOf(Utils.intToPaddedString(cal.get(Calendar.DAY_OF_MONTH), 2));

    // Target directory with correct path
    String expected = String.format("%s/%s-%s-%s", tableTemplate, year, month, day);
    Assert.assertEquals(
        expected,
        hdfsRecord.getHeader().getAttribute("targetDirectory")
    );
  }
}
