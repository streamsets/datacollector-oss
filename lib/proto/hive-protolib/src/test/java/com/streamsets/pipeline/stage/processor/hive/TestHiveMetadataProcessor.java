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
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.lib.util.SdcAvroTestUtil;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.BaseHiveIT;
import com.streamsets.pipeline.stage.HiveMetadataProcessorBuilder;
import com.streamsets.pipeline.stage.PartitionConfigBuilder;
import com.streamsets.pipeline.stage.lib.hive.Errors;
import com.streamsets.pipeline.stage.lib.hive.HiveConfigBean;
import com.streamsets.pipeline.stage.lib.hive.HiveMetastoreUtil;
import com.streamsets.pipeline.stage.lib.hive.HiveQueryExecutor;
import com.streamsets.pipeline.stage.lib.hive.TestHiveMetastoreUtil;
import com.streamsets.pipeline.stage.lib.hive.cache.HMSCache;
import com.streamsets.pipeline.stage.lib.hive.cache.HMSCacheType;
import com.streamsets.pipeline.stage.lib.hive.typesupport.DecimalHiveTypeSupport;
import com.streamsets.pipeline.stage.lib.hive.typesupport.HiveType;
import com.streamsets.pipeline.stage.lib.hive.typesupport.HiveTypeInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.api.support.membermodification.MemberMatcher;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.security.*")
@PrepareForTest({
    HiveConfigBean.class,
    HiveMetadataProcessor.class,
    HiveMetastoreUtil.class,
    BaseHiveIT.class,
    HMSCache.class,
    HMSCache.Builder.class,
    HiveQueryExecutor.class
})
public class TestHiveMetadataProcessor {

  static final String dbName = "testDB";
  static final String tableName = "testTable";
  static final String location = "/user/hive/warehouse/testDB.db/testTable";
  static final TimeZone timezone = TimeZone.getTimeZone("UTC");

  static final LinkedHashMap<String, HiveTypeInfo> SAMPLE_RECORD1
      = new LinkedHashMap<>(ImmutableMap.of(
      "column1",
      TestHiveMetastoreUtil.generatePrimitiveTypeInfo(HiveType.STRING, "column1"),
      "column2",
      TestHiveMetastoreUtil.generatePrimitiveTypeInfo(HiveType.INT, "column2")
  )
  );

  static final LinkedHashMap<String, HiveTypeInfo> SAMPLE_RECORD2
      = new LinkedHashMap<>(ImmutableMap.of(
      "column1",
      TestHiveMetastoreUtil.generatePrimitiveTypeInfo(HiveType.BOOLEAN, "column1"),
      "column2",
      TestHiveMetastoreUtil.generatePrimitiveTypeInfo(HiveType.DOUBLE, "column2")
  )
  );

  static final LinkedHashMap<String, HiveTypeInfo> SAMPLE_RECORD3
      = new LinkedHashMap<>(ImmutableMap.of(
      "first",
      TestHiveMetastoreUtil.generatePrimitiveTypeInfo(HiveType.STRING, "first"),
      "second",
      TestHiveMetastoreUtil.generatePrimitiveTypeInfo(HiveType.INT, "second")
  )
  );

  static final LinkedHashMap<String, HiveTypeInfo> DECIMAL_RECORD1
      = new LinkedHashMap<>(ImmutableMap.of(
      "decimal_val",
      TestHiveMetastoreUtil.generateDecimalTypeInfo("decimal_val", 5, 10),
      "long_val",
      TestHiveMetastoreUtil.generatePrimitiveTypeInfo(HiveType.BIGINT, "long_val")
  )
  );

  static final LinkedHashMap<String, HiveTypeInfo> DECIMAL_RECORD2
      = new LinkedHashMap<>(ImmutableMap.of(
      "decimal_val",
      TestHiveMetastoreUtil.generateDecimalTypeInfo("decimal_val", 10, 12),
      "long_value",
      TestHiveMetastoreUtil.generatePrimitiveTypeInfo(HiveType.BIGINT, "long_value")
  )
  );

  static final LinkedHashMap<String, HiveTypeInfo> SAMPLE_PARTITION
      = new LinkedHashMap<>(ImmutableMap.of(
      "dt",
      TestHiveMetastoreUtil.generatePrimitiveTypeInfo(HiveType.STRING, "dt"),
      "state",
      TestHiveMetastoreUtil.generatePrimitiveTypeInfo(HiveType.STRING, "state")
  )
  );

  static final Map<String, String> SAMPLE_HEADERS
      = new LinkedHashMap<>(ImmutableMap.of(
      "sample_header_1",
      "sample_value_1",
      "sample_header_2",
      "sample_value_2"
  )
  );

  @Before
  public void setup() throws Exception {
    // do not resolve JDBC URL
    PowerMockito.spy(HiveMetastoreUtil.class);

    // do not run Hive queries
    PowerMockito.suppress(
        MemberMatcher.method(
            HMSCache.class,
            "getOrLoad",
            HMSCacheType.class,
            String.class,
            HiveQueryExecutor.class
        )
    );
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
            HiveConfigBean.class,
            "getHiveConnection"
        )
    ).with(new InvocationHandler() {
      @Override
      public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        return null;
      }
    });

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
        bean.setHiveConf(new HiveConf());
        return bean;
      }
    });

    PowerMockito.replace(MemberMatcher.method(HiveQueryExecutor.class, "executeDescribeDatabase"))
      .with((proxy, method, args) -> "/user/hive/warehouse/" + args[0].toString() + ".db/");

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

      // Assert column comment (should be the same as column name in this test case)
      String comment = elem.get(HiveMetastoreUtil.TYPE_INFO).getValueAsMap().get(HiveMetastoreUtil.COMMENT).getValueAsString();
      Assert.assertEquals(comment, colName);

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
        ),
        SAMPLE_HEADERS
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
        ),
        SAMPLE_HEADERS
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
          location,
          SAMPLE_HEADERS
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
        .timeZone(timezone)
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
    cal.setTimeZone(timezone);

    TimeEL.setCalendarInContext(elVars, cal);
    TimeNowEL.setTimeNowInContext(elVars, new Date(System.currentTimeMillis()));
    String year = String.valueOf(cal.get(Calendar.YEAR));
    String month = String.valueOf(Utils.intToPaddedString(cal.get(Calendar.MONTH) + 1, 2));
    String day =  String.valueOf(Utils.intToPaddedString(cal.get(Calendar.DAY_OF_MONTH), 2));

    try {
      values = processor.getPartitionValuesFromRecord(elVars);
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
    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder().decimalConfig(39, 39).build();
    ProcessorRunner runner = getProcessRunner(processor);
    runner.runInit();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("name", Field.create(Field.Type.STRING, "default database"));
    map.put("decimal", Field.create(new BigDecimal(1.5)));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    runner.runProcess(ImmutableList.of(record));
    Assert.assertEquals(1, runner.getErrorRecords().size());
    Record errorRecord = runner.getErrorRecords().get(0);

    Assert.assertEquals(
        com.streamsets.pipeline.stage.processor.hive.Errors.HIVE_METADATA_07.name(),
        errorRecord.getHeader().getErrorCode()
    );
    runner.runDestroy();

    processor = new HiveMetadataProcessorBuilder().decimalConfig(39, 37).build();
    runner = getProcessRunner(processor);
    runner.runInit();
    runner.runProcess(ImmutableList.of(record));
    Assert.assertEquals(1, runner.getErrorRecords().size());
    errorRecord = runner.getErrorRecords().get(0);
    Assert.assertEquals(
        com.streamsets.pipeline.stage.processor.hive.Errors.HIVE_METADATA_07.name(),
        errorRecord.getHeader().getErrorCode()
    );
    runner.runDestroy();

    processor = new HiveMetadataProcessorBuilder().decimalConfig(2, 5).build();
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

    processor = new HiveMetadataProcessorBuilder().decimalConfig(2, 1).build();
    runner = getProcessRunner(processor);
    runner.runInit();
    runner.runProcess(ImmutableList.of(record));
    Assert.assertEquals(0, runner.getErrorRecords().size());
    runner.runDestroy();

    // Invalid precision evaluation
    processor = new HiveMetadataProcessorBuilder()
      .decimalConfig(
        "${record:attribute(str:concat(str:concat('jdbc.', field:field()), '.precision'))}",
        "2")
      .build();
    runner = getProcessRunner(processor);
    runner.runInit();
    runner.runProcess(ImmutableList.of(record));
    Assert.assertEquals(1, runner.getErrorRecords().size());
    errorRecord = runner.getErrorRecords().get(0);
    Assert.assertEquals(Errors.HIVE_29.name(), errorRecord.getHeader().getErrorCode());
    Assert.assertTrue(errorRecord.getHeader().getErrorMessage().contains("Can't calculate precision for field 'decimal'"));
    runner.runDestroy();

    // Invalid scale evaluation
    processor = new HiveMetadataProcessorBuilder()
      .decimalConfig(
        "2",
        "${record:attribute(str:concat(str:concat('jdbc.', field:field()), '.scale'))}")
      .build();
    runner = getProcessRunner(processor);
    runner.runInit();
    runner.runProcess(ImmutableList.of(record));
    Assert.assertEquals(1, runner.getErrorRecords().size());
    errorRecord = runner.getErrorRecords().get(0);
    Assert.assertEquals(Errors.HIVE_29.name(), errorRecord.getHeader().getErrorCode());
    Assert.assertTrue(errorRecord.getHeader().getErrorMessage().contains("Can't calculate scale for field 'decimal'"));
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
        .timeZone(timezone)
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
    cal.setTimeZone(timezone);
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
