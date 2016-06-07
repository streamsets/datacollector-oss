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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.destination.hive;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.stage.HiveMetastoreTargetBuilder;
import com.streamsets.pipeline.stage.lib.hive.Errors;
import com.streamsets.pipeline.stage.lib.hive.HiveMetastoreUtil;
import com.streamsets.pipeline.stage.lib.hive.HiveQueryExecutor;
import com.streamsets.pipeline.stage.lib.hive.TestHMSCache;
import com.streamsets.pipeline.stage.lib.hive.TestHiveMetastoreUtil;
import com.streamsets.pipeline.stage.lib.hive.cache.AvroSchemaInfoCacheSupport;
import com.streamsets.pipeline.stage.lib.hive.cache.HMSCache;
import com.streamsets.pipeline.stage.lib.hive.cache.HMSCacheSupport;
import com.streamsets.pipeline.stage.lib.hive.cache.HMSCacheType;
import com.streamsets.pipeline.stage.lib.hive.cache.PartitionInfoCacheSupport;
import com.streamsets.pipeline.stage.lib.hive.cache.TBLPropertiesInfoCacheSupport;
import com.streamsets.pipeline.stage.lib.hive.cache.TypeInfoCacheSupport;
import com.streamsets.pipeline.stage.lib.hive.typesupport.HiveType;
import com.streamsets.pipeline.stage.lib.hive.typesupport.HiveTypeInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.api.support.membermodification.MemberMatcher;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
    HiveMetastoreTarget.class,
    HMSTargetConfigBean.class,
    HiveMetastoreUtil.class,
    HMSCache.class,
    HMSCacheSupport.HMSCacheLoader.class,
    TBLPropertiesInfoCacheSupport.TBLPropertiesInfoCacheLoader.class,
    TypeInfoCacheSupport.TypeInfoCacheLoader.class,
    PartitionInfoCacheSupport.PartitionInfoCacheLoader.class,
    HiveQueryExecutor.class
})
@PowerMockIgnore("javax.security.*")
public class TestHiveMetastoreTarget {
  private static final Logger LOG = LoggerFactory.getLogger(TestHiveMetastoreTarget.class);

  @Before
  public void setup() throws Exception{
    //Don't resolve JDBC
    PowerMockito.spy(HiveMetastoreUtil.class);
    PowerMockito.suppress(MemberMatcher.method(
        HMSTargetConfigBean.class,
        "init",
        Stage.Context.class,
        String.class,
        List.class
    ));
    PowerMockito.suppress(
        MemberMatcher.method(
            HiveMetastoreUtil.class,
            "resolveJDBCUrl",
            ELEval.class,
            String.class,
            Record.class
        )
    );
    //Suppress queries
    PowerMockito.suppress(MemberMatcher.method(HiveQueryExecutor.class, "executeCreateTableQuery"));
    PowerMockito.suppress(MemberMatcher.method(HiveQueryExecutor.class, "executeAlterTableAddColumnsQuery"));
    PowerMockito.suppress(MemberMatcher.method(HiveQueryExecutor.class, "executeAlterTableAddPartitionQuery"));
  }

  private LinkedHashMap<String, HiveTypeInfo> generatePartitionTypeInfo() {
    LinkedHashMap<String, HiveTypeInfo> partitionTypeInfo = new LinkedHashMap<>();
    partitionTypeInfo.put("dt", TestHiveMetastoreUtil.generatePrimitiveTypeInfo(HiveType.STRING));
    return partitionTypeInfo;
  }

  private LinkedHashMap<String, HiveTypeInfo> generateColumnTypeInfo() {
    LinkedHashMap<String, HiveTypeInfo> columnTypeInfo = new LinkedHashMap<>();
    columnTypeInfo.put("id", TestHiveMetastoreUtil.generatePrimitiveTypeInfo(HiveType.STRING));
    columnTypeInfo.put("int", TestHiveMetastoreUtil.generatePrimitiveTypeInfo(HiveType.INT));
    columnTypeInfo.put("decimal", TestHiveMetastoreUtil.generateDecimalTypeInfo(10, 5));
    columnTypeInfo.put("string", TestHiveMetastoreUtil.generatePrimitiveTypeInfo(HiveType.STRING));
    return columnTypeInfo;
  }

  private LinkedHashMap<String, String> generatePartitionValueInfo(String value) {
    LinkedHashMap<String, String> partitionValues = new LinkedHashMap<>();
    partitionValues.put("dt", value);
    return partitionValues;
  }

  private Record createSchemaChangeRecordWithMissingFields(String missingField) throws StageException{
    Record r = RecordCreator.create();
    Field f = HiveMetastoreUtil.newSchemaMetadataFieldBuilder(
        "default",
        "sample",
        generateColumnTypeInfo(),
        generatePartitionTypeInfo(),
        true,
        "/user/hive/warehouse/sample",
        ""
    );
    Map<String, Field> fieldMap = f.getValueAsMap();
    fieldMap.remove(missingField);
    r.set(Field.create(fieldMap));
    return r;
  }

  private Record createPartitionAdditionRecordWithMissingFields(String missingField) throws StageException{
    Record r = RecordCreator.create();
    Field f = HiveMetastoreUtil.newPartitionMetadataFieldBuilder(
        "default",
        "sample",
        generatePartitionValueInfo("12-25-2015"),
        "/user/hive/warehouse/sample"
    );
    Map<String, Field> fieldMap = f.getValueAsMap();
    fieldMap.remove(missingField);
    r.set(Field.create(fieldMap));
    return r;
  }

  private List<Record> generateRecordWithMissingField(
      String missingField,
      HiveMetastoreUtil.MetadataRecordType metadataRecordType
  ) throws StageException {
    return Collections.singletonList(
        metadataRecordType== HiveMetastoreUtil.MetadataRecordType.TABLE ?
            createSchemaChangeRecordWithMissingFields(missingField)
            : createPartitionAdditionRecordWithMissingFields(missingField)
    );
  }

  private void runHMSTargetWriteAndCheckException(
      TargetRunner targetRunner,
      List<Record> records
  ) {
    try {
      targetRunner.runWrite(records);
      Assert.fail("Stage exception expected for missing/invalid field in record:" + records.toString());
    } catch (StageException e) {
      LOG.info("Expected exception: {}", e.getMessage());
      Assert.assertEquals("Error codes did not match", Errors.HIVE_17, e.getErrorCode());
    }
  }

  @Test
  public void testCommonMissingInfoMetadataRecord() throws Exception {
    TestHMSCache.setMockForHMSCacheLoader(
        TestHMSCache.EMPTY_TYPE_INFO,
        TestHMSCache.EMPTY_TYPE_INFO,
        TestHMSCache.EMPTY_PARTITION_INFO,
        false,
        true
    );

    HiveMetastoreTarget target = PowerMockito.spy(new HiveMetastoreTargetBuilder().build());
    TargetRunner targetRunner = new TargetRunner.Builder(HiveMetastoreDTarget.class, target).build();
    targetRunner.runInit();

    //Test schema change record
    runHMSTargetWriteAndCheckException(
        targetRunner,
        generateRecordWithMissingField(
            HiveMetastoreUtil.VERSION,
            HiveMetastoreUtil.MetadataRecordType.TABLE
        )
    );
    runHMSTargetWriteAndCheckException(
        targetRunner,
        generateRecordWithMissingField(
            HiveMetastoreUtil.METADATA_RECORD_TYPE,
            HiveMetastoreUtil.MetadataRecordType.TABLE
        )
    );
    runHMSTargetWriteAndCheckException(
        targetRunner,
        generateRecordWithMissingField(
            HiveMetastoreUtil.DATABASE_FIELD,
            HiveMetastoreUtil.MetadataRecordType.TABLE
        )
    );
    runHMSTargetWriteAndCheckException(
        targetRunner,
        generateRecordWithMissingField(
            HiveMetastoreUtil.TABLE_FIELD,
            HiveMetastoreUtil.MetadataRecordType.TABLE
        )
    );
    runHMSTargetWriteAndCheckException(
        targetRunner,
        generateRecordWithMissingField(
            HiveMetastoreUtil.LOCATION_FIELD,
            HiveMetastoreUtil.MetadataRecordType.TABLE
        )
    );

    //Test Partition Addition Record
    runHMSTargetWriteAndCheckException(
        targetRunner,
        generateRecordWithMissingField(
            HiveMetastoreUtil.VERSION,
            HiveMetastoreUtil.MetadataRecordType.PARTITION
        )
    );
    runHMSTargetWriteAndCheckException(
        targetRunner,
        generateRecordWithMissingField(
            HiveMetastoreUtil.METADATA_RECORD_TYPE,
            HiveMetastoreUtil.MetadataRecordType.PARTITION
        )
    );
    runHMSTargetWriteAndCheckException(
        targetRunner,
        generateRecordWithMissingField(
            HiveMetastoreUtil.DATABASE_FIELD,
            HiveMetastoreUtil.MetadataRecordType.PARTITION
        )
    );
    runHMSTargetWriteAndCheckException(
        targetRunner,
        generateRecordWithMissingField(
            HiveMetastoreUtil.TABLE_FIELD,
            HiveMetastoreUtil.MetadataRecordType.PARTITION
        )
    );
    runHMSTargetWriteAndCheckException(
        targetRunner,
        generateRecordWithMissingField(
            HiveMetastoreUtil.LOCATION_FIELD,
            HiveMetastoreUtil.MetadataRecordType.PARTITION
        )
    );
    targetRunner.runDestroy();
  }

  @Test
  public void testInvalidSchemaChangeRecord() throws Exception{
    TestHMSCache.setMockForHMSCacheLoader(
        TestHMSCache.EMPTY_TYPE_INFO,
        TestHMSCache.EMPTY_TYPE_INFO,
        TestHMSCache.EMPTY_PARTITION_INFO,
        false,
        true
    );
    HiveMetastoreTarget target = PowerMockito.spy(new HiveMetastoreTargetBuilder().build());
    TargetRunner targetRunner = new TargetRunner.Builder(HiveMetastoreDTarget.class, target).build();
    targetRunner.runInit();
    runHMSTargetWriteAndCheckException(
        targetRunner,
        generateRecordWithMissingField(
            HiveMetastoreUtil.INTERNAL_FIELD,
            HiveMetastoreUtil.MetadataRecordType.TABLE
        )
    );
    runHMSTargetWriteAndCheckException(
        targetRunner,
        generateRecordWithMissingField(
            HiveMetastoreUtil.COLUMNS_FIELD,
            HiveMetastoreUtil.MetadataRecordType.TABLE
        )
    );
    runHMSTargetWriteAndCheckException(
        targetRunner,
        generateRecordWithMissingField(
            HiveMetastoreUtil.PARTITION_FIELD,
            HiveMetastoreUtil.MetadataRecordType.TABLE
        )
    );

    //Check column type removal

    Map<String, Field> fieldMap =
        generateRecordWithMissingField("", HiveMetastoreUtil.MetadataRecordType.TABLE).get(0).get().getValueAsMap();
    List<Field> columnList = fieldMap.get(HiveMetastoreUtil.COLUMNS_FIELD).getValueAsList();
    Map<String, Field> idColumn = columnList.get(0).getValueAsMap();
    Map<String, Field> typeInfoColumn = idColumn.get(HiveMetastoreUtil.TYPE_INFO).getValueAsMap();
    typeInfoColumn.remove(HiveMetastoreUtil.TYPE);

    Record schemaChangeRecord = RecordCreator.create();
    schemaChangeRecord.set(Field.create(fieldMap));
    runHMSTargetWriteAndCheckException(targetRunner, Collections.singletonList(schemaChangeRecord));

    //Check column extra info removal
    fieldMap = generateRecordWithMissingField("", HiveMetastoreUtil.MetadataRecordType.TABLE).get(0).get().getValueAsMap();
    columnList = fieldMap.get(HiveMetastoreUtil.COLUMNS_FIELD).getValueAsList();
    idColumn = columnList.get(0).getValueAsMap();
    typeInfoColumn = idColumn.get(HiveMetastoreUtil.TYPE_INFO).getValueAsMap();
    typeInfoColumn.remove(HiveMetastoreUtil.EXTRA_INFO);

    schemaChangeRecord = RecordCreator.create();
    schemaChangeRecord.set(Field.create(fieldMap));
    runHMSTargetWriteAndCheckException(targetRunner, Collections.singletonList(schemaChangeRecord));

    //Check decimal column extra info removal
    fieldMap = generateRecordWithMissingField("", HiveMetastoreUtil.MetadataRecordType.TABLE).get(0).get().getValueAsMap();
    //Remove scale
    columnList = fieldMap.get(HiveMetastoreUtil.COLUMNS_FIELD).getValueAsList();
    Map<String, Field> decimalColumn = columnList.get(2).getValueAsMap();
    typeInfoColumn = decimalColumn.get(HiveMetastoreUtil.TYPE_INFO).getValueAsMap();
    Map<String, Field> extraInfo = typeInfoColumn.get(HiveMetastoreUtil.EXTRA_INFO).getValueAsMap();
    extraInfo.remove("scale");

    schemaChangeRecord = RecordCreator.create();
    schemaChangeRecord.set(Field.create(fieldMap));
    runHMSTargetWriteAndCheckException(targetRunner, Collections.singletonList(schemaChangeRecord));

    //Remove precision
    extraInfo.put("scale", Field.create(10));
    extraInfo.remove("precision");

    schemaChangeRecord = RecordCreator.create();
    schemaChangeRecord.set(Field.create(fieldMap));
    runHMSTargetWriteAndCheckException(targetRunner, Collections.singletonList(schemaChangeRecord));
    targetRunner.runDestroy();
  }

  @Test
  public void testMismatchExternalFlagDuringSchemaChange() throws Exception{
    TestHMSCache.setMockForHMSCacheLoader(
        generateColumnTypeInfo(),
        generatePartitionTypeInfo(),
        TestHMSCache.EMPTY_PARTITION_INFO,
        true,
        true
    );
    HiveMetastoreTarget target = PowerMockito.spy(new HiveMetastoreTargetBuilder().build());
    TargetRunner targetRunner = new TargetRunner.Builder(HiveMetastoreDTarget.class, target).build();
    targetRunner.runInit();
    //Trying to see whether we can send a record with internal whereas the table is external.
    try {
      targetRunner.runWrite(generateRecordWithMissingField("", HiveMetastoreUtil.MetadataRecordType.TABLE));
      Assert.fail("Stage exception expected for TableProperties mismatch");
    } catch (StageException e) {
      LOG.info("Expected exception: {}", e.getMessage());
      Assert.assertEquals("Error codes did not match", Errors.HIVE_23, e.getErrorCode());
    }
    targetRunner.runDestroy();
  }

  @Test
  public void testInvalidPartitionAdditionRecord() throws Exception{
    TestHMSCache.setMockForHMSCacheLoader(
        generateColumnTypeInfo(),
        generatePartitionTypeInfo(),
        TestHMSCache.EMPTY_PARTITION_INFO,
        false,
        true
    );
    HiveMetastoreTarget target = PowerMockito.spy(new HiveMetastoreTargetBuilder().build());
    TargetRunner targetRunner = new TargetRunner.Builder(HiveMetastoreDTarget.class, target).build();
    targetRunner.runInit();
    runHMSTargetWriteAndCheckException(
        targetRunner,
        generateRecordWithMissingField(
            HiveMetastoreUtil.PARTITION_FIELD,
            HiveMetastoreUtil.MetadataRecordType.PARTITION
        )
    );

    //Partition Name removal
    Map<String, Field> fieldMap
        = generateRecordWithMissingField("", HiveMetastoreUtil.MetadataRecordType.PARTITION).get(0).get().getValueAsMap();

    List<Field> partitions = fieldMap.get(HiveMetastoreUtil.PARTITION_FIELD).getValueAsList();
    Map<String, Field> firstPartition = partitions.get(0).getValueAsMap();
    firstPartition.remove(HiveMetastoreUtil.PARTITION_NAME);
    Record partitionAdditionRecord = RecordCreator.create();
    partitionAdditionRecord.set(Field.create(fieldMap));
    runHMSTargetWriteAndCheckException(
        targetRunner,
        Collections.singletonList(partitionAdditionRecord)
    );

    fieldMap = generateRecordWithMissingField("", HiveMetastoreUtil.MetadataRecordType.PARTITION).get(0).get().getValueAsMap();
    firstPartition = fieldMap.get(HiveMetastoreUtil.PARTITION_FIELD).getValueAsList().get(0).getValueAsMap();
    firstPartition.remove(HiveMetastoreUtil.PARTITION_VALUE);
    partitionAdditionRecord = RecordCreator.create();
    partitionAdditionRecord.set(Field.create(fieldMap));
    partitionAdditionRecord.set(Field.create(fieldMap));
    runHMSTargetWriteAndCheckException(
        targetRunner,
        Collections.singletonList(partitionAdditionRecord)
    );

    targetRunner.runDestroy();
  }

  @Test
  public void testPartitionAdditionForNonExistingTable() throws Exception{
    TestHMSCache.setMockForHMSCacheLoader(
        TestHMSCache.EMPTY_TYPE_INFO,
        TestHMSCache.EMPTY_TYPE_INFO,
        TestHMSCache.EMPTY_PARTITION_INFO,
        false,
        true
    );
    HiveMetastoreTarget target = PowerMockito.spy(new HiveMetastoreTargetBuilder().build());
    TargetRunner targetRunner = new TargetRunner.Builder(HiveMetastoreDTarget.class, target).build();
    targetRunner.runInit();
    //So table does not exist which means the partition addition should throw exception
    try {
      targetRunner.runWrite(generateRecordWithMissingField("", HiveMetastoreUtil.MetadataRecordType.PARTITION));
      Assert.fail("Partition addition without table presence should fail");
    } catch (StageException e) {
      Assert.assertEquals("Error code mismatch", Errors.HIVE_25, e.getErrorCode());
    }
    targetRunner.runDestroy();
  }

  @Test
  public void testValidRecord() throws Exception {
    TestHMSCache.setMockForHMSCacheLoader(
        TestHMSCache.EMPTY_TYPE_INFO,
        TestHMSCache.EMPTY_TYPE_INFO,
        TestHMSCache.EMPTY_PARTITION_INFO,
        false,
        true
    );
    HiveMetastoreTarget target = PowerMockito.spy(new HiveMetastoreTargetBuilder().build());
    TargetRunner targetRunner = new TargetRunner.Builder(HiveMetastoreDTarget.class, target).build();
    targetRunner.runInit();

    List<Record> records = new ArrayList<>(generateRecordWithMissingField("", HiveMetastoreUtil.MetadataRecordType.TABLE));
    records.addAll(generateRecordWithMissingField("", HiveMetastoreUtil.MetadataRecordType.PARTITION));

    targetRunner.runWrite(records);

    HMSCache hmsCache = (HMSCache) Whitebox.getInternalState(target, "hmsCache");
    TypeInfoCacheSupport.TypeInfo typeInfo =
        hmsCache.getIfPresent(HMSCacheType.TYPE_INFO, "default.sample");
    PartitionInfoCacheSupport.PartitionInfo partitionInfo =
        hmsCache.getIfPresent(HMSCacheType.PARTITION_VALUE_INFO, "default.sample");

    LinkedHashMap<String, HiveTypeInfo> expectedColumnTypeInfo = generateColumnTypeInfo();
    LinkedHashMap<String, HiveTypeInfo> expectedPartitionTypeInfo = generatePartitionTypeInfo();
    Set<LinkedHashMap<String, String>> expectedPartitionValueInfo = new HashSet<>();
    expectedPartitionValueInfo.add(generatePartitionValueInfo("12-25-2015"));

    Assert.assertEquals(expectedColumnTypeInfo, typeInfo.getColumnTypeInfo());
    Assert.assertEquals(expectedPartitionTypeInfo, typeInfo.getPartitionTypeInfo());
    Assert.assertEquals(expectedPartitionValueInfo, partitionInfo.getPartitions());
    targetRunner.runDestroy();
  }
}
