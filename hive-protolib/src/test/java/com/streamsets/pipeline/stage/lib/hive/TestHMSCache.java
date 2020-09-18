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
package com.streamsets.pipeline.stage.lib.hive;

import com.google.common.base.Optional;
import com.streamsets.pipeline.api.StageException;
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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
    HMSCache.class,
    HiveQueryExecutor.class,
    HMSCacheSupport.HMSCacheLoader.class,
    TBLPropertiesInfoCacheSupport.TBLPropertiesInfoCacheLoader.class,
    TypeInfoCacheSupport.TypeInfoCacheLoader.class,
    PartitionInfoCacheSupport.PartitionInfoCacheLoader.class,
    AvroSchemaInfoCacheSupport.AvroSchemaInfoCacheLoader.class
})
@PowerMockIgnore({
    "javax.security.*",
    "jdk.internal.reflect.*"
})
public class TestHMSCache {
  private static final String HMS_CACHE_LOADER_LOAD_METHOD = "load";
  private static final String qualifiedTableName = "default.sample";
  public static final LinkedHashMap<String, HiveTypeInfo> EMPTY_TYPE_INFO = new LinkedHashMap<>();
  public static final Map<PartitionInfoCacheSupport.PartitionValues, String> EMPTY_PARTITION_INFO = new HashMap<>();
  private static final HiveQueryExecutor queryExecutor = Mockito.mock(HiveQueryExecutor.class);

  private HMSCache hmsCache;

  /**
   * Creates mock implementations for
   * different {@link com.streamsets.pipeline.stage.lib.hive.cache.HMSCacheSupport.HMSCacheLoader}
   *
   * @param columnTypeInfo    column name and type information.
   * @param partitionTypeInfo partition name and type information (used only when column type info is present)
   * @param partitionInfo     partition name and values (used only when column type info is present).
   * @param external          external table (used only when column type info is present)
   * @param asAvro            as avro (used only when column type info is present)
   */
  public static void setMockForHMSCacheLoader(
      final LinkedHashMap<String, HiveTypeInfo> columnTypeInfo,
      final LinkedHashMap<String, HiveTypeInfo> partitionTypeInfo,
      final Map<PartitionInfoCacheSupport.PartitionValues, String> partitionInfo,
      final boolean external,
      final boolean asAvro
  ) throws Exception {
    Method cacheLoadMethod =
        HMSCacheSupport.HMSCacheLoader.class.getDeclaredMethod(HMS_CACHE_LOADER_LOAD_METHOD, String.class);
    PowerMockito.replace(cacheLoadMethod).with(new InvocationHandler() {
      @Override
      public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        Object returnVal = null;
        if (!columnTypeInfo.isEmpty()) {
          if (proxy.getClass() == TBLPropertiesInfoCacheSupport.TBLPropertiesInfoCacheLoader.class) {
            returnVal = new TBLPropertiesInfoCacheSupport.TBLPropertiesInfo(
                new TBLPropertiesInfoCacheSupport.TBLProperties(external, asAvro, HiveMetastoreUtil.AVRO_SERDE)
            );
          } else if (proxy.getClass() == TypeInfoCacheSupport.TypeInfoCacheLoader.class) {
            returnVal = new TypeInfoCacheSupport.TypeInfo(columnTypeInfo, partitionTypeInfo);
          } else if (proxy.getClass() == PartitionInfoCacheSupport.PartitionInfoCacheLoader.class) {
            returnVal = new PartitionInfoCacheSupport.PartitionInfo(partitionInfo, PowerMockito.mock(HiveQueryExecutor.class), "");
          } else if (proxy.getClass() == AvroSchemaInfoCacheSupport.AvroSchemaInfoCacheLoader.class) {
            Method avroLoadHMSCacheInfoMethod = proxy.getClass().getDeclaredMethod("loadHMSCacheInfo", String.class);
            returnVal = avroLoadHMSCacheInfoMethod.invoke(proxy, args);
          }
        }
        return returnVal != null ? Optional.of(returnVal) : Optional.absent();
      }
    });
  }

  @Test
  public void testInvalidHMSCache() throws Exception {
    try {
      hmsCache = HMSCache.newCacheBuilder().build();
      Assert.fail("Cache should have supported cache types");
    } catch (Exception e) {
      //Expected exception
    }
    //Trying to fetch an unsupported cache type
    try {
      hmsCache = HMSCache.newCacheBuilder()
          .addCacheTypeSupport(HMSCacheType.TBLPROPERTIES_INFO)
          .build();
      hmsCache.getIfPresent(HMSCacheType.TYPE_INFO, qualifiedTableName);
      Assert.fail("Unsupported cache types should fail");
    } catch (StageException e) {
      Assert.assertEquals("Error code did not match", Errors.HIVE_16, e.getErrorCode());
    }
  }

  private void buildSingleTypeCache(
      HMSCacheType hmsCacheType,
      long maxCachSize
  ) throws Exception {
    hmsCache = HMSCache.newCacheBuilder()
        .addCacheTypeSupport(hmsCacheType)
        .maxCacheSize(maxCachSize)
        .build();
  }

  private void checkCacheType(Class expected, HMSCacheSupport.HMSCacheInfo hmsCacheInfo) {
    Assert.assertNotNull(hmsCacheInfo);
    Assert.assertEquals("Cache class type mismatch", expected, hmsCacheInfo.getClass());
  }

  @Test
  public void testTblPropertiesInfoCache() throws Exception {
    HMSCacheType cacheType = HMSCacheType.TBLPROPERTIES_INFO;
    buildSingleTypeCache(
        cacheType,
        1
    );
    //Check correct return type and correct cache return
    TBLPropertiesInfoCacheSupport.TBLPropertiesInfo tblPropertiesInfo
        = new TBLPropertiesInfoCacheSupport.TBLPropertiesInfo(
        new TBLPropertiesInfoCacheSupport.TBLProperties(false, false, HiveMetastoreUtil.AVRO_SERDE)
    );
    hmsCache.put(cacheType, qualifiedTableName, tblPropertiesInfo);

    tblPropertiesInfo = hmsCache.getIfPresent(cacheType, qualifiedTableName);
    checkCacheType(
        TBLPropertiesInfoCacheSupport.TBLPropertiesInfo.class,
        hmsCache.getIfPresent(cacheType, qualifiedTableName)
    );
    Assert.assertFalse("External Mismatch", tblPropertiesInfo.isExternal());
    Assert.assertFalse("As Avro Mismatch", tblPropertiesInfo.isStoredAsAvro());

    hmsCache.invalidate(cacheType, qualifiedTableName);

    //Invalidated so no information.
    Assert.assertNull(hmsCache.getIfPresent(cacheType, qualifiedTableName));

    setMockForHMSCacheLoader(EMPTY_TYPE_INFO, EMPTY_TYPE_INFO, EMPTY_PARTITION_INFO, false, false);

    //Cache loader returns Optional.absent by default when column type info is not present
    Assert.assertNull(hmsCache.getOrLoad(cacheType, qualifiedTableName, queryExecutor));

    // Optional.absent is store after the above call so invalidating again.
    hmsCache.invalidate(cacheType, qualifiedTableName);

    LinkedHashMap<String, HiveTypeInfo> columnTypeInfo = new LinkedHashMap<>();
    columnTypeInfo.put("id", TestHiveMetastoreUtil.generatePrimitiveTypeInfo(HiveType.STRING, "id"));
    setMockForHMSCacheLoader(columnTypeInfo, EMPTY_TYPE_INFO, EMPTY_PARTITION_INFO, true, true);

    tblPropertiesInfo = hmsCache.getOrLoad(cacheType, qualifiedTableName, queryExecutor);

    //Check cache loading - returns
    Assert.assertTrue("External Mismatch", tblPropertiesInfo.isExternal());
    Assert.assertTrue("As Avro Mismatch", tblPropertiesInfo.isStoredAsAvro());

    //Finally given that we built the cache with max size 1, we are going to put another tblProperties
    //for table 1 and table2
    //which will evict table1 and force to be loaded.

    //No information in cache after this invalidation
    //All cache loaders will return empty
    setMockForHMSCacheLoader(EMPTY_TYPE_INFO, EMPTY_TYPE_INFO, EMPTY_PARTITION_INFO, true, true);
    hmsCache.invalidate(cacheType, qualifiedTableName);

    final String table1 = qualifiedTableName + "_1";
    final String table2 = qualifiedTableName + "_2";

    TBLPropertiesInfoCacheSupport.TBLPropertiesInfo tblPropertiesInfo1, tblPropertiesInfo2;

    tblPropertiesInfo1 = new TBLPropertiesInfoCacheSupport.TBLPropertiesInfo(
        new TBLPropertiesInfoCacheSupport.TBLProperties(false, false, HiveMetastoreUtil.AVRO_SERDE)
    );
    hmsCache.put(cacheType, table1, tblPropertiesInfo1);

    //not present entries cache should return null.
    tblPropertiesInfo1 = hmsCache.getIfPresent(cacheType, table1);
    Assert.assertNotNull(tblPropertiesInfo1);
    Assert.assertFalse("External Mismatch", tblPropertiesInfo1.isExternal());
    Assert.assertFalse("As Avro Mismatch", tblPropertiesInfo1.isStoredAsAvro());
    Assert.assertNull(hmsCache.getIfPresent(cacheType, table2));

    //Table 2 added.
    tblPropertiesInfo2 = new TBLPropertiesInfoCacheSupport.TBLPropertiesInfo(
        new TBLPropertiesInfoCacheSupport.TBLProperties(true, true, HiveMetastoreUtil.AVRO_SERDE)
    );
    hmsCache.put(cacheType, table2, tblPropertiesInfo2);

    tblPropertiesInfo1 = hmsCache.getIfPresent(cacheType, table1);
    tblPropertiesInfo2 = hmsCache.getIfPresent(cacheType, table2);

    //Table1 should have been evicted and should not be present in the cache.
    Assert.assertNotNull(tblPropertiesInfo2);
    Assert.assertTrue("External Mismatch", tblPropertiesInfo2.isExternal());
    Assert.assertTrue("As Avro Mismatch", tblPropertiesInfo2.isStoredAsAvro());
    Assert.assertNull(tblPropertiesInfo1);

    //Now set the cache loader to return true false
    setMockForHMSCacheLoader(columnTypeInfo, EMPTY_TYPE_INFO, EMPTY_PARTITION_INFO, true, false);

    tblPropertiesInfo1 = hmsCache.getOrLoad(cacheType, table1, queryExecutor);
    //This means cache is loaded and returns value from the mock.
    Assert.assertTrue("External Mismatch", tblPropertiesInfo1.isExternal());
    Assert.assertFalse("As Avro Mismatch", tblPropertiesInfo1.isStoredAsAvro());
  }

  private LinkedHashMap<String, HiveTypeInfo> getDefaultColumnTypeInfo() {
    LinkedHashMap<String, HiveTypeInfo> defaultColumnTypeInfo = new LinkedHashMap<>();
    defaultColumnTypeInfo.put("string", TestHiveMetastoreUtil.generatePrimitiveTypeInfo(HiveType.STRING, "string"));
    defaultColumnTypeInfo.put("int", TestHiveMetastoreUtil.generatePrimitiveTypeInfo(HiveType.INT, "int"));
    return defaultColumnTypeInfo;
  }

  private LinkedHashMap<String, HiveTypeInfo> getDefaultPartitionTypeInfo() {
    LinkedHashMap<String, HiveTypeInfo> defaultPartitionTypeInfo = new LinkedHashMap<>();
    defaultPartitionTypeInfo.put("dt", TestHiveMetastoreUtil.generatePrimitiveTypeInfo(HiveType.STRING, "dt"));
    defaultPartitionTypeInfo.put("year", TestHiveMetastoreUtil.generatePrimitiveTypeInfo(HiveType.INT, "year"));
    defaultPartitionTypeInfo.put("month", TestHiveMetastoreUtil.generatePrimitiveTypeInfo(HiveType.INT, "month"));
    defaultPartitionTypeInfo.put("day", TestHiveMetastoreUtil.generatePrimitiveTypeInfo(HiveType.INT, "day"));
    return defaultPartitionTypeInfo;
  }


  @Test
  public void testTypeInfoCache() throws Exception {
    HMSCacheType cacheType = HMSCacheType.TYPE_INFO;
    buildSingleTypeCache(
        cacheType,
        1
    );

    TypeInfoCacheSupport.TypeInfo typeInfo = new TypeInfoCacheSupport.TypeInfo(EMPTY_TYPE_INFO, EMPTY_TYPE_INFO);
    hmsCache.put(cacheType, qualifiedTableName, typeInfo);
    checkCacheType(TypeInfoCacheSupport.TypeInfo.class, hmsCache.getIfPresent(cacheType, qualifiedTableName));
    hmsCache.invalidate(cacheType, qualifiedTableName);

    LinkedHashMap<String, HiveTypeInfo> defaultColumnTypeInfo = getDefaultColumnTypeInfo();
    LinkedHashMap<String, HiveTypeInfo> defaultPartitionTypeInfo = getDefaultPartitionTypeInfo();


    typeInfo = new TypeInfoCacheSupport.TypeInfo(defaultColumnTypeInfo, defaultPartitionTypeInfo);
    hmsCache.put(cacheType, qualifiedTableName, typeInfo);

    //Check defaults
    typeInfo = hmsCache.getIfPresent(cacheType, qualifiedTableName);
    Assert.assertEquals("Column Size mismatch", defaultColumnTypeInfo.size(), typeInfo.getColumnTypeInfo().size());
    Assert.assertEquals(
        "Partition Size mismatch",
        defaultPartitionTypeInfo.size(),
        typeInfo.getPartitionTypeInfo().size()
    );

    //invalidate and set cache loader
    setMockForHMSCacheLoader(defaultColumnTypeInfo, defaultPartitionTypeInfo, EMPTY_PARTITION_INFO, false, true);
    hmsCache.invalidate(cacheType, qualifiedTableName);
    typeInfo = hmsCache.getOrLoad(cacheType, qualifiedTableName, queryExecutor);
    Assert.assertEquals("Column Size mismatch", defaultColumnTypeInfo.size(), typeInfo.getColumnTypeInfo().size());
    Assert.assertEquals(
        "Partition Size mismatch",
        defaultPartitionTypeInfo.size(),
        typeInfo.getPartitionTypeInfo().size()
    );


    //Now check diff.
    LinkedHashMap<String, HiveTypeInfo> extraColumn = new LinkedHashMap<>(defaultColumnTypeInfo);

    LinkedHashMap<String, HiveTypeInfo> diff = typeInfo.getDiff(extraColumn);
    Assert.assertEquals("Diff size mismatch", 0, diff.size());

    extraColumn = new LinkedHashMap<>(defaultColumnTypeInfo);
    extraColumn.put("decimal", TestHiveMetastoreUtil.generateDecimalTypeInfo("decimal",10, 5));

    diff = typeInfo.getDiff(extraColumn);
    Assert.assertEquals("Diff size mismatch", 1, diff.size());

    extraColumn = new LinkedHashMap<>(diff);
    diff = typeInfo.getDiff(extraColumn);
    Assert.assertEquals("Diff size mismatch", 1, diff.size());

    //Now update with diff
    typeInfo.updateState(diff);
    typeInfo = hmsCache.getIfPresent(cacheType, qualifiedTableName);
    Assert.assertEquals("Column Size mismatch", 3, typeInfo.getColumnTypeInfo().size());


    //Change a type of a column for c_string and see what happens
    extraColumn = new LinkedHashMap<>(defaultColumnTypeInfo);
    extraColumn.put("string", TestHiveMetastoreUtil.generatePrimitiveTypeInfo( HiveType.INT, "string"));
    try {
      diff = typeInfo.getDiff(extraColumn);
      Assert.fail("Diff with mismatched column should fail.");
    } catch (StageException e) {
      Assert.assertEquals("Error code mismatch", Errors.HIVE_21, e.getErrorCode());
    }

    //Change the scale of the decimal column
    extraColumn = new LinkedHashMap<>(defaultColumnTypeInfo);
    extraColumn.put("decimal", TestHiveMetastoreUtil.generateDecimalTypeInfo("decimal", 10, 3));
    try {
      diff = typeInfo.getDiff(extraColumn);
      Assert.fail("Diff with changed decimal column info should fail.");
    } catch (StageException e) {
      Assert.assertEquals("Error code mismatch", Errors.HIVE_21, e.getErrorCode());
    }
  }

  private Map<PartitionInfoCacheSupport.PartitionValues, String> getDefaultPartitionValuesInfo() {
    Map<PartitionInfoCacheSupport.PartitionValues, String> defaultPartitionValues = new HashMap<>();

    LinkedHashMap<String, String> partition1 = new LinkedHashMap<>();
    partition1.put("dt", "12-25-2015");
    partition1.put("year", "2015");
    partition1.put("month", "12");
    partition1.put("day", "25");
    defaultPartitionValues.put(new PartitionInfoCacheSupport.PartitionValues(partition1), "1");

    LinkedHashMap<String, String> partition2 = new LinkedHashMap<>();
    partition2.put("dt", "11-25-2015");
    partition2.put("year", "2015");
    partition2.put("month", "11");
    partition2.put("day", "25");
    defaultPartitionValues.put(new PartitionInfoCacheSupport.PartitionValues(partition2), "2");


    LinkedHashMap<String, String> partition3 = new LinkedHashMap<>();
    partition3.put("dt", "12-26-2015");
    partition3.put("year", "2015");
    partition3.put("month", "12");
    partition3.put("day", "26");
    defaultPartitionValues.put(new PartitionInfoCacheSupport.PartitionValues(partition3), "3");

    return defaultPartitionValues;
  }

  @Test
  public void testPartitionInfoCache() throws Exception {
    HMSCacheType cacheType = HMSCacheType.PARTITION_VALUE_INFO;
    buildSingleTypeCache(
        cacheType,
        5
    );

    PartitionInfoCacheSupport.PartitionInfo partitionInfo =
        new PartitionInfoCacheSupport.PartitionInfo(
            new HashMap<PartitionInfoCacheSupport.PartitionValues, String>(),
            PowerMockito.mock(HiveQueryExecutor.class),
            ""
        );
    hmsCache.put(cacheType, qualifiedTableName, partitionInfo);
    checkCacheType(PartitionInfoCacheSupport.PartitionInfo.class, hmsCache.getIfPresent(cacheType, qualifiedTableName));
    hmsCache.invalidate(cacheType, qualifiedTableName);

    Map<PartitionInfoCacheSupport.PartitionValues, String> defaultPartitionValues = getDefaultPartitionValuesInfo();

    //These are null but given that we are populating the cache we do not need this
    partitionInfo = new PartitionInfoCacheSupport.PartitionInfo(defaultPartitionValues, null, null);
    hmsCache.put(cacheType, qualifiedTableName, partitionInfo);

    //Check defaults
    partitionInfo = hmsCache.getIfPresent(cacheType, qualifiedTableName);
    Assert.assertEquals(
        "Partitions Value Size mismatch",
        defaultPartitionValues.size(),
        partitionInfo.getPartitions().size()
    );

    //invalidate and set cache loader
    setMockForHMSCacheLoader(
        getDefaultColumnTypeInfo(),
        getDefaultPartitionTypeInfo(),
        defaultPartitionValues,
        false,
        true
    );
    hmsCache.invalidate(cacheType, qualifiedTableName);
    partitionInfo = hmsCache.getOrLoad(cacheType, qualifiedTableName, queryExecutor);
    Assert.assertEquals(
        "Partition Values Size mismatch",
        defaultPartitionValues.size(),
        partitionInfo.getPartitions().size()
    );

    //Check diff
    Map<PartitionInfoCacheSupport.PartitionValues, String> newPartitionValues = new HashMap<>(defaultPartitionValues);
    Map<PartitionInfoCacheSupport.PartitionValues, String> diff = partitionInfo.getDiff(newPartitionValues);
    Assert.assertEquals("Partition Values Size mismatch", 0, diff.size());

    LinkedHashMap<String, String> partition4 = new LinkedHashMap<>();
    partition4.put("dt", "01-01-2016");
    partition4.put("year", "2016");
    partition4.put("month", "01");
    partition4.put("day", "01");
    newPartitionValues.put(new PartitionInfoCacheSupport.PartitionValues(partition4), "4");

    diff = partitionInfo.getDiff(newPartitionValues);
    Assert.assertEquals("Partition Values Size mismatch", 1, diff.size());

    partitionInfo.updateState(diff);

    partitionInfo = hmsCache.getIfPresent(cacheType, qualifiedTableName);
    Assert.assertEquals(
        "Partitions Value Size mismatch",
        4,
        partitionInfo.getPartitions().size()
    );
  }

  private String getDefaultAvroSchema() {
    return "{\"type\":\"record\"," +
        "\"name\":\"test\"," +
        "\"fields\":[" +
        "{\"name\":\"c_string\",\"type\":[\"null\",\"string\"],\"default\":null}," +
        "{\"name\":\"c_int\",\"type\":[\"null\",\"int\"],\"default\":null}," +
        "}";
  }

  @Test
  public void testAvroSchemaInfoCache() throws Exception {
    HMSCacheType cacheType = HMSCacheType.AVRO_SCHEMA_INFO;
    buildSingleTypeCache(
        cacheType,
        1
    );
    AvroSchemaInfoCacheSupport.AvroSchemaInfo avroSchemaInfo =
        new AvroSchemaInfoCacheSupport.AvroSchemaInfo("");
    hmsCache.put(cacheType, qualifiedTableName, avroSchemaInfo);
    checkCacheType(AvroSchemaInfoCacheSupport.AvroSchemaInfo.class, hmsCache.getIfPresent(cacheType, qualifiedTableName));
    hmsCache.invalidate(cacheType, qualifiedTableName);

    setMockForHMSCacheLoader(
        getDefaultColumnTypeInfo(),
        getDefaultPartitionTypeInfo(),
        getDefaultPartitionValuesInfo(),
        false,
        true
    );
    //Trying to load will fail.
    try {
      hmsCache.getOrLoad(cacheType, qualifiedTableName, queryExecutor);
      Assert.fail("Trying to use load on avro schema info should fail");
    } catch (StageException e) {
      Assert.assertEquals("Error code mismatch", Errors.HIVE_01, e.getErrorCode());
    }

    avroSchemaInfo = new AvroSchemaInfoCacheSupport.AvroSchemaInfo(getDefaultAvroSchema());
    hmsCache.put(cacheType, qualifiedTableName, avroSchemaInfo);
    avroSchemaInfo = hmsCache.getIfPresent(cacheType, qualifiedTableName);
    Assert.assertEquals("Schema mismatch", avroSchemaInfo.getSchema(), getDefaultAvroSchema());
    try {
      avroSchemaInfo.getDiff("");
      Assert.fail("Trying to call diff avro schema info should fail");
    } catch (StageException e) {
      Assert.assertEquals("Error code mismatch", Errors.HIVE_01, e.getErrorCode());
    }
  }
}
