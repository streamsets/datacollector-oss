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
package com.streamsets.pipeline.stage.destination.bigtable;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.testing.SingleForkNoReuseTest;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

@Category(SingleForkNoReuseTest.class)
@Ignore
public class BigtableTargetIT {
  private static final Logger LOG = LoggerFactory.getLogger(BigtableTargetIT.class);

  private static final String tableName = "myTable2";
  private static final String instanceID = "bobinstance";
  private static final String projectID = "hidden-terrain-146817";
  private static final String defaultColumnFamily = "dcf";

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  }

  @AfterClass
  public static void tearDown() throws Exception {
    dropTable();
    BigtableUtility btu = new BigtableUtility();
    btu.stopEmulator();

  }

  private void basicConfiguration(BigtableConfigBean conf) {

    conf.bigtableInstanceID = instanceID;
    conf.bigtableProjectID = projectID;
    conf.tableName = tableName;

    // set options:
    conf.createTableAndColumnFamilies = true;
    conf.explicitFieldMapping = true;

    //default column family.
    conf.columnFamily = "dcf";

  }

  @Test(timeout = 60000)
  public void testValidateNoConfigIssues() throws Exception {

    BigtableUtility btu = new BigtableUtility();
    btu.setupEnvironment();
    btu.startEmulator();

    BigtableConfigBean conf = new BigtableConfigBean();
    basicConfiguration(conf);
    // add a rowkey
    /*
    conf.createCompositeRowKey = false;
    BigtableRowKeyMapping rMap = new BigtableRowKeyMapping();
    rMap.columnWidth = 7;
    rMap.rowKeyComponent = "/rowkey";
    conf.rowKeyColumnMapping = ImmutableList.of(rMap);
*/
    // add a field to insert.
    BigtableFieldMapping fMap = new BigtableFieldMapping();
    fMap.storageType = BigtableStorageType.TEXT;
    fMap.column = "aa:bb";
    fMap.source = "/0";
    conf.fieldColumnMapping = ImmutableList.of(fMap);

    // add a rowkey
    conf.createCompositeRowKey = false;
    conf.singleColumnRowKey = "/rowkey";

    BigtableTarget target = new BigtableTarget(conf);

    TargetRunner targetRunner = new TargetRunner.Builder(BigtableDTarget.class,
        target
    ).setOnRecordError(OnRecordError.DISCARD).build();
    assertTrue(targetRunner.runValidateConfigs().isEmpty());

    btu.stopEmulator();
  }

  @Test(timeout = 60000)
  public void testSingleColumnRowKeyFailure() throws Exception {

    BigtableUtility btu = new BigtableUtility();
    btu.setupEnvironment();
    btu.startEmulator();

    BigtableConfigBean conf = new BigtableConfigBean();
    basicConfiguration(conf);

    // add a rowkey
    conf.createCompositeRowKey = false;

    // add a field to insert.
    BigtableFieldMapping fMap = new BigtableFieldMapping();
    fMap.storageType = BigtableStorageType.TEXT;
    fMap.column = "aa:bb";
    fMap.source = "/0";
    conf.fieldColumnMapping = ImmutableList.of(fMap);

    //more config if necessary.

    BigtableTarget target = new BigtableTarget(conf);

    TargetRunner targetRunner = new TargetRunner.Builder(BigtableDTarget.class,
        target
    ).setOnRecordError(OnRecordError.DISCARD).build();

    List<Stage.ConfigIssue> configIssues = targetRunner.runValidateConfigs();
    assertEquals(1, configIssues.size());
    assertTrue(configIssues.get(0).toString().contains(Errors.BIGTABLE_11.getCode()));

    btu.stopEmulator();
  }

  @Test(timeout = 60000)
  public void testFailToConnect() throws Exception {

    BigtableConfigBean conf = new BigtableConfigBean();
    basicConfiguration(conf);

    // add a rowkey
    conf.createCompositeRowKey = false;
    conf.singleColumnRowKey = "/rowkey";

    // add a field to insert.
    BigtableFieldMapping fMap = new BigtableFieldMapping();
    fMap.storageType = BigtableStorageType.TEXT;
    fMap.column = "aa:bb";
    fMap.source = "/0";
    conf.fieldColumnMapping = ImmutableList.of(fMap);

    BigtableTarget target = new BigtableTarget(conf);

    TargetRunner targetRunner = new TargetRunner.Builder(BigtableDTarget.class,
        target
    ).setOnRecordError(OnRecordError.DISCARD).build();

    List<Stage.ConfigIssue> configIssues = targetRunner.runValidateConfigs();
    assertEquals(1, configIssues.size());
    assertTrue(configIssues.get(0).toString().contains(Errors.BIGTABLE_17.getCode()));

  }

  @Test(timeout = 60000)
  public void testConnect() throws Exception {

    BigtableUtility btu = new BigtableUtility();
    btu.setupEnvironment();
    btu.startEmulator();

    BigtableConfigBean conf = new BigtableConfigBean();
    basicConfiguration(conf);
    //more config if necessary.

    // add a rowkey
    conf.createCompositeRowKey = false;
    conf.singleColumnRowKey = "/rowkey";

    BigtableFieldMapping fMap = new BigtableFieldMapping();
    fMap.storageType = BigtableStorageType.TEXT;
    fMap.column = "bb";
    fMap.source = "/0";
    conf.fieldColumnMapping = ImmutableList.of(fMap);

    BigtableTarget target = new BigtableTarget(conf);

    TargetRunner targetRunner = new TargetRunner.Builder(BigtableDTarget.class,
        target
    ).setOnRecordError(OnRecordError.DISCARD).build();

    List<Stage.ConfigIssue> configIssues = targetRunner.runValidateConfigs();
    assertEquals(0, configIssues.size());

    btu.stopEmulator();
  }

  @Test(timeout = 60000)
  public void TestInsertARecordWithStringRowKey() throws Exception {

    BigtableUtility btu = new BigtableUtility();
    btu.setupEnvironment();
    btu.startEmulator();

    BigtableConfigBean conf = new BigtableConfigBean();
    basicConfiguration(conf);

    // add a rowkey
    conf.createCompositeRowKey = false;
    conf.singleColumnRowKey = "/rowkey";

    // metadata for column mapping.
    BigtableFieldMapping fMap = new BigtableFieldMapping();
    fMap.storageType = BigtableStorageType.TEXT;
    fMap.column = "a";
    fMap.source = "/a";

    BigtableFieldMapping fMap1 = new BigtableFieldMapping();
    fMap1.storageType = BigtableStorageType.TEXT;
    fMap1.column = "b";
    fMap1.source = "/b";

    BigtableFieldMapping fMap2 = new BigtableFieldMapping();
    fMap2.storageType = BigtableStorageType.TEXT;
    fMap2.column = "c";
    fMap2.source = "/c";

    BigtableFieldMapping fMap3 = new BigtableFieldMapping();
    fMap3.storageType = BigtableStorageType.TEXT;
    fMap3.column = "d";
    fMap3.source = "/d";
    conf.fieldColumnMapping = ImmutableList.of(fMap, fMap1, fMap2, fMap3);

    BigtableTarget target = new BigtableTarget(conf);

    TargetRunner targetRunner = new TargetRunner.Builder(BigtableDTarget.class,
        target
    ).setOnRecordError(OnRecordError.DISCARD).build();

    assertTrue(targetRunner.runValidateConfigs().isEmpty());

    String rowkey = "row_aaa";
    Map<String, Field> map = new LinkedHashMap<>();
    map.put("rowkey", Field.create(rowkey));
    map.put("a", Field.create(20));
    map.put("b", Field.create(30));
    map.put("c", Field.create(40));
    map.put("d", Field.create(50));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));

    List<Record> singleRecord = ImmutableList.of(record);
    targetRunner.runInit();
    targetRunner.runWrite(singleRecord);

    // retrieve it through standard APIs.
    Table hTable = getTable();
    Get g = new Get(Bytes.toBytes(rowkey));
    g.addFamily(Bytes.toBytes(defaultColumnFamily));
    Result r = hTable.get(g);

    assertEquals("20", Bytes.toString(r.getValue(Bytes.toBytes(defaultColumnFamily), Bytes.toBytes("a"))));
    assertEquals("30", Bytes.toString(r.getValue(Bytes.toBytes(defaultColumnFamily), Bytes.toBytes("b"))));
    assertEquals("40", Bytes.toString(r.getValue(Bytes.toBytes(defaultColumnFamily), Bytes.toBytes("c"))));
    assertEquals("50", Bytes.toString(r.getValue(Bytes.toBytes(defaultColumnFamily), Bytes.toBytes("d"))));

    targetRunner.runDestroy();
  }
  @Test(timeout = 60000)
  public void TestInsertARecordWithLongRowKey() throws Exception {

    BigtableUtility btu = new BigtableUtility();
    btu.setupEnvironment();
    btu.startEmulator();

    BigtableConfigBean conf = new BigtableConfigBean();
    basicConfiguration(conf);

    // add a rowkey
    conf.createCompositeRowKey = false;
    conf.singleColumnRowKey = "/rowkey";

    // metadata for column mapping.
    BigtableFieldMapping fMap = new BigtableFieldMapping();
    fMap.storageType = BigtableStorageType.TEXT;
    fMap.column = "a";
    fMap.source = "/a";

    BigtableFieldMapping fMap1 = new BigtableFieldMapping();
    fMap1.storageType = BigtableStorageType.TEXT;
    fMap1.column = "b";
    fMap1.source = "/b";

    BigtableFieldMapping fMap2 = new BigtableFieldMapping();
    fMap2.storageType = BigtableStorageType.TEXT;
    fMap2.column = "c";
    fMap2.source = "/c";

    BigtableFieldMapping fMap3 = new BigtableFieldMapping();
    fMap3.storageType = BigtableStorageType.TEXT;
    fMap3.column = "d";
    fMap3.source = "/d";
    conf.fieldColumnMapping = ImmutableList.of(fMap, fMap1, fMap2, fMap3);

    BigtableTarget target = new BigtableTarget(conf);

    TargetRunner targetRunner = new TargetRunner.Builder(BigtableDTarget.class,
        target
    ).setOnRecordError(OnRecordError.DISCARD).build();

    assertTrue(targetRunner.runValidateConfigs().isEmpty());

    long rowkey = 1128L;
    Map<String, Field> map = new LinkedHashMap<>();
    map.put("rowkey", Field.create(rowkey));
    map.put("a", Field.create(20));
    map.put("b", Field.create(30));
    map.put("c", Field.create(40));
    map.put("d", Field.create(50));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));

    List<Record> singleRecord = ImmutableList.of(record);
    targetRunner.runInit();
    targetRunner.runWrite(singleRecord);

    // retrieve it through standard APIs.
    Table hTable = getTable();
    Get g = new Get(Bytes.toBytes(rowkey));
    g.addFamily(Bytes.toBytes(defaultColumnFamily));
    Result r = hTable.get(g);

    assertEquals("20", Bytes.toString(r.getValue(Bytes.toBytes(defaultColumnFamily), Bytes.toBytes("a"))));
    assertEquals("30", Bytes.toString(r.getValue(Bytes.toBytes(defaultColumnFamily), Bytes.toBytes("b"))));
    assertEquals("40", Bytes.toString(r.getValue(Bytes.toBytes(defaultColumnFamily), Bytes.toBytes("c"))));
    assertEquals("50", Bytes.toString(r.getValue(Bytes.toBytes(defaultColumnFamily), Bytes.toBytes("d"))));

    targetRunner.runDestroy();
  }

  @Test(timeout = 60000)
  public void TestTableExistsDefaultColumnFamilyDoesNotExist() throws Exception {

    BigtableUtility btu = new BigtableUtility();
    btu.setupEnvironment();
    btu.startEmulator();

    BigtableConfigBean conf = new BigtableConfigBean();
    basicConfiguration(conf);
    conf.createTableAndColumnFamilies = false;

    // fake out that the table already exists.
    createTable(tableName);

    // add a rowkey
    conf.createCompositeRowKey = false;
    conf.singleColumnRowKey = "/rowkey";

    // metadata for column mapping.
    BigtableFieldMapping fMap = new BigtableFieldMapping();
    fMap.storageType = BigtableStorageType.TEXT;
    fMap.column = "a";
    fMap.source = "/a";

    conf.fieldColumnMapping = ImmutableList.of(fMap);

    BigtableTarget target = new BigtableTarget(conf);

    TargetRunner targetRunner = new TargetRunner.Builder(BigtableDTarget.class,
        target
    ).setOnRecordError(OnRecordError.DISCARD).build();

    assertEquals(1, targetRunner.runValidateConfigs().size());
    assertTrue(targetRunner.runValidateConfigs().get(0).toString().contains(Errors.BIGTABLE_04.getCode()));

    btu.stopEmulator();
  }

  @Test(timeout = 60000)
  public void TestInsertWithExplicitColumnFamily() throws Exception {

    BigtableUtility btu = new BigtableUtility();
    btu.setupEnvironment();
    btu.startEmulator();

    BigtableConfigBean conf = new BigtableConfigBean();
    basicConfiguration(conf);
    conf.explicitFieldMapping = true;

    // add a rowkey
    conf.createCompositeRowKey = false;
    conf.singleColumnRowKey = "/rowkey";

    // metadata for column mapping.
    BigtableFieldMapping fMap = new BigtableFieldMapping();
    fMap.storageType = BigtableStorageType.TEXT;
    fMap.column = "aaa:aa";
    fMap.source = "/a";

    BigtableFieldMapping fMap1 = new BigtableFieldMapping();
    fMap1.storageType = BigtableStorageType.TEXT;
    fMap1.column = "bbb:bb";
    fMap1.source = "/b";

    BigtableFieldMapping fMap2 = new BigtableFieldMapping();
    fMap2.storageType = BigtableStorageType.TEXT;
    fMap2.column = "ccc:cc";
    fMap2.source = "/c";

    BigtableFieldMapping fMap3 = new BigtableFieldMapping();
    fMap3.storageType = BigtableStorageType.TEXT;
    fMap3.column = "d";
    fMap3.source = "/d";
    conf.fieldColumnMapping = ImmutableList.of(fMap, fMap1, fMap2, fMap3);

    BigtableTarget target = new BigtableTarget(conf);

    TargetRunner targetRunner = new TargetRunner.Builder(BigtableDTarget.class,
        target
    ).setOnRecordError(OnRecordError.DISCARD).build();

    assertTrue(targetRunner.runValidateConfigs().isEmpty());

    String rowkey = "row_aaa";
    Map<String, Field> map = new LinkedHashMap<>();
    map.put("rowkey", Field.create(rowkey));
    map.put("a", Field.create(20));
    map.put("b", Field.create(30));
    map.put("c", Field.create(40));
    map.put("d", Field.create(50));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));

    //    assertTrue(targetRunner.runValidateConfigs().isEmpty());
    List<Record> singleRecord = ImmutableList.of(record);
    targetRunner.runInit();
    targetRunner.runWrite(singleRecord);

    // retrieve it.
    Table hTable = getTable();
    Get g = new Get(Bytes.toBytes(rowkey));
    g.addFamily(Bytes.toBytes("aaa"));
    g.addFamily(Bytes.toBytes("bbb"));
    g.addFamily(Bytes.toBytes("ccc"));
    g.addFamily(Bytes.toBytes(defaultColumnFamily));
    Result r = hTable.get(g);

    assertEquals("20", Bytes.toString(r.getValue(Bytes.toBytes("aaa"), Bytes.toBytes("aa"))));
    assertEquals("30", Bytes.toString(r.getValue(Bytes.toBytes("bbb"), Bytes.toBytes("bb"))));
    assertEquals("40", Bytes.toString(r.getValue(Bytes.toBytes("ccc"), Bytes.toBytes("cc"))));
    assertEquals("50", Bytes.toString(r.getValue(Bytes.toBytes(defaultColumnFamily), Bytes.toBytes("d"))));

    targetRunner.runDestroy();
    dropTable();
    btu.stopEmulator();
  }

  @Test(timeout = 60000)
  public void TestInsertWithDefaultColumnFamily() throws Exception {

    BigtableUtility btu = new BigtableUtility();
    btu.setupEnvironment();
    btu.startEmulator();

    BigtableConfigBean conf = new BigtableConfigBean();
    basicConfiguration(conf);
    conf.explicitFieldMapping = false;

    // add a rowkey
    conf.createCompositeRowKey = false;
    conf.singleColumnRowKey = "/rowkey";

    // metadata for column mapping.
    BigtableFieldMapping fMap = new BigtableFieldMapping();
    fMap.storageType = BigtableStorageType.TEXT;
    fMap.column = "aaa:aa";
    fMap.source = "/a";

    BigtableFieldMapping fMap1 = new BigtableFieldMapping();
    fMap1.storageType = BigtableStorageType.TEXT;
    fMap1.column = "bbb:bb";
    fMap1.source = "/b";

    BigtableFieldMapping fMap2 = new BigtableFieldMapping();
    fMap2.storageType = BigtableStorageType.TEXT;
    fMap2.column = "ccc:cc";
    fMap2.source = "/c";

    BigtableFieldMapping fMap3 = new BigtableFieldMapping();
    fMap3.storageType = BigtableStorageType.TEXT;
    fMap3.column = "dd";
    fMap3.source = "/d";
    conf.fieldColumnMapping = ImmutableList.of(fMap, fMap1, fMap2, fMap3);

    BigtableTarget target = new BigtableTarget(conf);

    TargetRunner targetRunner = new TargetRunner.Builder(BigtableDTarget.class,
        target
    ).setOnRecordError(OnRecordError.DISCARD).build();

    assertTrue(targetRunner.runValidateConfigs().isEmpty());

    String rowkey = "row_aaa";
    Map<String, Field> map = new LinkedHashMap<>();
    map.put("rowkey", Field.create(rowkey));
    map.put("a", Field.create(20));
    map.put("b", Field.create(30));
    map.put("c", Field.create(40));
    map.put("d", Field.create(50));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));

    assertTrue(targetRunner.runValidateConfigs().isEmpty());
    List<Record> singleRecord = ImmutableList.of(record);
    targetRunner.runInit();
    targetRunner.runWrite(singleRecord);

    // retrieve the record.
    Table hTable = getTable();
    Get g = new Get(Bytes.toBytes(rowkey));
    g.addFamily(Bytes.toBytes("aaa"));
    g.addFamily(Bytes.toBytes("bbb"));
    g.addFamily(Bytes.toBytes("ccc"));
    g.addFamily(Bytes.toBytes(defaultColumnFamily));
    Result r = hTable.get(g);

    assertNotEquals("20", Bytes.toString(r.getValue(Bytes.toBytes("aaa"), Bytes.toBytes("aa"))));
    assertNotEquals("30", Bytes.toString(r.getValue(Bytes.toBytes("bbb"), Bytes.toBytes("bb"))));
    assertNotEquals("40", Bytes.toString(r.getValue(Bytes.toBytes("ccc"), Bytes.toBytes("cc"))));

    assertEquals("20", Bytes.toString(r.getValue(Bytes.toBytes(defaultColumnFamily), Bytes.toBytes("aa"))));
    assertEquals("30", Bytes.toString(r.getValue(Bytes.toBytes(defaultColumnFamily), Bytes.toBytes("bb"))));
    assertEquals("40", Bytes.toString(r.getValue(Bytes.toBytes(defaultColumnFamily), Bytes.toBytes("cc"))));
    assertEquals("50", Bytes.toString(r.getValue(Bytes.toBytes(defaultColumnFamily), Bytes.toBytes("dd"))));

    targetRunner.runDestroy();
    dropTable();
    btu.stopEmulator();
  }

  @Test(timeout = 60000)
  public void TestInsertWithPipelineStartTime() throws Exception {

    BigtableUtility btu = new BigtableUtility();
    btu.setupEnvironment();
    btu.startEmulator();

    BigtableConfigBean conf = new BigtableConfigBean();
    basicConfiguration(conf);
    conf.explicitFieldMapping = false;
    conf.timeBasis = TimeBasis.PIPELINE_START;

    // add a rowkey
    conf.createCompositeRowKey = false;
    conf.singleColumnRowKey = "/rowkey";

    // metadata for column mapping.
    BigtableFieldMapping fMap = new BigtableFieldMapping();
    fMap.storageType = BigtableStorageType.TEXT;
    fMap.column = "aaa:aa";
    fMap.source = "/a";

    BigtableFieldMapping fMap1 = new BigtableFieldMapping();
    fMap1.storageType = BigtableStorageType.TEXT;
    fMap1.column = "bbb:bb";
    fMap1.source = "/b";

    BigtableFieldMapping fMap2 = new BigtableFieldMapping();
    fMap2.storageType = BigtableStorageType.TEXT;
    fMap2.column = "ccc:cc";
    fMap2.source = "/c";

    BigtableFieldMapping fMap3 = new BigtableFieldMapping();
    fMap3.storageType = BigtableStorageType.TEXT;
    fMap3.column = "dd";
    fMap3.source = "/d";
    conf.fieldColumnMapping = ImmutableList.of(fMap, fMap1, fMap2, fMap3);

    BigtableTarget target = new BigtableTarget(conf);

    TargetRunner targetRunner = new TargetRunner.Builder(BigtableDTarget.class,
        target
    ).setOnRecordError(OnRecordError.DISCARD).build();

    assertTrue(targetRunner.runValidateConfigs().isEmpty());

    String rowkey = "row_aaa";
    Map<String, Field> map = new LinkedHashMap<>();
    map.put("rowkey", Field.create(rowkey));
    map.put("a", Field.create(20));
    map.put("b", Field.create(30));
    map.put("c", Field.create(40));
    map.put("d", Field.create(50));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    List<Record> singleRecord = ImmutableList.of(record);
    targetRunner.runInit();
    targetRunner.runWrite(singleRecord);

    //make second "batch"
    map.put("rowkey", Field.create("row_xxx"));
    record.set(Field.create(map));
    singleRecord = ImmutableList.of(record);
    targetRunner.runWrite(singleRecord);

    // retrieve the record.
    Table hTable = getTable();
    Get g = new Get(Bytes.toBytes(rowkey));
    g.addFamily(Bytes.toBytes("aaa"));
    g.addFamily(Bytes.toBytes("bbb"));
    g.addFamily(Bytes.toBytes("ccc"));
    g.addFamily(Bytes.toBytes(defaultColumnFamily));
    Result r = hTable.get(g);

    //verify timestamps for cell consistency...
    long ts1;
    Cell[] cells = r.rawCells();
    assertEquals(4, cells.length);
    for(int i  = 0 ; i < cells.length -1 ;i++){
      // within the record each cell's timestamp should match
      assertEquals(cells[i].getTimestamp(), cells[i+1].getTimestamp());
    }
    ts1 = cells[0].getTimestamp();

    //get the second record.
    g = new Get(Bytes.toBytes("row_xxx"));
    g.addFamily(Bytes.toBytes("aaa"));
    g.addFamily(Bytes.toBytes("bbb"));
    g.addFamily(Bytes.toBytes("ccc"));
    g.addFamily(Bytes.toBytes(defaultColumnFamily));
    r = hTable.get(g);

    //verify timestamps for cell consistency...
    long ts2;
    cells = r.rawCells();
    assertEquals(4, cells.length);
    for(int i  = 0 ; i < cells.length -1 ;i++){
      // within the record each cell's timestamp should match
      assertEquals(cells[i].getTimestamp(), cells[i+1].getTimestamp());
    }
    ts2 = cells[0].getTimestamp();

    //compare first record's timestamp vs. seconds record's timestamp.
    assertEquals(ts1, ts2);

    targetRunner.runDestroy();
    dropTable();
    btu.stopEmulator();
  }

  @Test(timeout = 60000)
  public void TestInsertWithBatchStartTime() throws Exception {

    BigtableUtility btu = new BigtableUtility();
    btu.setupEnvironment();
    btu.startEmulator();

    BigtableConfigBean conf = new BigtableConfigBean();
    basicConfiguration(conf);
    conf.explicitFieldMapping = false;
    conf.timeBasis = TimeBasis.BATCH_START;

    // add a rowkey
    conf.createCompositeRowKey = false;
    conf.singleColumnRowKey = "/rowkey";

    // metadata for column mapping.
    BigtableFieldMapping fMap = new BigtableFieldMapping();
    fMap.storageType = BigtableStorageType.TEXT;
    fMap.column = "aaa:aa";
    fMap.source = "/a";

    BigtableFieldMapping fMap1 = new BigtableFieldMapping();
    fMap1.storageType = BigtableStorageType.TEXT;
    fMap1.column = "bbb:bb";
    fMap1.source = "/b";

    BigtableFieldMapping fMap2 = new BigtableFieldMapping();
    fMap2.storageType = BigtableStorageType.TEXT;
    fMap2.column = "ccc:cc";
    fMap2.source = "/c";

    BigtableFieldMapping fMap3 = new BigtableFieldMapping();
    fMap3.storageType = BigtableStorageType.TEXT;
    fMap3.column = "dd";
    fMap3.source = "/d";
    conf.fieldColumnMapping = ImmutableList.of(fMap, fMap1, fMap2, fMap3);

    BigtableTarget target = new BigtableTarget(conf);

    TargetRunner targetRunner = new TargetRunner.Builder(BigtableDTarget.class,
        target
    ).setOnRecordError(OnRecordError.DISCARD).build();

    assertTrue(targetRunner.runValidateConfigs().isEmpty());

    String rowkey = "row_aaa";
    Map<String, Field> map = new LinkedHashMap<>();
    map.put("rowkey", Field.create(rowkey));
    map.put("a", Field.create(20));
    map.put("b", Field.create(30));
    map.put("c", Field.create(40));
    map.put("d", Field.create(50));
    Record record = RecordCreator.create();
    record.set(Field.create(map));

    rowkey = "row_bbb";
    Map<String, Field> map1 = new LinkedHashMap<>();
    map1.put("rowkey", Field.create(rowkey));
    map1.put("a", Field.create(120));
    map1.put("b", Field.create(130));
    map1.put("c", Field.create(140));
    map1.put("d", Field.create(150));
    Record record1 = RecordCreator.create();
    record1.set(Field.create(map1));

    targetRunner.runInit();
    List<Record> someRecords = ImmutableList.of(record, record1);
    targetRunner.runWrite(someRecords);
    Thread.sleep(10);

    //make second "batch"
    rowkey = "row_ccc";
    Map<String, Field> map2 = new LinkedHashMap<>();
    map2.put("rowkey", Field.create(rowkey));
    map2.put("a", Field.create(220));
    map2.put("b", Field.create(230));
    map2.put("c", Field.create(240));
    map2.put("d", Field.create(250));
    Record record2 = RecordCreator.create();
    record2.set(Field.create(map2));

    rowkey = "row_ddd";
    Map<String, Field> map3 = new LinkedHashMap<>();
    map3.put("rowkey", Field.create(rowkey));
    map3.put("a", Field.create(320));
    map3.put("b", Field.create(330));
    map3.put("c", Field.create(340));
    map3.put("d", Field.create(350));
    Record record3 = RecordCreator.create();
    record3.set(Field.create(map3));

    List<Record> moreRecords = ImmutableList.of(record2, record3);
    targetRunner.runWrite(moreRecords);

    // retrieve the record.
    rowkey = "row_aaa";
    Table hTable = getTable();
    Get g = new Get(Bytes.toBytes(rowkey));
    g.addFamily(Bytes.toBytes("aaa"));
    g.addFamily(Bytes.toBytes("bbb"));
    g.addFamily(Bytes.toBytes("ccc"));
    g.addFamily(Bytes.toBytes(defaultColumnFamily));
    Result r = hTable.get(g);

    //verify timestamps for cell consistency...
    long ts1;
    Cell[] cells = r.rawCells();
    assertEquals(4, cells.length);
    for(int i  = 0 ; i < cells.length -1 ;i++){
      // within the record each cell's timestamp should match
      assertEquals(cells[i].getTimestamp(), cells[i+1].getTimestamp());
    }
    ts1 = cells[0].getTimestamp();

    //get the second record.
    g = new Get(Bytes.toBytes("row_bbb"));
    g.addFamily(Bytes.toBytes("aaa"));
    g.addFamily(Bytes.toBytes("bbb"));
    g.addFamily(Bytes.toBytes("ccc"));
    g.addFamily(Bytes.toBytes(defaultColumnFamily));
    r = hTable.get(g);

    //verify timestamps for cell consistency...
    long ts2;
    cells = r.rawCells();
    assertEquals(4, cells.length);
    for(int i  = 0 ; i < cells.length -1 ;i++){
      // within the record each cell's timestamp should match
      assertEquals(cells[i].getTimestamp(), cells[i+1].getTimestamp());
    }
    ts2 = cells[0].getTimestamp();

    //compare first record's timestamp vs. seconds record's timestamp.
    long diff = ts2 - ts1;
    // both records from this batch have the sampe timestamp.
    assertEquals(ts1, ts2);

    // deal with second batch...
    // retrieve the record.
    rowkey = "row_ccc";
    hTable = getTable();
    g = new Get(Bytes.toBytes(rowkey));
    g.addFamily(Bytes.toBytes("aaa"));
    g.addFamily(Bytes.toBytes("bbb"));
    g.addFamily(Bytes.toBytes("ccc"));
    g.addFamily(Bytes.toBytes(defaultColumnFamily));
    r = hTable.get(g);

    //verify timestamps for cell consistency...
    cells = r.rawCells();
    assertEquals(4, cells.length);
    for(int i  = 0 ; i < cells.length -1 ;i++){
      // within the record each cell's timestamp should match
      assertEquals(cells[i].getTimestamp(), cells[i+1].getTimestamp());
    }
    long ts3 = cells[0].getTimestamp();

    //get the second batch, second record.
    g = new Get(Bytes.toBytes("row_ddd"));
    g.addFamily(Bytes.toBytes("aaa"));
    g.addFamily(Bytes.toBytes("bbb"));
    g.addFamily(Bytes.toBytes("ccc"));
    g.addFamily(Bytes.toBytes(defaultColumnFamily));
    r = hTable.get(g);

    //verify timestamps for cell consistency...
    cells = r.rawCells();
    assertEquals(4, cells.length);
    for(int i  = 0 ; i < cells.length -1 ;i++){
      // within the record each cell's timestamp should match
      assertEquals(cells[i].getTimestamp(), cells[i+1].getTimestamp());
    }
    long ts4 = cells[0].getTimestamp();

    //compare first record's timestamp vs. seconds record's timestamp.
    diff = ts3 - ts4;
    // both records from this batch have the sampe timestamp.
    assertEquals(ts3, ts4);


    targetRunner.runDestroy();
    dropTable();
    btu.stopEmulator();
  }

  //functions to directly access the emulator.
  private Table getTable() {
    Connection conn = BigtableConfiguration.connect(projectID, instanceID);
    Table tab;
    try {
      tab = conn.getTable(TableName.valueOf(tableName));

    } catch (Exception ex) {
      tab = null;
    }
    return tab;
  }

  private void createTable(String tab) throws Exception {
    Connection conn = BigtableConfiguration.connect(projectID, instanceID);
    Admin admin = conn.getAdmin();
    admin.createTable(new HTableDescriptor(TableName.valueOf(tab)));
  }

  private void createColumnFamily(String tab, String cf) throws Exception {
    Connection conn = BigtableConfiguration.connect(projectID, instanceID);
    Admin admin = conn.getAdmin();
    admin.addColumn(TableName.valueOf(tab), new HColumnDescriptor(cf));
  }

  private static void dropTable() {
    Connection conn = BigtableConfiguration.connect(projectID, instanceID);
    try {
      Admin admin = conn.getAdmin();
      admin.disableTable(TableName.valueOf(tableName));
      admin.deleteTable(TableName.valueOf(tableName));
    } catch (Exception ex) {
      LOG.info("dropTable(): exception {} ", ex.toString());
    }
  }
}
