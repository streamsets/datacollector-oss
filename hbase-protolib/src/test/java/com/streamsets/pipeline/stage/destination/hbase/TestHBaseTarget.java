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

package com.streamsets.pipeline.stage.destination.hbase;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.Sets;
import com.streamsets.pipeline.api.impl.Utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.Stage.ConfigIssue;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.Field.Type;
import com.streamsets.pipeline.lib.util.JsonUtil;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestHBaseTarget {
  private static final Logger LOG = LoggerFactory.getLogger(TestHBaseTarget.class);
  private static HBaseTestingUtility utility;
  private static MiniZooKeeperCluster miniZK;
  private static final String tableName = "TestHBaseSink";
  private static final String familyName = "cf";
  private static final Configuration conf = HBaseConfiguration.create();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    try {
      conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/hbase");
      conf.set("hadoop.proxyuser." + System.getProperty("user.name") + ".hosts", "*");
      conf.set("hadoop.proxyuser." + System.getProperty("user.name") + ".groups", "*");
      UserGroupInformation.createUserForTesting("foo", new String[]{"all"});
      utility = new HBaseTestingUtility(conf);
      utility.startMiniCluster();
      miniZK = utility.getZkCluster();
      HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
      HColumnDescriptor hcd = new HColumnDescriptor(familyName);
      hcd.setMaxVersions(HConstants.ALL_VERSIONS);
      htd.addFamily(hcd);
      utility.getHBaseAdmin().createTable(htd);
    } catch (Throwable throwable) {
      LOG.error("Error in startup: " + throwable, throwable);
      throw throwable;
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    try {
      utility.getHBaseAdmin().disableTable(tableName);
      utility.getHBaseAdmin().deleteTable(tableName);
    } finally {
      utility.shutdownMiniCluster();
    }
  }

  @Test(timeout=60000)
  public void validateNoConfigIssues() throws Exception {
    TargetRunner targetRunner =
        new TargetRunner.Builder(HBaseDTarget.class)
            .addConfiguration("zookeeperQuorum", "127.0.0.1")
            .addConfiguration("clientPort", miniZK.getClientPort())
            .addConfiguration("zookeeperParentZnode", "/hbase")
            .addConfiguration("tableName", tableName)
            .addConfiguration("hbaseRowKey", "[0]")
            .addConfiguration("hbaseFieldColumnMapping",
              ImmutableList.of(new HBaseFieldMappingConfig("cf:a", "[1]", StorageType.TEXT)))
            .addConfiguration("kerberosAuth", false)
            .addConfiguration("hbaseConfigs", new HashMap<String, String>())
            .addConfiguration("hbaseUser", "")
            .addConfiguration("hbaseConfDir", "")
            .addConfiguration("rowKeyStorageType", StorageType.BINARY)
            .addConfiguration("implicitFieldMapping", false)
            .addConfiguration("ignoreMissingFieldPath", false)
            .addConfiguration("ignoreInvalidColumn", false)
            .addConfiguration("timeDriver", "${time:now()}")
            .setOnRecordError(OnRecordError.DISCARD).build();
    assertTrue(targetRunner.runValidateConfigs().isEmpty());
  }

  @Test(timeout=60000)
  public void testInvalidConfigs() throws Exception {
    HBaseDTarget dTarget = new ForTestHBaseTarget();
    configure(dTarget);
    dTarget.zookeeperQuorum = "";
    dTarget.clientPort = 0;
    dTarget.zookeeperParentZnode = "";
    dTarget.timeDriver = "${time:now()}";
    HBaseTarget target = (HBaseTarget) dTarget.createTarget();

    TargetRunner runner = new TargetRunner.Builder(HBaseDTarget.class, target)
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    Assert.assertEquals(3, issues.size());
    assertTrue(issues.get(0).toString().contains("HBASE_04"));
    assertTrue(issues.get(1).toString().contains("HBASE_09"));
    assertTrue(issues.get(2).toString().contains("HBASE_13"));

    configure(dTarget);
    dTarget.tableName = "NonExistingTable";
    target = (HBaseTarget) dTarget.createTarget();
    runner = new TargetRunner.Builder(HBaseDTarget.class, target)
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .build();
    issues = runner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
    assertTrue(issues.get(0).toString().contains("HBASE_07"));

    configure(dTarget);
    dTarget.zookeeperQuorum = "dummyhost";
    target = (HBaseTarget) dTarget.createTarget();
    runner = new TargetRunner.Builder(HBaseDTarget.class, target)
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .build();
    issues = runner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
    assertTrue(issues.get(0).toString().contains("HBASE_06"));
    assertTrue(issues.get(0).toString().contains("UnknownHostException"));
  }

  @Test(timeout=60000)
  public void getGetHBaseConfiguration() throws Exception {
    HBaseDTarget dTarget = new ForTestHBaseTarget();
    configure(dTarget);
    HBaseTarget target = (HBaseTarget) dTarget.createTarget();
    try {
      target.init(null,
                  ContextInfoCreator.createTargetContext("n", false, OnRecordError.TO_ERROR));
      assertNotNull(target.getHBaseConfiguration());
    } finally {
      target.destroy();
    }
  }

  @Test(timeout=60000)
  public void testHBaseConfigs() throws Exception {
    HBaseDTarget dTarget = new ForTestHBaseTarget();
    configure(dTarget);
    HBaseTarget target = (HBaseTarget) dTarget.createTarget();
    try {
      target.init(null,
                  ContextInfoCreator.createTargetContext("n", false, OnRecordError.TO_ERROR));
      assertEquals("X", target.getHBaseConfiguration().get("x"));
    } finally {
      target.destroy();
    }
  }

  @Test(timeout=60000)
  public void testSingleRecordTextStorage() throws InterruptedException, StageException,
      IOException {
    List<HBaseFieldMappingConfig> fieldMappings =
        ImmutableList.of(new HBaseFieldMappingConfig("cf:a", "[1]", StorageType.TEXT),
          new HBaseFieldMappingConfig("cf:b", "[2]", StorageType.TEXT),
          new HBaseFieldMappingConfig("cf:c", "[3]", StorageType.TEXT),
          new HBaseFieldMappingConfig("cf:d", "[4]", StorageType.TEXT));

    TargetRunner targetRunner = buildRunner(fieldMappings, StorageType.TEXT, OnRecordError.DISCARD, "", false, "[0]", false, false);

    Record record = RecordCreator.create();
    List<Field> fields = new ArrayList<>();
    String rowKey = "row_key";
    fields.add(Field.create(rowKey));
    fields.add(Field.create(20));
    fields.add(Field.create(30));
    fields.add(Field.create(40));
    fields.add(Field.create(50));
    record.set(Field.create(fields));

    List<Record> singleRecord = ImmutableList.of(record);
    targetRunner.runInit();
    targetRunner.runWrite(singleRecord);
    targetRunner.runDestroy();

    HTable htable = new HTable(conf, tableName);

    Get g = new Get(Bytes.toBytes(rowKey));
    Result r = htable.get(g);
    assertEquals("20", Bytes.toString(r.getValue(Bytes.toBytes(familyName), Bytes.toBytes("a"))));
    assertEquals("30", Bytes.toString(r.getValue(Bytes.toBytes(familyName), Bytes.toBytes("b"))));
    assertEquals("40", Bytes.toString(r.getValue(Bytes.toBytes(familyName), Bytes.toBytes("c"))));
    assertEquals("50", Bytes.toString(r.getValue(Bytes.toBytes(familyName), Bytes.toBytes("d"))));

  }

  @Test(timeout = 600000)
  public void testOnlyImplicitFieldMapping() throws Exception {
    String rowKeyFieldPath = "/row_key";
    String rowKey = "testOnlyImplicitFieldMapping";
    TargetRunner targetRunner =
      buildRunner(new ArrayList<HBaseFieldMappingConfig>(), StorageType.TEXT, OnRecordError.DISCARD, "", true, rowKeyFieldPath, false, false);
    Record record = RecordCreator.create();
    LinkedHashMap<String, Field> map = new LinkedHashMap<>();
    map.put("cf:a", Field.create("value_a"));
    map.put("cf:b", Field.create("value_b"));
    map.put(rowKeyFieldPath.substring(1), Field.create(rowKey));
    Field mapField = Field.createListMap(map);
    record.set(mapField);
    List<Record> singleRecord = ImmutableList.of(record);
    targetRunner.runInit();
    targetRunner.runWrite(singleRecord);
    targetRunner.runDestroy();
    HTable htable = new HTable(conf, tableName);
    Get g = new Get(Bytes.toBytes(rowKey));
    Result r = htable.get(g);
    assertEquals("value_a", Bytes.toString(r.getValue(Bytes.toBytes(familyName), Bytes.toBytes("a"))));
    assertEquals("value_b", Bytes.toString(r.getValue(Bytes.toBytes(familyName), Bytes.toBytes("b"))));
  }

  @Test(timeout = 60000)
  public void testFieldMappings() throws Exception {
    String rowKeyFieldPath = "/row_key";
    String rowKey = "testFieldMappings";
    List<HBaseFieldMappingConfig> fieldMappings =
      ImmutableList.of(new HBaseFieldMappingConfig("cf:explicit", "/explicit", StorageType.TEXT));
    TargetRunner targetRunner = buildRunner(fieldMappings, StorageType.TEXT, OnRecordError.DISCARD, "", true, rowKeyFieldPath, false, false);
    Record record = RecordCreator.create();
    Map<String, Field> map = new HashMap<>();
    map.put("cf:a", Field.create("value_a"));
    map.put(rowKeyFieldPath.substring(1), Field.create(rowKey));
    map.put("cf:b", Field.create("value_b"));
    map.put("explicit", Field.create("explicitValue"));
    Field mapField = Field.create(map);
    record.set(mapField);
    List<Record> singleRecord = ImmutableList.of(record);
    targetRunner.runInit();
    targetRunner.runWrite(singleRecord);
    targetRunner.runDestroy();

    HTable htable = new HTable(conf, tableName);

    Get g = new Get(Bytes.toBytes(rowKey));
    Result r = htable.get(g);
    assertEquals("value_a", Bytes.toString(r.getValue(Bytes.toBytes(familyName), Bytes.toBytes("a"))));
    assertEquals("value_b", Bytes.toString(r.getValue(Bytes.toBytes(familyName), Bytes.toBytes("b"))));
    assertEquals("explicitValue", Bytes.toString(r.getValue(Bytes.toBytes(familyName), Bytes.toBytes("explicit"))));
  }

  @Test(timeout = 60000)
  public void testFieldMappingIgnoreInvalidColumn() throws Exception {
    String rowKeyFieldPath = "/row_key";
    String rowKey = "testFieldMappingIgnoreOnError";
    List<HBaseFieldMappingConfig> fieldMappings =
      ImmutableList.of(new HBaseFieldMappingConfig("cf:explicit", "/explicit", StorageType.TEXT));
    TargetRunner targetRunner = buildRunner(fieldMappings, StorageType.TEXT, OnRecordError.DISCARD, "", true, rowKeyFieldPath, false, true);
    Record record = RecordCreator.create();
    Map<String, Field> map = new HashMap<>();
    Map<String, Field> dummyInnerMap = new HashMap<>();
    dummyInnerMap.put("f1", Field.create("abc"));
    // some invalid fieldpath as it cannot be mapped to column
    map.put("mymap", Field.create(dummyInnerMap));
    map.put("cf:a", Field.create("value_a"));
    map.put(rowKeyFieldPath.substring(1), Field.create(rowKey));
    map.put("cf:b", Field.create("value_b"));
    // some invalid fieldpath as it cannot be mapped to column
    map.put("dummy", Field.create("value_b"));
    map.put("explicit", Field.create("explicitValue"));
    Field mapField = Field.create(map);
    record.set(mapField);
    List<Record> singleRecord = ImmutableList.of(record);
    targetRunner.runInit();
    targetRunner.runWrite(singleRecord);
    targetRunner.runDestroy();

    HTable htable = new HTable(conf, tableName);

    Get g = new Get(Bytes.toBytes(rowKey));
    Result r = htable.get(g);
    assertEquals("value_a", Bytes.toString(r.getValue(Bytes.toBytes(familyName), Bytes.toBytes("a"))));
    assertEquals("value_b", Bytes.toString(r.getValue(Bytes.toBytes(familyName), Bytes.toBytes("b"))));
    assertEquals("explicitValue", Bytes.toString(r.getValue(Bytes.toBytes(familyName), Bytes.toBytes("explicit"))));
  }

  @Test(timeout = 60000)
  public void testFieldMappingNotIgnoreInvalidColumn() throws Exception {
    String rowKeyFieldPath = "/row_key";
    String rowKey = "testAllFieldMappingError";
    List<HBaseFieldMappingConfig> fieldMappings =
      ImmutableList.of(new HBaseFieldMappingConfig("cf:explicit", "/explicit", StorageType.TEXT));
    TargetRunner targetRunner = buildRunner(fieldMappings, StorageType.TEXT, OnRecordError.TO_ERROR, "", true, rowKeyFieldPath, true, false);
    Record record = RecordCreator.create();
    Map<String, Field> map = new HashMap<>();
    Map<String, Field> dummyInnerMap = new HashMap<>();
    dummyInnerMap.put("f1", Field.create("abc"));
    map.put("mymap", Field.create(dummyInnerMap));
    map.put(rowKeyFieldPath.substring(1), Field.create(rowKey));
    map.put("dummy", Field.create("value_b"));
    Field mapField = Field.create(map);
    record.set(mapField);
    List<Record> singleRecord = ImmutableList.of(record);
    targetRunner.runInit();
    targetRunner.runWrite(singleRecord);
    assertEquals(1, targetRunner.getErrorRecords().size());
    assertEquals(record.getHeader().getSourceId(), targetRunner.getErrorRecords().get(0).getHeader().getSourceId());
    assertTrue(targetRunner.getErrors().isEmpty());
    targetRunner.runDestroy();

    HTable htable = new HTable(conf, tableName);
    Get g = new Get(Bytes.toBytes(rowKey));
    Result r = htable.get(g);
    assertTrue(r.isEmpty());

  }

  @Test
  public void testWriteWrongColumn() throws Exception {
    String rowKeyFieldPath = "/row_key";
    String rowKey = "testWriteWrongColumn";
    TargetRunner targetRunner = buildRunner(new ArrayList<HBaseFieldMappingConfig>(), StorageType.TEXT,  OnRecordError.TO_ERROR, "", true, rowKeyFieldPath, true, false);
    Record record = RecordCreator.create();
    Map<String, Field> map = new HashMap<>();
    map.put("invalidcf:a", Field.create("value_a"));
    map.put("cf:b", Field.create("value_b"));
    map.put(rowKeyFieldPath.substring(1), Field.create(rowKey));
    record.set(Field.create(map));
    List<Record> singleRecord = ImmutableList.of(record);
    targetRunner.runInit();
    targetRunner.runWrite(singleRecord);
    assertEquals(1, targetRunner.getErrorRecords().size());
    assertEquals(record.getHeader().getSourceId(), targetRunner.getErrorRecords().get(0).getHeader().getSourceId());
    assertTrue(targetRunner.getErrors().isEmpty());
    targetRunner.runDestroy();

    HTable htable = new HTable(conf, tableName);
    Get g = new Get(Bytes.toBytes(rowKey));
    Result r = htable.get(g);
    assertTrue(r.isEmpty());

  }

  @Test
  public void testInvalidRowKey() throws Exception {
    String rowKeyFieldPath = "/row_key";
    TargetRunner targetRunner = buildRunner(new ArrayList<HBaseFieldMappingConfig>(), StorageType.TEXT,  OnRecordError.TO_ERROR, "", true, rowKeyFieldPath, true, false);

    Record record1 = RecordCreator.create();
    Map<String, Field> map = new HashMap<>();
    map.put("cf:a", Field.create("value_a"));
    map.put("cf:b", Field.create("value_b"));
    map.put(rowKeyFieldPath.substring(1), Field.create(""));
    record1.set(Field.create(map));

    Record record2 = RecordCreator.create();
    map = new HashMap<>();
    map.put("cf:a", Field.create("value_a"));
    map.put("cf:b", Field.create("value_b"));
    record2.set(Field.create(map));

    List<Record> records = ImmutableList.of(record1, record2);
    targetRunner.runInit();
    targetRunner.runWrite(records);
    assertEquals(2, targetRunner.getErrorRecords().size());
    assertEquals(Errors.HBASE_35.getCode(), targetRunner.getErrorRecords().get(0).getHeader().getErrorCode());
    assertEquals(Errors.HBASE_27.getCode(), targetRunner.getErrorRecords().get(1).getHeader().getErrorCode());
    targetRunner.runDestroy();
  }

  @Test(timeout = 60000)
  public void testNotFlatMap() throws Exception {
    String rowKeyFieldPath = "/row_key";
    String rowKey = "testNotFlatMapError";
    TargetRunner targetRunner =
      buildRunner(new ArrayList<HBaseFieldMappingConfig>(), StorageType.TEXT, OnRecordError.DISCARD, "", true, rowKeyFieldPath, false, false);
    Record record = RecordCreator.create();
    Map<String, Field> map = new HashMap<>();
    map.put("cf:a", Field.create("value_a"));
    map.put("cf:b", Field.create("value_b"));
    map.put("cf:c", Field.create(ImmutableMap.of("key_1", Field.create(60), "key_2", Field.create(70))));
    map.put(rowKeyFieldPath.substring(1), Field.create(rowKey));
    Field mapField = Field.create(map);
    record.set(mapField);
    List<Record> singleRecord = ImmutableList.of(record);
    targetRunner.runInit();
    targetRunner.runWrite(singleRecord);
    targetRunner.runDestroy();
    HTable htable = new HTable(conf, tableName);
    Get g = new Get(Bytes.toBytes(rowKey));
    Result r = htable.get(g);
    assertTrue(!r.isEmpty());
    assertEquals("value_a", Bytes.toString(r.getValue(Bytes.toBytes(familyName), Bytes.toBytes("a"))));
    assertEquals("value_b", Bytes.toString(r.getValue(Bytes.toBytes(familyName), Bytes.toBytes("b"))));
    Field field = JsonUtil.bytesToField(r.getValue(Bytes.toBytes(familyName), Bytes.toBytes("c")));
    assertTrue(field.getType() == Type.MAP);
    map = field.getValueAsMap();
    assertEquals(60, map.get("key_1").getValueAsInteger());
    assertEquals(70, map.get("key_2").getValueAsInteger());
  }

  @Test(timeout = 60000)
  public void testNotMapError() throws Exception {
    String rowKey = "testNotMapError";
    TargetRunner targetRunner =
      buildRunner(new ArrayList<HBaseFieldMappingConfig>(), StorageType.TEXT, OnRecordError.TO_ERROR, "", true, "[0]", false, true);
    Record record = RecordCreator.create();
    List<Field> fields = new ArrayList<Field>();
    fields.add(Field.create(rowKey));
    fields.add(Field.create("20"));
    record.set(Field.create(fields));
    List<Record> singleRecord = ImmutableList.of(record);
    targetRunner.runInit();
    targetRunner.runWrite(singleRecord);
    assertEquals(1, targetRunner.getErrorRecords().size());
    assertEquals(record.getHeader().getSourceId(), targetRunner.getErrorRecords().get(0).getHeader().getSourceId());
    assertTrue(targetRunner.getErrors().isEmpty());
    targetRunner.runDestroy();
    HTable htable = new HTable(conf, tableName);
    Get g = new Get(Bytes.toBytes(rowKey));
    Result r = htable.get(g);
    assertTrue(r.isEmpty());
  }

  @Test(timeout=60000)
  public void testSingleRecordBinaryStorage() throws InterruptedException, StageException,
      IOException {
    List<HBaseFieldMappingConfig> fieldMappings =
        ImmutableList.of(new HBaseFieldMappingConfig("cf:a", "[1]", StorageType.BINARY),
          new HBaseFieldMappingConfig("cf:b", "[2]", StorageType.BINARY),
          new HBaseFieldMappingConfig("cf:c", "[3]", StorageType.TEXT),
          new HBaseFieldMappingConfig("cf:d", "[4]", StorageType.TEXT));

    TargetRunner targetRunner =
        buildRunner(fieldMappings, StorageType.BINARY, OnRecordError.DISCARD, "", false, "[0]", false, false);

    Record record = RecordCreator.create();
    List<Field> fields = new ArrayList<>();
    int rowKey = 123;
    fields.add(Field.create(rowKey));
    fields.add(Field.create(20));
    fields.add(Field.create(30));
    fields.add(Field.create(40));
    fields.add(Field.create(50));
    record.set(Field.create(fields));

    List<Record> singleRecord = ImmutableList.of(record);
    targetRunner.runInit();
    targetRunner.runWrite(singleRecord);

    // Should not be any error records.
    assertTrue(targetRunner.getErrorRecords().isEmpty());
    assertTrue(targetRunner.getErrors().isEmpty());

    targetRunner.runDestroy();

    HTable htable = new HTable(conf, tableName);
    Get g = new Get(Bytes.toBytes(rowKey));
    Result r = htable.get(g);
    assertEquals(20, Bytes.toInt(r.getValue(Bytes.toBytes(familyName), Bytes.toBytes("a"))));
    assertEquals(30, Bytes.toInt(r.getValue(Bytes.toBytes(familyName), Bytes.toBytes("b"))));
    assertEquals("40", Bytes.toString(r.getValue(Bytes.toBytes(familyName), Bytes.toBytes("c"))));
    assertEquals("50", Bytes.toString(r.getValue(Bytes.toBytes(familyName), Bytes.toBytes("d"))));

  }

  @Test(timeout=60000)
  public void testMultipleRecords() throws InterruptedException, StageException, IOException {
    // first two columns are binary, other are text
    List<HBaseFieldMappingConfig> fieldMappings =
        ImmutableList.of(new HBaseFieldMappingConfig("cf:a", "[1]", StorageType.BINARY),
          new HBaseFieldMappingConfig("cf:b", "[2]", StorageType.BINARY),
          new HBaseFieldMappingConfig("cf:c", "[3]", StorageType.TEXT),
          new HBaseFieldMappingConfig("cf:d", "[4]", StorageType.TEXT));

    TargetRunner targetRunner =
        buildRunner(fieldMappings, StorageType.BINARY, OnRecordError.DISCARD, "", false, "[0]", false, false);

    // Add two records
    Record record = RecordCreator.create();
    List<Field> fields = new ArrayList<>();
    int rowKey = 1111;
    fields.add(Field.create(rowKey));
    fields.add(Field.create(20));
    fields.add(Field.create(30));
    fields.add(Field.create(40));
    fields.add(Field.create(50));
    record.set(Field.create(fields));

    Record record2 = RecordCreator.create();
    List<Field> fields2 = new ArrayList<>();
    int rowKey2 = 2222;
    fields2.add(Field.create(rowKey2));
    fields2.add(Field.create(60));
    fields2.add(Field.create(70));
    fields2.add(Field.create(80));
    fields2.add(Field.create(90));
    record2.set(Field.create(fields2));

    List<Record> multipleRecords = ImmutableList.of(record, record2);
    targetRunner.runInit();
    targetRunner.runWrite(multipleRecords);

    // Should not be any error records.
    assertTrue(targetRunner.getErrorRecords().isEmpty());
    assertTrue(targetRunner.getErrors().isEmpty());

    targetRunner.runDestroy();

    HTable htable = new HTable(conf, tableName);
    Get g = new Get(Bytes.toBytes(rowKey));
    Result r = htable.get(g);
    assertEquals(20, Bytes.toInt(r.getValue(Bytes.toBytes(familyName), Bytes.toBytes("a"))));
    assertEquals(30, Bytes.toInt(r.getValue(Bytes.toBytes(familyName), Bytes.toBytes("b"))));
    assertEquals("40", Bytes.toString(r.getValue(Bytes.toBytes(familyName), Bytes.toBytes("c"))));
    assertEquals("50", Bytes.toString(r.getValue(Bytes.toBytes(familyName), Bytes.toBytes("d"))));

    g = new Get(Bytes.toBytes(rowKey2));
    r = htable.get(g);
    assertEquals(60, Bytes.toInt(r.getValue(Bytes.toBytes(familyName), Bytes.toBytes("a"))));
    assertEquals(70, Bytes.toInt(r.getValue(Bytes.toBytes(familyName), Bytes.toBytes("b"))));
    assertEquals("80", Bytes.toString(r.getValue(Bytes.toBytes(familyName), Bytes.toBytes("c"))));
    assertEquals("90", Bytes.toString(r.getValue(Bytes.toBytes(familyName), Bytes.toBytes("d"))));

  }

  @Test(timeout=60000)
  public void testCollectionTypes() throws InterruptedException, StageException, IOException {
    // first column binary, second text and last two are json string
    List<HBaseFieldMappingConfig> fieldMappings =
        ImmutableList.of(new HBaseFieldMappingConfig("cf:a", "[1]", StorageType.BINARY),
          new HBaseFieldMappingConfig("cf:b", "[2]", StorageType.TEXT),
          new HBaseFieldMappingConfig("cf:c", "[3]", StorageType.JSON_STRING),
          new HBaseFieldMappingConfig("cf:d", "[4]", StorageType.JSON_STRING));

    TargetRunner targetRunner =
        buildRunner(fieldMappings, StorageType.BINARY, OnRecordError.DISCARD, "", false, "[0]", false, false);

    Record record = RecordCreator.create();
    List<Field> fields = new ArrayList<>();
    int rowKey = 123;
    fields.add(Field.create(rowKey));
    fields.add(Field.create(20));
    fields.add(Field.create(30));
    fields.add(Field.create(ImmutableList.of(Field.create(40), Field.create(50))));
    fields.add(Field.create(ImmutableMap.of("key_1", Field.create(60), "key_2", Field.create(70))));
    record.set(Field.create(fields));

    List<Record> singleRecord = ImmutableList.of(record);
    targetRunner.runInit();
    targetRunner.runWrite(singleRecord);

    // Should not be any error records.
    assertTrue(targetRunner.getErrorRecords().isEmpty());
    assertTrue(targetRunner.getErrors().isEmpty());

    targetRunner.runDestroy();

    HTable htable = new HTable(conf, tableName);
    Get g = new Get(Bytes.toBytes(rowKey));
    Result r = htable.get(g);
    assertEquals(20, Bytes.toInt(r.getValue(Bytes.toBytes(familyName), Bytes.toBytes("a"))));
    assertEquals("30", Bytes.toString(r.getValue(Bytes.toBytes(familyName), Bytes.toBytes("b"))));
    // deserialize to json
    Field field = JsonUtil.bytesToField(r.getValue(Bytes.toBytes(familyName), Bytes.toBytes("c")));
    assertTrue(field.getType() == Type.LIST);
    List<Field> list = field.getValueAsList();
    assertEquals(40, list.get(0).getValueAsInteger());
    assertEquals(50, list.get(1).getValueAsInteger());

    field = JsonUtil.bytesToField(r.getValue(Bytes.toBytes(familyName), Bytes.toBytes("d")));
    assertTrue(field.getType() == Type.MAP);
    Map<String, Field> map = field.getValueAsMap();
    assertEquals(60, map.get("key_1").getValueAsInteger());
    assertEquals(70, map.get("key_2").getValueAsInteger());
  }

  @Test(timeout=60000)
  public void testWriteRecordsOnErrorDiscard() throws InterruptedException, StageException,
      IOException {
    List<HBaseFieldMappingConfig> fieldMappings =
        ImmutableList.of(new HBaseFieldMappingConfig("cf:a", "[1]", StorageType.BINARY),
          new HBaseFieldMappingConfig("cf:b", "[2]", StorageType.BINARY),
          new HBaseFieldMappingConfig("cf:c", "[3]", StorageType.TEXT),
          new HBaseFieldMappingConfig("cf:d", "[4]", StorageType.TEXT));

    TargetRunner targetRunner =
        buildRunner(fieldMappings, StorageType.BINARY, OnRecordError.DISCARD, "", false, "[0]", false, false);

    Record record = RecordCreator.create();
    List<Field> fields = new ArrayList<>();
    String rowKey = "val"; // Invalid as row key storage type is binary
    fields.add(Field.create(rowKey));
    fields.add(Field.create(20)); // Invalid
    fields.add(Field.create(30)); // Invalid
    fields.add(Field.create(40));
    fields.add(Field.create(50));
    record.set(Field.create(fields));

    List<Record> singleRecord = ImmutableList.of(record);
    targetRunner.runInit();
    targetRunner.runWrite(singleRecord);

    // Should not be any error records if we are discarding.
    assertTrue(targetRunner.getErrorRecords().isEmpty());
    assertTrue(targetRunner.getErrors().isEmpty());

    targetRunner.runDestroy();

    HTable htable = new HTable(conf, tableName);
    Get g = new Get(Bytes.toBytes(rowKey));
    Result r = htable.get(g);
    assertTrue(r.isEmpty());

  }

  @Test(timeout=60000)
  public void testWriteRecordsOnErrorToError() throws InterruptedException, StageException,
      IOException {
    List<HBaseFieldMappingConfig> fieldMappings =
        ImmutableList.of(new HBaseFieldMappingConfig("cf:a", "[1]", StorageType.BINARY),
          new HBaseFieldMappingConfig("cf:b", "[2]", StorageType.BINARY),
          new HBaseFieldMappingConfig("cf:c", "[3]", StorageType.TEXT),
          new HBaseFieldMappingConfig("cf:d", "[4]", StorageType.TEXT));

    TargetRunner targetRunner =
        buildRunner(fieldMappings, StorageType.BINARY, OnRecordError.TO_ERROR, "", false, "[0]", false, false);

    Record record = RecordCreator.create();
    List<Field> fields = new ArrayList<>();
    int rowKey = 123;
    fields.add(Field.create(rowKey));
    fields.add(Field.create("20")); // Invalid field as this cannot be mapped to binary
    fields.add(Field.create("30")); // Invalid field as this cannot be mapped to binary
    fields.add(Field.create(40));
    fields.add(Field.create(50));
    record.set(Field.create(fields));

    List<Record> singleRecord = ImmutableList.of(record);
    targetRunner.runInit();
    targetRunner.runWrite(singleRecord);

    // Should not be any error records if we are discarding.
    assertEquals(1, targetRunner.getErrorRecords().size());
    assertTrue(targetRunner.getErrors().isEmpty());

    targetRunner.runDestroy();

    HTable htable = new HTable(conf, tableName);
    Get g = new Get(Bytes.toBytes(rowKey));
    Result r = htable.get(g);
    assertTrue(r.isEmpty());

  }

  @Test(timeout=60000)
  public void testWriteRecordsOnErrorToStopPipeline() throws InterruptedException, StageException,
      IOException {
    List<HBaseFieldMappingConfig> fieldMappings =
        ImmutableList.of(new HBaseFieldMappingConfig("cf:a", "[1]", StorageType.JSON_STRING),
          new HBaseFieldMappingConfig("cf:b", "[2]", StorageType.TEXT),
          new HBaseFieldMappingConfig("cf:c", "[3]", StorageType.TEXT),
          new HBaseFieldMappingConfig("cf:d", "[4]", StorageType.TEXT));

    TargetRunner targetRunner =
        buildRunner(fieldMappings, StorageType.BINARY, OnRecordError.STOP_PIPELINE, "", false, "[0]", false, false);

    Record record = RecordCreator.create();
    List<Field> fields = new ArrayList<>();
    int rowKey = 123;
    fields.add(Field.create(rowKey));
    fields.add(Field.create("20")); // Invalid field as this cannot be mapped to json string
    fields.add(Field.create("30"));
    fields.add(Field.create(40));
    fields.add(Field.create(50));
    record.set(Field.create(fields));

    List<Record> singleRecord = ImmutableList.of(record);
    targetRunner.runInit();
    try {
      targetRunner.runWrite(singleRecord);
      fail("Expected exception but didn't get any");
    } catch (Exception e) {
      assertTrue(e.getCause() instanceof StageException);
    }
  }

  @Test(timeout=60000)
  public void testMultipleRecordsOnError() throws InterruptedException, StageException, IOException {
    List<HBaseFieldMappingConfig> fieldMappings =
        ImmutableList.of(new HBaseFieldMappingConfig("cf:a", "[1]", StorageType.BINARY),
          new HBaseFieldMappingConfig("cf:b", "[2]", StorageType.BINARY),
          new HBaseFieldMappingConfig("cf:c", "[3]", StorageType.TEXT),
          new HBaseFieldMappingConfig("cf:d", "[4]", StorageType.TEXT));

    TargetRunner targetRunner =
        buildRunner(fieldMappings, StorageType.TEXT, OnRecordError.TO_ERROR, "", false, "[0]", false, false);

    Record record = RecordCreator.create();
    List<Field> fields = new ArrayList<>();

    String rowKey = "testMultipleRecordsOnError1";
    // / Invalid record
    fields.add(Field.create(rowKey));
    fields.add(Field.create("20")); // Invalid
    fields.add(Field.create("30")); // Invalid
    fields.add(Field.create(40));
    fields.add(Field.create(50));
    record.set(Field.create(fields));

    // Now enter a valid record
    Record record2 = RecordCreator.create();
    List<Field> fields2 = new ArrayList<>();
    String rowKey2 = "testMultipleRecordsOnError2";
    fields2.add(Field.create(rowKey2));
    fields2.add(Field.create(60));
    fields2.add(Field.create(70));
    fields2.add(Field.create(80));
    fields2.add(Field.create(90));
    record2.set(Field.create(fields2));

    List<Record> multipleRecords = ImmutableList.of(record, record2);
    targetRunner.runInit();
    // Record1 is invalid, record 2 is valid
    targetRunner.runWrite(multipleRecords);

    assertEquals(1, targetRunner.getErrorRecords().size());
    assertTrue(targetRunner.getErrors().isEmpty());

    targetRunner.runDestroy();

    // First row is not inserted
    HTable htable = new HTable(conf, tableName);
    Get g = new Get(Bytes.toBytes(rowKey));
    Result r = htable.get(g);
    assertTrue(r.isEmpty());

    g = new Get(Bytes.toBytes(rowKey2));
    r = htable.get(g);
    assertEquals(60, Bytes.toInt(r.getValue(Bytes.toBytes(familyName), Bytes.toBytes("a"))));
    assertEquals(70, Bytes.toInt(r.getValue(Bytes.toBytes(familyName), Bytes.toBytes("b"))));
    assertEquals("80", Bytes.toString(r.getValue(Bytes.toBytes(familyName), Bytes.toBytes("c"))));
    assertEquals("90", Bytes.toString(r.getValue(Bytes.toBytes(familyName), Bytes.toBytes("d"))));
  }

  @Test(timeout=60000)
  public void testCustomTimeBasis() throws InterruptedException, StageException, IOException, ParseException {
    List<HBaseFieldMappingConfig> fieldMappings =
        ImmutableList.of(new HBaseFieldMappingConfig("cf:a", "[1]", StorageType.TEXT),
            new HBaseFieldMappingConfig("cf:b", "[2]", StorageType.TEXT),
            new HBaseFieldMappingConfig("cf:c", "[3]", StorageType.TEXT),
            new HBaseFieldMappingConfig("cf:d", "[4]", StorageType.TEXT));

    TargetRunner targetRunner =
        buildRunner(fieldMappings, StorageType.TEXT, OnRecordError.TO_ERROR, "", false, "[0]", false, false,
            "${record:value('[4]')}");

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    Record record1 = RecordCreator.create();
    List<Field> fields1 = new ArrayList<>();
    String rowKey = "testCustomTimeBasis1";
    fields1.add(Field.create(rowKey));
    fields1.add(Field.create(70));
    fields1.add(Field.create(80));
    fields1.add(Field.create(90));
    Date d = sdf.parse("1986-01-17");
    fields1.add(Field.create(Type.DATE, d));
    record1.set(Field.create(fields1));

    Record record2 = RecordCreator.create();
    List<Field> fields2 = new ArrayList<>();
    String rowKey2 = "testCustomTimeBasis2";
    fields2.add(Field.create(rowKey2));
    fields2.add(Field.create(60));
    fields2.add(Field.create(70));
    fields2.add(Field.create(80));
    d = sdf.parse("1988-01-19");
    fields2.add(Field.create(Type.DATE, d));
    record2.set(Field.create(fields2));

    Record record3 = RecordCreator.create();
    List<Field> fields3 = new ArrayList<>();
    fields3.add(Field.create(rowKey));
    fields3.add(Field.create(60));
    fields3.add(Field.create(70));
    fields3.add(Field.create(80));
    d = sdf.parse("1993-01-18");
    fields3.add(Field.create(Type.DATE, d));
    record3.set(Field.create(fields3));

    List<Record> multipleRecords = ImmutableList.of(record1, record2, record3);
    targetRunner.runInit();
    targetRunner.runWrite(multipleRecords);

    targetRunner.runDestroy();

    HTable htable = new HTable(conf, tableName);
    Get g = new Get(Bytes.toBytes(rowKey));
    g.setMaxVersions(HConstants.ALL_VERSIONS);
    Result r = htable.get(g);
    NavigableMap<Long, byte[]> map = r.getMap().get(Bytes.toBytes("cf")).get(Bytes.toBytes("a"));

    assertEquals(2, map.size());
    Set<Long> timestamps = Sets.newHashSet(sdf.parse("1993-01-18").getTime(), sdf.parse("1986-01-17").getTime());
    for (Long ts : map.keySet()) {
      assertTrue(timestamps.contains(ts));
    }

    g = new Get(Bytes.toBytes(rowKey2));
    g.setMaxVersions(HConstants.ALL_VERSIONS);
    r = htable.get(g);
    map = r.getMap().get(Bytes.toBytes("cf")).get(Bytes.toBytes("a"));
    assertEquals(1, map.size());
    assertEquals(Long.valueOf(sdf.parse("1988-01-19").getTime()), map.firstKey());
  }

  @Test(timeout=60000)
  public void testEmptyTimeBasis() throws InterruptedException, StageException, IOException, ParseException {
    List<HBaseFieldMappingConfig> fieldMappings =
        ImmutableList.of(new HBaseFieldMappingConfig("cf:a", "[1]", StorageType.BINARY),
            new HBaseFieldMappingConfig("cf:b", "[2]", StorageType.TEXT),
            new HBaseFieldMappingConfig("cf:c", "[3]", StorageType.TEXT));

    TargetRunner targetRunner =
        buildRunner(fieldMappings, StorageType.TEXT, OnRecordError.TO_ERROR, "", false, "[0]", false, false, "");

    Record record1 = RecordCreator.create();
    List<Field> fields1 = new ArrayList<>();
    String rowKey = "testEmptyTimeBasis1";
    fields1.add(Field.create(rowKey));
    fields1.add(Field.create(70));
    fields1.add(Field.create(80));
    fields1.add(Field.create(90));
    record1.set(Field.create(fields1));

    Record record2 = RecordCreator.create();
    List<Field> fields2 = new ArrayList<>();
    fields2.add(Field.create(rowKey));
    fields2.add(Field.create(60));
    fields2.add(Field.create(70));
    fields2.add(Field.create(80));
    record2.set(Field.create(fields2));

    targetRunner.runInit();
    targetRunner.runWrite(ImmutableList.of(record1));
    targetRunner.runWrite(ImmutableList.of(record2));

    assertEquals(0, targetRunner.getErrorRecords().size());

    HTable htable = new HTable(conf, tableName);
    Get g = new Get(Bytes.toBytes(rowKey));
    g.setMaxVersions(HConstants.ALL_VERSIONS);
    Result r = htable.get(g);
    NavigableMap<Long, byte[]> map = r.getMap().get(Bytes.toBytes("cf")).get(Bytes.toBytes("a"));
    assertEquals(2, map.size());
    assertArrayEquals(Bytes.toBytes(60), map.firstEntry().getValue());
    assertArrayEquals(Bytes.toBytes(70), map.lastEntry().getValue());
  }

  @Test(timeout=60000)
  public void testTimeNowTimeBasis() throws InterruptedException, StageException, IOException, ParseException {
    List<HBaseFieldMappingConfig> fieldMappings =
        ImmutableList.of(new HBaseFieldMappingConfig("cf:a", "[1]", StorageType.BINARY),
            new HBaseFieldMappingConfig("cf:b", "[2]", StorageType.TEXT),
            new HBaseFieldMappingConfig("cf:c", "[3]", StorageType.TEXT));

    TargetRunner targetRunner =
        buildRunner(fieldMappings, StorageType.TEXT, OnRecordError.TO_ERROR, "", false, "[0]", false, false,
            "${time:now()}");

    Record record1 = RecordCreator.create();
    List<Field> fields1 = new ArrayList<>();
    String rowKey = "testTimeNowTimeBasis1";
    fields1.add(Field.create(rowKey));
    fields1.add(Field.create(70));
    fields1.add(Field.create(80));
    fields1.add(Field.create(90));
    record1.set(Field.create(fields1));

    Record record2 = RecordCreator.create();
    List<Field> fields2 = new ArrayList<>();
    String rowKey2 = "testTimeNowTimeBasis2";
    fields2.add(Field.create(rowKey2));
    fields2.add(Field.create(60));
    fields2.add(Field.create(70));
    fields2.add(Field.create(80));
    record2.set(Field.create(fields2));

    Record record3 = RecordCreator.create();
    List<Field> fields3 = new ArrayList<>();
    fields3.add(Field.create(rowKey));
    fields3.add(Field.create(60));
    fields3.add(Field.create(70));
    fields3.add(Field.create(80));
    record3.set(Field.create(fields3));

    List<Record> multipleRecords = ImmutableList.of(record1, record2, record3);
    targetRunner.runInit();
    targetRunner.runWrite(multipleRecords);

    HTable htable = new HTable(conf, tableName);
    Get g = new Get(Bytes.toBytes(rowKey));
    g.setMaxVersions(HConstants.ALL_VERSIONS);
    Result r = htable.get(g);
    NavigableMap<Long, byte[]> map = r.getMap().get(Bytes.toBytes("cf")).get(Bytes.toBytes("a"));
    assertEquals(1, map.size());
    // last one wins with a single batch
    assertArrayEquals(Bytes.toBytes(60), map.firstEntry().getValue());

    targetRunner.runWrite(ImmutableList.of(record1));
    targetRunner.runDestroy();

    g = new Get(Bytes.toBytes(rowKey));
    g.setMaxVersions(HConstants.ALL_VERSIONS);
    r = htable.get(g);
    map = r.getMap().get(Bytes.toBytes("cf")).get(Bytes.toBytes("a"));
    assertEquals(2, map.size());
    assertArrayEquals(Bytes.toBytes(70), map.firstEntry().getValue());

    g = new Get(Bytes.toBytes(rowKey2));
    g.setMaxVersions(HConstants.ALL_VERSIONS);
    r = htable.get(g);
    map = r.getMap().get(Bytes.toBytes("cf")).get(Bytes.toBytes("a"));
    assertEquals(1, map.size());
  }

  @Test(timeout = 60000)
  public void testInvalidColumnFamily() throws Exception {
    List<HBaseFieldMappingConfig> fieldMappings =
      ImmutableList.of(new HBaseFieldMappingConfig("invalid_cf:a", "[1]", StorageType.BINARY), new HBaseFieldMappingConfig(
        "cf:b", "[2]", StorageType.BINARY));
    TargetRunner targetRunner =
      buildRunner(fieldMappings, StorageType.TEXT, OnRecordError.TO_ERROR, "", false, "[0]", false, false);
    List<ConfigIssue> configIssues = targetRunner.runValidateConfigs();
    assertEquals(1, configIssues.size());
    assertTrue(configIssues.get(0).toString().contains(Errors.HBASE_32.getCode()));
  }

  static class ForTestHBaseTarget extends HBaseDTarget {
    @Override
    protected Target createTarget() {
      return new HBaseTarget(zookeeperQuorum, clientPort, zookeeperParentZnode, tableName, hbaseRowKey,
        rowKeyStorageType, hbaseFieldColumnMapping, kerberosAuth, hbaseConfDir, hbaseConfigs, hbaseUser, implicitFieldMapping,
        ignoreMissingFieldPath, ignoreInvalidColumn, timeDriver) {
        @Override
        public void write(Batch batch) throws StageException {
        }
      };
    }
  }

  @Test
  public void testGetHBaseConfigurationWithResources() throws Exception {
    File resourcesDir = new File("target", UUID.randomUUID().toString());
    File fooDir = new File(resourcesDir, "foo");
    Assert.assertTrue(fooDir.mkdirs());
    Files.write("<configuration><property><name>xx</name><value>XX</value></property></configuration>",
                new File(fooDir, "hbase-site.xml"), StandardCharsets.UTF_8);
    HBaseDTarget dTarget = new ForTestHBaseTarget();
    configure(dTarget);
    dTarget.hbaseConfDir = fooDir.getName();
    HBaseTarget target = (HBaseTarget) dTarget.createTarget();
    try {
      target.init(null, ContextInfoCreator.createTargetContext(HBaseDTarget.class, "n", false,
                                                               OnRecordError.TO_ERROR,
                                                               resourcesDir.getAbsolutePath()));
      Configuration conf = target.getHBaseConfiguration();
      Assert.assertEquals("XX", conf.get("xx"));
    } finally {
      target.destroy();
    }

    // Provide hbaseConfDir as an absolute path
    File absoluteFilePath = new File(new File("target", UUID.randomUUID().toString()), "foo");
    Assert.assertTrue(absoluteFilePath.mkdirs());
    Files.write("<configuration><property><name>zz</name><value>ZZ</value></property></configuration>",
      new File(absoluteFilePath, "hbase-site.xml"), StandardCharsets.UTF_8);
    dTarget.hbaseConfDir = absoluteFilePath.getAbsolutePath();
    dTarget.timeDriver = "${time:now()}";
    target = (HBaseTarget) dTarget.createTarget();
    try {
      target.init(null, ContextInfoCreator.createTargetContext(HBaseDTarget.class, "n", false,
                                                               OnRecordError.TO_ERROR,
                                                               resourcesDir.getAbsolutePath()));
      Configuration conf = target.getHBaseConfiguration();
      Assert.assertEquals("ZZ", conf.get("zz"));
    } finally {
      target.destroy();
    }
  }

  private void configure(HBaseDTarget target) {
    target.zookeeperQuorum = "127.0.0.1";
    target.clientPort = miniZK.getClientPort();
    target.zookeeperParentZnode = "/hbase";
    target.tableName = tableName;
    target.hbaseRowKey = "[0]";
    target.rowKeyStorageType = StorageType.BINARY;
    target.hbaseConfigs = new HashMap<String, String>();
    target.hbaseConfigs.put("x", "X");
    target.hbaseFieldColumnMapping = new ArrayList<HBaseFieldMappingConfig>();
    target.hbaseFieldColumnMapping
        .add(new HBaseFieldMappingConfig("cf:a", "[1]", StorageType.TEXT));
    target.hbaseUser = "";
    target.timeDriver = "${time:now()}";
  }

  private TargetRunner buildRunner(List<HBaseFieldMappingConfig> fieldMappings,
                                   StorageType storageType, OnRecordError onRecordError, String hbaseUser, boolean implicitFieldMapping,
                                   String hbaseRowKey, boolean ignoreMissingFieldPath, boolean ignoreInvalidColumn) {
    return buildRunner(fieldMappings, storageType, onRecordError, hbaseUser, implicitFieldMapping, hbaseRowKey,
        ignoreMissingFieldPath, ignoreInvalidColumn, "${time:now()}");
  }

  private TargetRunner buildRunner(List<HBaseFieldMappingConfig> fieldMappings,
      StorageType storageType, OnRecordError onRecordError, String hbaseUser, boolean implicitFieldMapping,
      String hbaseRowKey, boolean ignoreMissingFieldPath, boolean ignoreInvalidColumn, String timeDriver) {
    TargetRunner targetRunner =
        new TargetRunner.Builder(HBaseDTarget.class)
            .addConfiguration("zookeeperQuorum", "127.0.0.1")
            .addConfiguration("clientPort", miniZK.getClientPort())
            .addConfiguration("zookeeperParentZnode", "/hbase")
            .addConfiguration("tableName", tableName)
            .addConfiguration("hbaseRowKey", hbaseRowKey)
            .addConfiguration("hbaseFieldColumnMapping", fieldMappings)
            .addConfiguration("kerberosAuth", false)
            .addConfiguration("hbaseConfDir", "")
            .addConfiguration("hbaseConfigs", new HashMap<String, String>())
            .addConfiguration("implicitFieldMapping", implicitFieldMapping)
            .addConfiguration("rowKeyStorageType", storageType).setOnRecordError(onRecordError)
            .addConfiguration("hbaseUser", hbaseUser)
            .addConfiguration("ignoreMissingFieldPath", ignoreMissingFieldPath)
            .addConfiguration("ignoreInvalidColumn", ignoreInvalidColumn)
            .addConfiguration("timeDriver", timeDriver)
            .build();
    return targetRunner;
  }

  private void testUser(String user) throws Exception {
    List<HBaseFieldMappingConfig> fieldMappings =
      ImmutableList.of(new HBaseFieldMappingConfig("cf:a", "[1]", StorageType.TEXT),
        new HBaseFieldMappingConfig("cf:b", "[2]", StorageType.TEXT),
        new HBaseFieldMappingConfig("cf:c", "[3]", StorageType.TEXT),
        new HBaseFieldMappingConfig("cf:d", "[4]", StorageType.TEXT));

    TargetRunner targetRunner = buildRunner(fieldMappings, StorageType.TEXT, OnRecordError.DISCARD, user, false, "[0]", false, false);

    Record record = RecordCreator.create();
    List<Field> fields = new ArrayList<>();
    String rowkey = "row_key";
    fields.add(Field.create(rowkey));
    fields.add(Field.create(20));
    fields.add(Field.create(30));
    fields.add(Field.create(40));
    fields.add(Field.create(50));
    record.set(Field.create(fields));
    assertTrue(targetRunner.runValidateConfigs().isEmpty());
    List<Record> singleRecord = ImmutableList.of(record);
    targetRunner.runInit();
    targetRunner.runWrite(singleRecord);
    targetRunner.runDestroy();
    HTable htable = new HTable(conf, tableName);
    Get g = new Get(Bytes.toBytes(rowkey));
    Result r = htable.get(g);
    assertEquals("20", Bytes.toString(r.getValue(Bytes.toBytes(familyName), Bytes.toBytes("a"))));
    assertEquals("30", Bytes.toString(r.getValue(Bytes.toBytes(familyName), Bytes.toBytes("b"))));
    assertEquals("40", Bytes.toString(r.getValue(Bytes.toBytes(familyName), Bytes.toBytes("c"))));
    assertEquals("50", Bytes.toString(r.getValue(Bytes.toBytes(familyName), Bytes.toBytes("d"))));
  }

  @Test
  public void testRegularUser() throws Exception {
    testUser("");
  }

  @Test
  public void testProxyUser() throws Exception {
    testUser("foo");
  }

  @Test(timeout=60000)
  public void testClusterModeHbaseConfDirAbsPath() throws Exception {

    File dir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(dir.mkdirs());

    TargetRunner targetRunner =
      new TargetRunner.Builder(HBaseDTarget.class)
        .addConfiguration("zookeeperQuorum", "127.0.0.1")
        .addConfiguration("clientPort", miniZK.getClientPort())
        .addConfiguration("zookeeperParentZnode", "/hbase")
        .addConfiguration("tableName", tableName)
        .addConfiguration("hbaseRowKey", "[0]")
        .addConfiguration("hbaseFieldColumnMapping",
          ImmutableList.of(new HBaseFieldMappingConfig("cf:a", "[1]", StorageType.TEXT)))
        .addConfiguration("kerberosAuth", false)
        .addConfiguration("hbaseConfigs", new HashMap<String, String>())
        .addConfiguration("hbaseUser", "")
        .addConfiguration("hbaseConfDir", dir.getAbsolutePath())
        .addConfiguration("rowKeyStorageType", StorageType.BINARY)
        .addConfiguration("implicitFieldMapping", false)
        .addConfiguration("ignoreMissingFieldPath", false)
        .addConfiguration("ignoreInvalidColumn", false)
        .addConfiguration("timeDriver", "${time:now()}")
      .setOnRecordError(OnRecordError.DISCARD)
      .setExecutionMode(ExecutionMode.CLUSTER_BATCH).build();

    try {
      targetRunner.runInit();
      Assert.fail(Utils.format("Expected StageException as absolute hbaseConfDir path '{}' is specified in cluster mode",
        dir.getAbsolutePath()));
    } catch (StageException e) {
      Assert.assertTrue(e.getMessage().contains("HBASE_24"));
    }
  }
}
