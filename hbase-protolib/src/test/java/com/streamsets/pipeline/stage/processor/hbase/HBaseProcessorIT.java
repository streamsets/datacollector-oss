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
package com.streamsets.pipeline.stage.processor.hbase;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.hbase.common.Errors;
import com.streamsets.pipeline.lib.hbase.common.HBaseColumn;
import com.streamsets.pipeline.lib.hbase.common.HBaseConnectionConfig;
import com.streamsets.pipeline.lib.hbase.common.HBaseUtil;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.stage.common.hbase.HBaseTestUtil;
import com.streamsets.pipeline.stage.processor.kv.EvictionPolicyType;
import com.streamsets.pipeline.stage.processor.kv.LookupMode;
import com.streamsets.testing.SingleForkNoReuseTest;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(SingleForkNoReuseTest.class)
@RunWith(Parameterized.class)
public class HBaseProcessorIT {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseProcessorIT.class);
  private static HBaseTestingUtility utility;
  private static MiniZooKeeperCluster miniZK;
  private static final String tableName = "TestHBaseProcessor";
  private static final String familyName = "cf";
  private static final Configuration conf = HBaseTestUtil.getHBaseTestConfiguration();
  private static Processor.Context context;

  @Parameterized.Parameters
  public static Iterable<Object[]> data() {
    return Arrays.asList(
        new Object[][]{
            {LookupMode.RECORD, false}, {LookupMode.RECORD, true}, {LookupMode.BATCH, false}, {LookupMode.BATCH, true}
        }
    );
  }

  @Parameterized.Parameter(0)
  public LookupMode mode;

  @Parameterized.Parameter(1)
  public Boolean cacheEnable;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    try {
      context = ContextInfoCreator.createProcessorContext("n", false, OnRecordError.TO_ERROR);
      UserGroupInformation.createUserForTesting("foo", new String[]{"all"});
      utility = new HBaseTestingUtility(conf);
      utility.startMiniCluster();
      miniZK = utility.getZkCluster();
      HTable ht = utility.createTable(Bytes.toBytes(tableName), Bytes.toBytes(familyName));

      // setup data
      List<Put> puts = new ArrayList<>();
      Put put = new Put(Bytes.toBytes("row1"));
      put.add(Bytes.toBytes(familyName), Bytes.toBytes("column1"),
        Bytes.toBytes("value1"));
      puts.add(put);

      put = new Put(Bytes.toBytes("row1"));
      put.add(Bytes.toBytes(familyName), Bytes.toBytes("column2"),
        Bytes.toBytes("value2"));
      puts.add(put);

      put = new Put(Bytes.toBytes("row2"));
      put.add(Bytes.toBytes(familyName), Bytes.toBytes("column2"),
        Bytes.toBytes("value2"));
      puts.add(put);

      put = new Put(Bytes.toBytes("row3"));
      put.add(Bytes.toBytes(familyName), Bytes.toBytes("column3"),
        Bytes.toBytes("value3"));
      puts.add(put);

      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
      Date date = sdf.parse("1986-09-21");
      put = new Put(Bytes.toBytes("rowTimestamp"), date.getTime());
      put.add(Bytes.toBytes(familyName), Bytes.toBytes("columnTimestamp"), Bytes.toBytes("valueTimestamp"));

      date = sdf.parse("2000-10-10") ;
      puts.add(put);
      put = new Put(Bytes.toBytes("rowTimestamp"), date.getTime());
      put.add(Bytes.toBytes(familyName), Bytes.toBytes("columnTimestamp"), Bytes.toBytes("valueTimestamp"));

      ht.put(puts);
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

  @Test(timeout = 60000)
  public void testEmptyBatch() throws Exception {
    Processor processor = new HBaseLookupProcessor(getDefaultConfig());

    List<Record> records = new ArrayList<>();

    ProcessorRunner runner = new ProcessorRunner.Builder(HBaseLookupDProcessor.class, processor)
      .addOutputLane("lane")
      .build();
    runner.runInit();
    runner.runProcess(records);

    assertTrue(runner.getErrorRecords().isEmpty());
  }

  @Test(timeout = 60000)
  public void testInvalidURI() throws Exception {
    HBaseLookupConfig config = getDefaultConfig();
    config.hBaseConnectionConfig.zookeeperQuorum = "";

    Processor processor = new HBaseLookupProcessor(config);

    ProcessorRunner runner = new ProcessorRunner.Builder(HBaseLookupDProcessor.class, processor)
      .addOutputLane("lane")
      .build();
    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertFalse(issues.isEmpty());
  }

  @Test(timeout = 60000)
  public void testInvalidTableName() throws Exception {
    HBaseLookupConfig config = getDefaultConfig();
    config.hBaseConnectionConfig.tableName = "randomTable";

    Processor processor = new HBaseLookupProcessor(config);
    ProcessorRunner runner = new ProcessorRunner.Builder(HBaseLookupDProcessor.class, processor)
      .addOutputLane("lane")
      .build();
    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertFalse(issues.isEmpty());
  }

  @Test(timeout = 60000)
  public void testEmptyKeyExpression() throws Exception {
    HBaseLookupConfig config = getDefaultConfig();
    config.lookups.get(0).rowExpr = "";
    Processor processor = new HBaseLookupProcessor(config);

    ProcessorRunner runner = new ProcessorRunner.Builder(HBaseLookupDProcessor.class, processor)
        .addOutputLane("lane")
        .setOnRecordError(OnRecordError.DISCARD)
        .build();

    List<Stage.ConfigIssue> configIssues = runner.runValidateConfigs();
    assertEquals(1, configIssues.size());
  }

  @Test(timeout = 60000)
  public void testGetEmptyKeyToDiscard() throws Exception {
    HBaseLookupConfig config = getDefaultConfig();
    config.cache.enabled = false;

    Processor processor = new HBaseLookupProcessor(config);

    List<Record> records = new ArrayList<>();
    Record record = RecordCreator.create();

    Map<String, Field> fields = new HashMap<>();
    fields.put("columnField", Field.create("cf1:" + "column"));
    record.set(Field.create(fields));
    records.add(record);

    ProcessorRunner runner = new ProcessorRunner.Builder(HBaseLookupDProcessor.class, processor)
        .addOutputLane("lane")
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();
    runner.runProcess(records);

    assertTrue(runner.getErrorRecords().isEmpty());
  }

  @Test(timeout = 60000)
  public void testGetEmptyKeyToError() throws Exception {
    HBaseLookupConfig config = getDefaultConfig();
    config.cache.enabled = false;
    config.ignoreMissingFieldPath = false;

    Processor processor = new HBaseLookupProcessor(config);

    List<Record> records = new ArrayList<>();
    Record record = RecordCreator.create();

    Map<String, Field> fields = new HashMap<>();
    fields.put("columnField", Field.create("cf1:" + "column"));
    record.set(Field.create(fields));
    records.add(record);

    ProcessorRunner runner = new ProcessorRunner.Builder(HBaseLookupDProcessor.class, processor)
        .addOutputLane("lane")
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();
    runner.runProcess(records);

    assertFalse(runner.getErrorRecords().isEmpty());
  }

  @Test(timeout = 60000)
  public void testGetKey() throws Exception {
    final Optional<String> expected = Optional.of("value1");
    HBaseLookupConfig config = getDefaultConfig();
    config.cache.enabled = false;
    Configuration hbaseConfig = getHBaseConfiguration(config);

    HBaseStore hbaseStore = new HBaseStore(config, hbaseConfig);
    Optional<String> value = hbaseStore.get(Pair.of("row1", new HBaseColumn(Bytes.toBytes(familyName), Bytes.toBytes("column1"))));
    assertTrue(value.isPresent());
    assertEquals(expected, value);
  }

  @Test(timeout = 60000)
  public void testGetMultipleKeys() throws Exception {
    final List<Optional<String>> expected = ImmutableList.of(
      Optional.of("value1"),
      Optional.of("value2"),
      Optional.of("value3"),
      Optional.<String>absent()
    );

    HBaseLookupConfig config = getDefaultConfig();
    config.cache.enabled = false;
    context.isPreview();

    Configuration hbaseConfig = getHBaseConfiguration(config);

    HBaseStore hbaseStore = new HBaseStore(config, hbaseConfig);
    List<Pair<String, HBaseColumn>> params = new ArrayList<>();
    params.add(Pair.of("row1", new HBaseColumn(Bytes.toBytes(familyName), Bytes.toBytes("column1"))));
    params.add(Pair.of("row2", new HBaseColumn(Bytes.toBytes(familyName), Bytes.toBytes("column2"))));
    params.add(Pair.of("row3", new HBaseColumn(Bytes.toBytes(familyName), Bytes.toBytes("column3"))));
    params.add(Pair.of("row4", new HBaseColumn(Bytes.toBytes(familyName), Bytes.toBytes("column4"))));

    List<Optional<String>> value = hbaseStore.get(params);
    assertTrue(value.size() > 0);
    assertArrayEquals(expected.toArray(), value.toArray());
  }

  @Test(timeout = 60000)
  public void testGetKeyWithTimeStamp() throws Exception {
    final Optional<String> expected = Optional.of("valueTimestamp");
    HBaseLookupConfig config = getDefaultConfig();
    config.cache.enabled = false;
    context.isPreview();

    Configuration hbaseConfig = getHBaseConfiguration(config);

    HBaseStore hbaseStore = new HBaseStore(config, hbaseConfig);
    HBaseColumn key = new HBaseColumn(Bytes.toBytes(familyName), Bytes.toBytes("columnTimestamp"));
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    key.setTimestamp(sdf.parse("1986-09-21").getTime());
    Optional<String> value = hbaseStore.get(Pair.of("rowTimestamp", key));
    assertEquals(expected, value);
  }

  @Test(timeout = 60000)
  public void testGetKeyWithInvalidTimeStamp() throws Exception {
    final Optional<String> expected = Optional.absent();
    HBaseLookupConfig config = getDefaultConfig();
    config.cache.enabled = false;
    context.isPreview();

    Configuration hbaseConfig = getHBaseConfiguration(config);

    HBaseStore hbaseStore = new HBaseStore(config, hbaseConfig);
    HBaseColumn key = new HBaseColumn(Bytes.toBytes(familyName), Bytes.toBytes("columnTimestamp"));
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    key.setTimestamp(sdf.parse("2019-09-21").getTime());
    Optional<String> value = hbaseStore.get(Pair.of("rowTimestamp", key));
    assertEquals(expected, value);
  }

  @Test(timeout = 60000)
  public void testInvalidColumnFamily() throws Exception {
    HBaseLookupConfig config = getDefaultConfig();
    config.cache.enabled = false;
    context.isPreview();

    Processor processor = new HBaseLookupProcessor(config);

    List<Record> records = new ArrayList<>();
    Record record = RecordCreator.create();

    Map<String, Field> fields = new HashMap<>();
    fields.put("keyField", Field.create("key"));
    fields.put("columnField", Field.create("WrongColumnFamily:" + "column"));
    record.set(Field.create(fields));
    records.add(record);

    ProcessorRunner runner = new ProcessorRunner.Builder(HBaseLookupDProcessor.class, processor)
      .addOutputLane("lane")
      .setOnRecordError(OnRecordError.TO_ERROR)
      .build();
    runner.runInit();
    runner.runProcess(records);

    assertFalse(runner.getErrorRecords().isEmpty());
    assertEquals(Errors.HBASE_37.getCode(), runner.getErrorRecords().get(0).getHeader().getErrorCode());
  }

  @Test(timeout = 60000)
  public void testGetRow() throws Exception {
    Map<String, Field> expectedMap = new HashMap<>();
    expectedMap.put("cf:column1", Field.create("value1"));
    expectedMap.put("cf:column2", Field.create("value2"));

    HBaseLookupConfig config = getDefaultConfig();
    config.lookups.get(0).columnExpr = "";
    Processor processor = new HBaseLookupProcessor(config);

    List<Record> records = new ArrayList<>();
    Record record = RecordCreator.create();

    Map<String, Field> fields = new HashMap<>();
    fields.put("keyField", Field.create("row1"));
    record.set(Field.create(fields));
    records.add(record);

    ProcessorRunner runner = new ProcessorRunner.Builder(HBaseLookupDProcessor.class, processor)
      .addOutputLane("lane")
      .setOnRecordError(OnRecordError.TO_ERROR)
      .build();
    runner.runInit();
    runner.runProcess(records);

    assertTrue(runner.getErrorRecords().isEmpty());

    Map<String, Field> value = (Map<String, Field>)record.get(config.lookups.get(0).outputFieldPath).getValue();
    assertEquals(expectedMap.toString(), value.toString());
  }

  private HBaseLookupConfig getDefaultConfig() {
    HBaseLookupConfig config = new HBaseLookupConfig();
    config.hBaseConnectionConfig = new HBaseConnectionConfig();
    config.hBaseConnectionConfig.zookeeperQuorum = "127.0.0.1";
    config.hBaseConnectionConfig.clientPort = miniZK.getClientPort();
    config.hBaseConnectionConfig.zookeeperParentZNode = "/hbase";
    config.hBaseConnectionConfig.tableName = tableName;
    config.hBaseConnectionConfig.hbaseUser = "";
    config.hBaseConnectionConfig.hbaseConfigs = new HashMap<>();
    config.mode = mode;
    config.cache.enabled = cacheEnable;
    config.cache.evictionPolicyType = EvictionPolicyType.EXPIRE_AFTER_ACCESS;
    config.cache.timeUnit = TimeUnit.MINUTES;
    config.lookups = new ArrayList<>();

    config.lookups = new ArrayList<>();
    HBaseLookupParameterConfig parameter = new HBaseLookupParameterConfig();
    parameter.rowExpr = "${record:value('/keyField')}";
    parameter.columnExpr = "${record:value('/columnField')}";
    parameter.outputFieldPath = "/output";
    config.lookups.add(parameter);

    return config;
  }

  private Configuration getHBaseConfiguration(HBaseLookupConfig config) {
    Configuration hbaseConfig = HBaseUtil.getHBaseConfiguration(
      new ArrayList<Stage.ConfigIssue>(),
      context,
      Groups.HBASE.getLabel(),
      config.hBaseConnectionConfig.hbaseConfDir, config.hBaseConnectionConfig.tableName, config.hBaseConnectionConfig.hbaseConfigs
    );

    HBaseUtil.setIfNotNull(hbaseConfig, HConstants.ZOOKEEPER_QUORUM, config.hBaseConnectionConfig.zookeeperQuorum);
    hbaseConfig.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, config.hBaseConnectionConfig.clientPort);
    HBaseUtil.setIfNotNull(hbaseConfig, HConstants.ZOOKEEPER_ZNODE_PARENT, config.hBaseConnectionConfig.zookeeperParentZNode);

    return hbaseConfig;
  }
}
