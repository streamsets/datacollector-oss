/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not be copied, modified, or distributed in
 * whole or part without written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
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
      htd.addFamily(new HColumnDescriptor(familyName));
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
    HBaseTarget target = (HBaseTarget) dTarget.createTarget();
    List<Stage.ConfigIssue> issues =
        target.init(null,
                    ContextInfoCreator.createTargetContext("n", false, OnRecordError.TO_ERROR));
    Assert.assertEquals(3, issues.size());
    assertTrue(issues.get(0).toString().contains("HBASE_04"));
    assertTrue(issues.get(1).toString().contains("HBASE_09"));
    assertTrue(issues.get(2).toString().contains("HBASE_13"));

    configure(dTarget);
    dTarget.tableName = "NonExistingTable";
    target = (HBaseTarget) dTarget.createTarget();
    issues =
        target.init(null,
                    ContextInfoCreator.createTargetContext("n", false, OnRecordError.TO_ERROR));
    Assert.assertEquals(1, issues.size());
    assertTrue(issues.get(0).toString().contains("HBASE_07"));

    configure(dTarget);
    dTarget.zookeeperQuorum = "dummyhost";
    target = (HBaseTarget) dTarget.createTarget();
    issues =
        target.init(null,
                    ContextInfoCreator.createTargetContext("n", false, OnRecordError.TO_ERROR));
    Assert.assertEquals(1, issues.size());
    assertTrue(issues.get(0).toString().contains("HBASE_06"));
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

    TargetRunner targetRunner = buildRunner(fieldMappings, StorageType.TEXT, OnRecordError.DISCARD, "");

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

  @Test(timeout=60000)
  public void testSingleRecordBinaryStorage() throws InterruptedException, StageException,
      IOException {
    List<HBaseFieldMappingConfig> fieldMappings =
        ImmutableList.of(new HBaseFieldMappingConfig("cf:a", "[1]", StorageType.BINARY),
          new HBaseFieldMappingConfig("cf:b", "[2]", StorageType.BINARY),
          new HBaseFieldMappingConfig("cf:c", "[3]", StorageType.TEXT),
          new HBaseFieldMappingConfig("cf:d", "[4]", StorageType.TEXT));

    TargetRunner targetRunner =
        buildRunner(fieldMappings, StorageType.BINARY, OnRecordError.DISCARD, "");

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
        buildRunner(fieldMappings, StorageType.BINARY, OnRecordError.DISCARD, "");

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
        buildRunner(fieldMappings, StorageType.BINARY, OnRecordError.DISCARD, "");

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
        buildRunner(fieldMappings, StorageType.BINARY, OnRecordError.DISCARD, "");

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
        buildRunner(fieldMappings, StorageType.BINARY, OnRecordError.TO_ERROR, "");

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
        buildRunner(fieldMappings, StorageType.BINARY, OnRecordError.STOP_PIPELINE, "");

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
        buildRunner(fieldMappings, StorageType.BINARY, OnRecordError.TO_ERROR, "");

    Record record = RecordCreator.create();
    List<Field> fields = new ArrayList<>();
    int rowKey = 3333;
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
    int rowKey2 = 4444;
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
  public void testWriteRecordsWrongColumn() throws InterruptedException, StageException,
      IOException {
    List<HBaseFieldMappingConfig> fieldMappings =
        ImmutableList.of(new HBaseFieldMappingConfig("invalid_cf:a", "[1]", StorageType.BINARY),
          new HBaseFieldMappingConfig("cf:b", "[2]", StorageType.BINARY),
          new HBaseFieldMappingConfig("cf:c", "[3]", StorageType.TEXT),
          new HBaseFieldMappingConfig("cf:d", "[4]", StorageType.TEXT));

    TargetRunner targetRunner =
        buildRunner(fieldMappings, StorageType.TEXT, OnRecordError.TO_ERROR, "");

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
    try {
      targetRunner.runWrite(singleRecord);
      fail("Expected StageException but didn't get any");
    } catch (StageException e) {
      assertEquals(Errors.HBASE_26, e.getErrorCode());
    } catch (Exception e) {

    }
  }

  static class ForTestHBaseTarget extends HBaseDTarget {
    @Override
    protected Target createTarget() {
      return new HBaseTarget(zookeeperQuorum, clientPort, zookeeperParentZnode, tableName, hbaseRowKey,
        rowKeyStorageType, hbaseFieldColumnMapping, kerberosAuth, hbaseConfDir, hbaseConfigs, hbaseUser) {
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
  }

  private TargetRunner buildRunner(List<HBaseFieldMappingConfig> fieldMappings,
      StorageType storageType, OnRecordError onRecordError, String hbaseUser) {
    TargetRunner targetRunner =
        new TargetRunner.Builder(HBaseDTarget.class)
            .addConfiguration("zookeeperQuorum", "127.0.0.1")
            .addConfiguration("clientPort", miniZK.getClientPort())
            .addConfiguration("zookeeperParentZnode", "/hbase")
            .addConfiguration("tableName", tableName).addConfiguration("hbaseRowKey", "[0]")
            .addConfiguration("hbaseFieldColumnMapping", fieldMappings)
            .addConfiguration("kerberosAuth", false)
            .addConfiguration("hbaseConfDir", "")
            .addConfiguration("hbaseConfigs", new HashMap<String, String>())
            .addConfiguration("rowKeyStorageType", storageType).setOnRecordError(onRecordError)
            .addConfiguration("hbaseUser", hbaseUser)
            .build();
    return targetRunner;
  }

  private void testUser(String user) throws Exception {
    List<HBaseFieldMappingConfig> fieldMappings =
      ImmutableList.of(new HBaseFieldMappingConfig("cf:a", "[1]", StorageType.TEXT),
        new HBaseFieldMappingConfig("cf:b", "[2]", StorageType.TEXT),
        new HBaseFieldMappingConfig("cf:c", "[3]", StorageType.TEXT),
        new HBaseFieldMappingConfig("cf:d", "[4]", StorageType.TEXT));

    TargetRunner targetRunner = buildRunner(fieldMappings, StorageType.TEXT, OnRecordError.DISCARD, user);

    Record record = RecordCreator.create();
    List<Field> fields = new ArrayList<>();
    String rowKey = "row_key";
    fields.add(Field.create(rowKey));
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
    Get g = new Get(Bytes.toBytes(rowKey));
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
      .setOnRecordError(OnRecordError.DISCARD)
      .setClusterMode(true).build();

    try {
      targetRunner.runInit();
      Assert.fail(Utils.format("Expected StageException as absolute hbaseConfDir path '{}' is specified in cluster mode",
        dir.getAbsolutePath()));
    } catch (StageException e) {
      Assert.assertTrue(e.getMessage().contains("HBASE_24"));
    }
  }
}
