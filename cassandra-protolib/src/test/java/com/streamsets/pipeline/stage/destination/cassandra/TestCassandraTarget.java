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
package com.streamsets.pipeline.stage.destination.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.fail;

public class TestCassandraTarget {
  private static final Logger LOG = LoggerFactory.getLogger(TestCassandraTarget.class);

  private static final Double EPSILON = 1e-15;
  private static final long CASSANDRA_STARTUP_TIMEOUT = 20000;
  private static final String SAMPLE_TIMEUUID = "474b1386-0379-11e7-bdfe-fa245441bcee";
  private static final String SAMPLE_UUID = "46c5379c-a083-4ccd-bfac-c4a8d17574c7";
  private static int CASSANDRA_NATIVE_PORT = 9142;

  private static Cluster cluster = null;
  private static Session session = null;

  @SuppressWarnings("unchecked")
  @BeforeClass
  public static void setUpClass() throws InterruptedException, TTransportException, ConfigurationException, IOException {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(CASSANDRA_STARTUP_TIMEOUT);
    cluster = Cluster.builder()
        .addContactPoint("127.0.0.1")
        .withPort(CASSANDRA_NATIVE_PORT)
        .withProtocolVersion(ProtocolVersion.V4)
        .build();
    session = cluster.connect();
  }

  @AfterClass
  public static void tearDownClass() {
    session.close();
    cluster.close();
  }

  @Before
  public void setUp() {
    session.execute("CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");

    session.execute(
        "CREATE TABLE IF NOT EXISTS test.trips (" +
            "driver_id int," +
            "trip_id int," +
            "time int," +
            "x double," +
            "y double," +
            "dt date," +
            "ts timestamp," +
            "time_id timeuuid, " +
            "unique_id uuid, " +
            "PRIMARY KEY (driver_id, trip_id)" +
            ");"
    );

    session.execute(
        "CREATE TABLE IF NOT EXISTS test.collections (id int, a_list list<int>, a_map map<text, int>, PRIMARY KEY(id));"
    );

    session.execute(
        "CREATE TABLE IF NOT EXISTS test.test_null_values (a varchar, b varchar, PRIMARY KEY(a));"
    );
  }

  @After
  public void tearDown() {
    session.execute("DROP TABLE IF EXISTS test.test_null_values");
    session.execute("DROP TABLE IF EXISTS test.collections");
    session.execute("DROP TABLE IF EXISTS test.trips");
    session.execute("DROP KEYSPACE IF EXISTS test");
  }

  @Test
  public void testWriteEmptyBatch() throws InterruptedException, StageException {
    final String tableName = "test.trips";
    List<CassandraFieldMappingConfig> fieldMappings = ImmutableList.of(
        new CassandraFieldMappingConfig("[0]", "driver_id"),
        new CassandraFieldMappingConfig("[1]", "trip_id"),
        new CassandraFieldMappingConfig("[2]", "time"),
        new CassandraFieldMappingConfig("[3]", "x"),
        new CassandraFieldMappingConfig("[4]", "y"),
        new CassandraFieldMappingConfig("[5]", "time_id"),
        new CassandraFieldMappingConfig("[6]", "unique_id")
    );

    CassandraTargetConfig conf = new CassandraTargetConfig();
    conf.contactPoints.add("localhost");
    conf.port = CASSANDRA_NATIVE_PORT;
    conf.protocolVersion = ProtocolVersion.V4;
    conf.authProviderOption = AuthProviderOption.NONE;
    conf.compression = CassandraCompressionCodec.NONE;
    conf.columnNames = fieldMappings;
    conf.qualifiedTableName = tableName;

    Target target = new CassandraTarget(conf);
    TargetRunner targetRunner = new TargetRunner.Builder(CassandraDTarget.class, target).build();

    List<Record> emptyBatch = ImmutableList.of();
    targetRunner.runInit();
    targetRunner.runWrite(emptyBatch);
    targetRunner.runDestroy();
  }

  @Test
  public void testWriteSingleRecord() throws InterruptedException, StageException {
    final String tableName = "test.trips";
    List<CassandraFieldMappingConfig> fieldMappings = ImmutableList.of(
        new CassandraFieldMappingConfig("[0]", "driver_id"),
        new CassandraFieldMappingConfig("[1]", "trip_id"),
        new CassandraFieldMappingConfig("[2]", "time"),
        new CassandraFieldMappingConfig("[3]", "x"),
        new CassandraFieldMappingConfig("[4]", "y"),
        new CassandraFieldMappingConfig("[5]", "dt"),
        new CassandraFieldMappingConfig("[6]", "ts"),
        new CassandraFieldMappingConfig("[7]", "time_id"),
        new CassandraFieldMappingConfig("[8]", "unique_id")
    );

    CassandraTargetConfig conf = new CassandraTargetConfig();
    conf.contactPoints.add("localhost");
    conf.port = CASSANDRA_NATIVE_PORT;
    conf.protocolVersion = ProtocolVersion.V4;
    conf.authProviderOption = AuthProviderOption.NONE;
    conf.compression = CassandraCompressionCodec.NONE;
    conf.columnNames = fieldMappings;
    conf.qualifiedTableName = tableName;

    Target target = new CassandraTarget(conf);
    TargetRunner targetRunner = new TargetRunner.Builder(CassandraDTarget.class, target).build();

    long now = System.currentTimeMillis();
    LocalDate dt = LocalDate.fromMillisSinceEpoch(now);
    Date ts = new Date();

    Record record = RecordCreator.create();
    List<Field> fields = new ArrayList<>();
    fields.add(Field.create(1));
    fields.add(Field.create(2));
    fields.add(Field.create(3));
    fields.add(Field.create(4.0));
    fields.add(Field.create(5.0));
    fields.add(Field.create(Field.Type.DATE, new Date(dt.getMillisSinceEpoch())));
    fields.add(Field.create(Field.Type.DATETIME, ts));
    fields.add(Field.create(SAMPLE_TIMEUUID));
    fields.add(Field.create(SAMPLE_UUID));
    record.set(Field.create(fields));

    List<Record> singleRecord = ImmutableList.of(record);
    targetRunner.runInit();
    targetRunner.runWrite(singleRecord);

    // Should not be any error records.
    Assert.assertTrue(targetRunner.getErrorRecords().isEmpty());
    Assert.assertTrue(targetRunner.getErrors().isEmpty());

    targetRunner.runDestroy();

    ResultSet resultSet = session.execute("SELECT * FROM test.trips");
    List<Row> allRows = resultSet.all();
    Assert.assertEquals(1, allRows.size());

    Row row = allRows.get(0);
    Assert.assertEquals(1, row.getInt("driver_id"));
    Assert.assertEquals(2, row.getInt("trip_id"));
    Assert.assertEquals(3, row.getInt("time"));
    Assert.assertEquals(4.0, row.getDouble("x"), EPSILON);
    Assert.assertEquals(5.0, row.getDouble("y"), EPSILON);
    Assert.assertEquals(dt, row.getDate("dt"));
    Assert.assertEquals(ts, row.getTimestamp("ts"));
    Assert.assertEquals(SAMPLE_TIMEUUID, row.getUUID("time_id").toString());
    Assert.assertEquals(SAMPLE_UUID, row.getUUID("unique_id").toString());
  }

  @Test
  public void testCollectionTypes() throws InterruptedException, StageException {
    final String tableName = "test.collections";
    List<CassandraFieldMappingConfig> fieldMappings = ImmutableList.of(
        new CassandraFieldMappingConfig("[0]", "id"),
        new CassandraFieldMappingConfig("[1]", "a_list"),
        new CassandraFieldMappingConfig("[2]", "a_map")
    );

    CassandraTargetConfig conf = new CassandraTargetConfig();
    conf.contactPoints.add("localhost");
    conf.port = CASSANDRA_NATIVE_PORT;
    conf.protocolVersion = ProtocolVersion.V4;
    conf.authProviderOption = AuthProviderOption.NONE;
    conf.compression = CassandraCompressionCodec.NONE;
    conf.columnNames = fieldMappings;
    conf.qualifiedTableName = tableName;

    Target target = new CassandraTarget(conf);
    TargetRunner targetRunner = new TargetRunner.Builder(CassandraDTarget.class, target).build();

    Record record = RecordCreator.create();
    List<Field> fields = new ArrayList<>();
    fields.add(Field.create(1));
    fields.add(Field.create(ImmutableList.of(Field.create(2))));
    fields.add(Field.create(ImmutableMap.of("3", Field.create(4))));
    record.set(Field.create(fields));

    List<Record> singleRecord = ImmutableList.of(record);
    targetRunner.runInit();
    targetRunner.runWrite(singleRecord);

    // Should not be any error records.
    Assert.assertTrue(targetRunner.getErrorRecords().isEmpty());
    Assert.assertTrue(targetRunner.getErrors().isEmpty());

    targetRunner.runDestroy();

    ResultSet resultSet = session.execute("SELECT * FROM test.collections");
    List<Row> allRows = resultSet.all();
    assert (1 == allRows.size());

    Row row = allRows.get(0);
    assert (row.getInt("id") == 1);
    assert (row.getList("a_list", Integer.class).contains(2));
    assert (row.getMap("a_map", String.class, Integer.class).containsKey("3"));
  }

  @Test
  public void testWriteRecordsOnErrorDiscard() throws Exception {
    final String tableName = "test.trips";
    List<CassandraFieldMappingConfig> fieldMappings = ImmutableList.of(
        new CassandraFieldMappingConfig("[0]", "driver_id"),
        new CassandraFieldMappingConfig("[1]", "trip_id"),
        new CassandraFieldMappingConfig("[2]", "time"),
        new CassandraFieldMappingConfig("[3]", "x"),
        new CassandraFieldMappingConfig("[4]", "y"),
        new CassandraFieldMappingConfig("[5]", "time_id"),
        new CassandraFieldMappingConfig("[6]", "unique_id")
    );

    CassandraTargetConfig conf = new CassandraTargetConfig();
    conf.contactPoints.add("localhost");
    conf.port = CASSANDRA_NATIVE_PORT;
    conf.protocolVersion = ProtocolVersion.V4;
    conf.authProviderOption = AuthProviderOption.NONE;
    conf.compression = CassandraCompressionCodec.NONE;
    conf.columnNames = fieldMappings;
    conf.qualifiedTableName = tableName;

    Target target = new CassandraTarget(conf);
    TargetRunner targetRunner = new TargetRunner.Builder(CassandraDTarget.class, target)
        .setOnRecordError(OnRecordError.DISCARD)
        .build();

    Record record = RecordCreator.create();
    List<Field> fields = new ArrayList<>();
    fields.add(Field.create(1.3));
    fields.add(Field.create(2));
    fields.add(Field.create(3));
    fields.add(Field.create(4.0));
    fields.add(Field.create(5.0));
    fields.add(Field.create(SAMPLE_TIMEUUID));
    fields.add(Field.create(SAMPLE_UUID));
    record.set(Field.create(fields));

    List<Record> singleRecord = ImmutableList.of(record);
    targetRunner.runInit();
    targetRunner.runWrite(singleRecord);

    // Should not be any error records if we are discarding.
    Assert.assertTrue(targetRunner.getErrorRecords().isEmpty());
    Assert.assertTrue(targetRunner.getErrors().isEmpty());

    targetRunner.runDestroy();

    ResultSet resultSet = session.execute("SELECT * FROM test.trips");
    List<Row> allRows = resultSet.all();
    Assert.assertEquals(0, allRows.size());
  }

  @Test
  public void testWriteRecordsOnErrorToError() throws Exception {
    final String tableName = "test.trips";
    List<CassandraFieldMappingConfig> fieldMappings = ImmutableList.of(
        new CassandraFieldMappingConfig("[0]", "driver_id"),
        new CassandraFieldMappingConfig("[1]", "trip_id"),
        new CassandraFieldMappingConfig("[2]", "time"),
        new CassandraFieldMappingConfig("[3]", "x"),
        new CassandraFieldMappingConfig("[4]", "y"),
        new CassandraFieldMappingConfig("[5]", "time_id"),
        new CassandraFieldMappingConfig("[6]", "unique_id")
    );

    CassandraTargetConfig conf = new CassandraTargetConfig();
    conf.contactPoints.add("localhost");
    conf.port = CASSANDRA_NATIVE_PORT;
    conf.protocolVersion = ProtocolVersion.V4;
    conf.authProviderOption = AuthProviderOption.NONE;
    conf.compression = CassandraCompressionCodec.NONE;
    conf.columnNames = fieldMappings;
    conf.qualifiedTableName = tableName;

    Target target = new CassandraTarget(conf);
    TargetRunner targetRunner = new TargetRunner.Builder(CassandraDTarget.class, target)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();

    Record record = RecordCreator.create();
    List<Field> fields = new ArrayList<>();
    fields.add(Field.create(1.3));
    fields.add(Field.create(2));
    fields.add(Field.create(3));
    // intentionally passing doubles where these values are expected to be ints triggers errors!
    fields.add(Field.create(4.0));
    fields.add(Field.create(5.0));
    fields.add(Field.create(SAMPLE_TIMEUUID));
    fields.add(Field.create(SAMPLE_UUID));
    record.set(Field.create(fields));

    List<Record> singleRecord = ImmutableList.of(record);
    targetRunner.runInit();
    targetRunner.runWrite(singleRecord);

    // Should have gone to error pipeline
    Assert.assertEquals(1, targetRunner.getErrorRecords().size());
    Assert.assertTrue(targetRunner.getErrors().isEmpty());

    targetRunner.runDestroy();

    ResultSet resultSet = session.execute("SELECT * FROM test.trips");
    List<Row> allRows = resultSet.all();
    Assert.assertEquals(0, allRows.size());
  }

  @Test(expected = StageException.class)
  public void testWriteRecordsOnErrorStopPipeline() throws Exception {
    final String tableName = "test.trips";
    List<CassandraFieldMappingConfig> fieldMappings = ImmutableList.of(
        new CassandraFieldMappingConfig("[0]", "driver_id"),
        new CassandraFieldMappingConfig("[1]", "trip_id"),
        new CassandraFieldMappingConfig("[2]", "time"),
        new CassandraFieldMappingConfig("[3]", "x"),
        new CassandraFieldMappingConfig("[4]", "y"),
        new CassandraFieldMappingConfig("[5]", "time_id"),
        new CassandraFieldMappingConfig("[6]", "unique_id")
    );

    CassandraTargetConfig conf = new CassandraTargetConfig();
    conf.contactPoints.add("localhost");
    conf.port = CASSANDRA_NATIVE_PORT;
    conf.protocolVersion = ProtocolVersion.V4;
    conf.authProviderOption = AuthProviderOption.NONE;
    conf.compression = CassandraCompressionCodec.NONE;
    conf.columnNames = fieldMappings;
    conf.qualifiedTableName = tableName;

    Target target = new CassandraTarget(conf);
    TargetRunner targetRunner = new TargetRunner.Builder(CassandraDTarget.class, target)
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .build();

    Record record = RecordCreator.create();
    List<Field> fields = new ArrayList<>();
    fields.add(Field.create(1.3));
    fields.add(Field.create(2));
    fields.add(Field.create(3));
    fields.add(Field.create(4.0));
    fields.add(Field.create(5.0));
    fields.add(Field.create(SAMPLE_TIMEUUID));
    fields.add(Field.create(SAMPLE_UUID));
    record.set(Field.create(fields));

    List<Record> singleRecord = ImmutableList.of(record);
    targetRunner.runInit();
    targetRunner.runWrite(singleRecord);

    // Should have gone to error pipeline
    Assert.assertEquals(1, targetRunner.getErrorRecords().size());
    Assert.assertTrue(targetRunner.getErrors().isEmpty());

    targetRunner.runDestroy();

    ResultSet resultSet = session.execute("SELECT * FROM test.trips");
    List<Row> allRows = resultSet.all();
    Assert.assertEquals(0, allRows.size());
  }

  @Test
  public void testWriteRecordWithMissingFields() throws InterruptedException, StageException {
    final String tableName = "test.trips";
    List<CassandraFieldMappingConfig> fieldMappings = ImmutableList.of(
        new CassandraFieldMappingConfig("/driver", "driver_id"),
        new CassandraFieldMappingConfig("/trip", "trip_id"),
        new CassandraFieldMappingConfig("/time", "time"),
        new CassandraFieldMappingConfig("/x", "x"),
        new CassandraFieldMappingConfig("/y", "y"),
        new CassandraFieldMappingConfig("/time_id", "time_id"),
        new CassandraFieldMappingConfig("/unique_id", "unique_id")
    );

    CassandraTargetConfig conf = new CassandraTargetConfig();
    conf.contactPoints.add("localhost");
    conf.port = CASSANDRA_NATIVE_PORT;
    conf.protocolVersion = ProtocolVersion.V4;
    conf.authProviderOption = AuthProviderOption.NONE;
    conf.compression = CassandraCompressionCodec.NONE;
    conf.columnNames = fieldMappings;
    conf.qualifiedTableName = tableName;

    Target target = new CassandraTarget(conf);
    TargetRunner targetRunner = new TargetRunner.Builder(CassandraDTarget.class, target).build();

    Record record = RecordCreator.create();
    Map<String, Field> fields = new ImmutableMap.Builder<String, Field>()
        .put("driver", Field.create(1))
        .put("trip", Field.create(2))
        .put("time", Field.create(3))
        .put("y", Field.create(5.0))
        .put("time_id", Field.create(SAMPLE_TIMEUUID))
        .put("unique_id", Field.create(SAMPLE_UUID))
        .build();
    record.set(Field.create(fields));

    List<Record> singleRecord = ImmutableList.of(record);
    targetRunner.runInit();
    targetRunner.runWrite(singleRecord);

    // Should not be any error records.
    Assert.assertTrue(targetRunner.getErrorRecords().isEmpty());
    Assert.assertTrue(targetRunner.getErrors().isEmpty());

    targetRunner.runDestroy();

    ResultSet resultSet = session.execute("SELECT * FROM test.trips");
    List<Row> allRows = resultSet.all();
    Assert.assertEquals(1, allRows.size());

    Row row = allRows.get(0);
    Assert.assertEquals(1, row.getInt("driver_id"));
    Assert.assertEquals(2, row.getInt("trip_id"));
    Assert.assertEquals(3, row.getInt("time"));
    Assert.assertEquals(null, row.getBytesUnsafe("x"));
    Assert.assertEquals(5.0, row.getDouble("y"), EPSILON);
    Assert.assertEquals(SAMPLE_TIMEUUID, row.getUUID("time_id").toString());
    Assert.assertEquals(SAMPLE_UUID, row.getUUID("unique_id").toString());
  }

  @Test(expected = StageException.class)
  public void testMalformedTableName() throws Exception {
    List<CassandraFieldMappingConfig> fieldMappings = ImmutableList.of(
        new CassandraFieldMappingConfig("/driver", "driver_id"),
        new CassandraFieldMappingConfig("/trip", "trip_id"),
        new CassandraFieldMappingConfig("/time", "time"),
        new CassandraFieldMappingConfig("/x", "x"),
        new CassandraFieldMappingConfig("/y", "y"),
        new CassandraFieldMappingConfig("/time_id", "time_id"),
        new CassandraFieldMappingConfig("/unique_id", "unique_id")
    );

    CassandraTargetConfig conf = new CassandraTargetConfig();
    conf.contactPoints.add("localhost");
    conf.port = CASSANDRA_NATIVE_PORT;
    conf.protocolVersion = ProtocolVersion.V4;
    conf.authProviderOption = AuthProviderOption.NONE;
    conf.compression = CassandraCompressionCodec.NONE;
    conf.columnNames = fieldMappings;
    conf.qualifiedTableName = "tableName";

    Target target = new CassandraTarget(conf);
    TargetRunner targetRunner = new TargetRunner.Builder(CassandraDTarget.class, target).build();

    targetRunner.runInit();
    fail("should have thrown a StageException!");
  }

  @Test
  public void testInternalSubBatching() throws Exception {
    final String tableName = "test.trips";
    List<CassandraFieldMappingConfig> fieldMappings = ImmutableList.of(
        new CassandraFieldMappingConfig("[0]", "driver_id"),
        new CassandraFieldMappingConfig("[1]", "trip_id"),
        new CassandraFieldMappingConfig("[2]", "time"),
        new CassandraFieldMappingConfig("[3]", "x"),
        new CassandraFieldMappingConfig("[4]", "y"),
        new CassandraFieldMappingConfig("[5]", "time_id"),
        new CassandraFieldMappingConfig("[6]", "unique_id")
    );

    CassandraTargetConfig conf = new CassandraTargetConfig();
    conf.contactPoints.add("localhost");
    conf.port = CASSANDRA_NATIVE_PORT;
    conf.protocolVersion = ProtocolVersion.V4;
    conf.authProviderOption = AuthProviderOption.NONE;
    conf.compression = CassandraCompressionCodec.NONE;
    conf.columnNames = fieldMappings;
    conf.qualifiedTableName = tableName;
    conf.maxBatchSize = 35;

    Target target = new CassandraTarget(conf);
    TargetRunner targetRunner = new TargetRunner.Builder(CassandraDTarget.class, target).build();

    List<Record> records = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      Record record = RecordCreator.create();
      List<Field> fields = new ArrayList<>();
      fields.add(Field.create(i));
      fields.add(Field.create(2));
      fields.add(Field.create(3));
      fields.add(Field.create(4.0));
      fields.add(Field.create(5.0));
      fields.add(Field.create(SAMPLE_TIMEUUID));
      fields.add(Field.create(SAMPLE_UUID));
      record.set(Field.create(fields));
      records.add(record);
    }
    targetRunner.runInit();
    targetRunner.runWrite(records);

    // Should not be any error records.
    Assert.assertTrue(targetRunner.getErrorRecords().isEmpty());
    Assert.assertTrue(targetRunner.getErrors().isEmpty());

    targetRunner.runDestroy();

    // simple verification that there are as many records as expected
    ResultSet resultSet = session.execute("SELECT * FROM test.trips");
    List<Row> allRows = resultSet.all();
    Assert.assertEquals(1000, allRows.size());
  }

  @Test
  public void testWriteNullValuedColumns() throws Exception {
    final String tableName = "test.test_null_values";
    List<CassandraFieldMappingConfig> fieldMappings = ImmutableList.of(new CassandraFieldMappingConfig("[0]", "a"),
        new CassandraFieldMappingConfig("[1]", "b")
    );

    CassandraTargetConfig conf = new CassandraTargetConfig();
    conf.contactPoints.add("localhost");
    conf.port = CASSANDRA_NATIVE_PORT;
    conf.protocolVersion = ProtocolVersion.V4;
    conf.authProviderOption = AuthProviderOption.NONE;
    conf.compression = CassandraCompressionCodec.NONE;
    conf.columnNames = fieldMappings;
    conf.qualifiedTableName = tableName;

    Target target = new CassandraTarget(conf);
    TargetRunner targetRunner = new TargetRunner.Builder(CassandraDTarget.class, target).build();

    Record record = RecordCreator.create();
    List<Field> fields = new ArrayList<>();
    fields.add(Field.create("abc"));
    fields.add(Field.create(Field.Type.STRING, null));
    record.set(Field.create(fields));

    List<Record> singleRecord = ImmutableList.of(record);
    targetRunner.runInit();
    targetRunner.runWrite(singleRecord);

    // Should not be any error records.
    Assert.assertTrue(targetRunner.getErrorRecords().isEmpty());
    Assert.assertTrue(targetRunner.getErrors().isEmpty());

    targetRunner.runDestroy();

    ResultSet resultSet = session.execute("SELECT * FROM test.test_null_values");
    List<Row> allRows = resultSet.all();
    Assert.assertEquals(1, allRows.size());

    Row row = allRows.get(0);
    Assert.assertEquals("abc", row.getString("a"));
    Assert.assertEquals(null, row.getString("b"));
  }
}
