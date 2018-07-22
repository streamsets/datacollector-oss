/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.lib.jdbc;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.SchemaAndTable;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.SchemaTableConfigBean;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.postgres.DecoderValues;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.postgres.PgVersionValues;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.postgres.PostgresCDCConfigBean;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.postgres.PostgresCDCSource;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.postgres.PostgresCDCWalReceiver;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.postgres.PostgresChangeTypeValues;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.postgres.PostgresWalRecord;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.postgres.PostgresWalRunner;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.postgres.StartValues;
import com.zaxxer.hikari.HikariDataSource;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.zone.ZoneRules;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.postgresql.replication.LogSequenceNumber;

public class TestJdbcPostgresCDCWalRunner {

  private PostgresCDCConfigBean configBean;
  private HikariPoolConfigBean hikariConfigBean;
  private String h2ConnectionString = "jdbc:postgresql://localhost:5432/sdctest";
  private String username = "postgres";
  private String password = "postgres";

  private PostgresCDCSource pgSourceMock;
  private PostgresCDCWalReceiver walReceiverMock;
  private PostgresWalRecord walRecordMock;
  private List<SchemaAndTable> schemasAndTables;
  private Field baseRecordField;

  private void createConfigBeans() {
    hikariConfigBean = new HikariPoolConfigBean();
    hikariConfigBean.connectionString = h2ConnectionString;
    hikariConfigBean.useCredentials = true;
    hikariConfigBean.username = () -> username;
    hikariConfigBean.password = () -> password;

    configBean = new PostgresCDCConfigBean();
    configBean.slot = "slot";
    configBean.minVersion = PgVersionValues.NINEFOUR;
    configBean.queryTimeout = 20;
    configBean.replicationType = "database";
    configBean.pollInterval = 1000;

    configBean.postgresChangeTypes = new ArrayList<PostgresChangeTypeValues>() {{
      add(PostgresChangeTypeValues.DELETE);
      add(PostgresChangeTypeValues.UPDATE);
      add(PostgresChangeTypeValues.INSERT);
    }};
    
  }

  private void setupBaseCDCRecordField() {
    /* Base CDC record only as a Field. To be augmented */

    TestPgMockCDCRecord testPgMockCDCRecord = new TestPgMockCDCRecord("511", "0/0",
        "2018-07-09 10:16:23.815-07");
    baseRecordField = Field.create(testPgMockCDCRecord.getCDCRecord());
  }


  @Before
  public void setup() {
    createConfigBeans();

    walRecordMock = mock(PostgresWalRecord.class);
    when(walRecordMock.getField()).thenReturn(baseRecordField);

    pgSourceMock = mock(PostgresCDCSource.class);
    walReceiverMock = mock(PostgresCDCWalReceiver.class);

    when(pgSourceMock.getWalReceiver()).thenReturn(walReceiverMock);
    when(walReceiverMock.getSchemasAndTables()).thenReturn(schemasAndTables);

  }

  private void setupBeanTableFilter1() {
    // Set up table filtering in config bean
    SchemaTableConfigBean filterRule1 = new SchemaTableConfigBean();
    filterRule1.schema = "public";
    filterRule1.table = "table1";
    filterRule1.excludePattern = null;

    configBean.baseConfigBean.schemaTableConfigs = new ArrayList<SchemaTableConfigBean>() {{
      add(filterRule1);
    }};

  }

  @Test
  public void testPassesTableFilter() {


    /* Set up valid schemasAndTables that have already been pre-filtered
        in walReceiver. This List will be returned from mock class
     */
    schemasAndTables = new ArrayList<SchemaAndTable>() {{
      add(new SchemaAndTable("public", "table1"));
      add(new SchemaAndTable("public", "table2"));
      add(new SchemaAndTable("public", "table3"));
    }};

    /* Setup change in WAL record that will be tested against table filter */
    final Map<String, Field> change1 = new HashMap<String, Field> () {{
      put("kind", Field.create("update"));
      put("schema", Field.create("public"));
      put("table", Field.create("table1"));
    }};

    List<Field> changes = new ArrayList<Field>() {{
      add(Field.create(change1));
    }};

    TestPgMockCDCRecord testPgMockCDCRecord = new TestPgMockCDCRecord("511", "0/0",
        "2018-07-09 10:16:23.815-07", changes);

    PostgresWalRunner pgRunner = new PostgresWalRunner(pgSourceMock);
    //Setting startValue to latest means filter only checks tables, not dates
    configBean.startValue = StartValues.LATEST;
    when(pgSourceMock.getWalReceiver()).thenReturn(walReceiverMock);
    when(pgSourceMock.getConfigBean()).thenReturn(configBean);

    when(walReceiverMock.getSchemasAndTables()).thenReturn(schemasAndTables);
    when(walRecordMock.getChanges()).thenReturn(testPgMockCDCRecord.getCDCRecordChanges());

    Assert.assertTrue(pgRunner.passesFilter(walRecordMock));

    final Map<String, Field> change2 = new HashMap<String, Field> () {{
      put("kind", Field.create("update"));
      put("schema", Field.create("public"));
      put("table", Field.create("table_no_match"));
    }};

    changes = new ArrayList<Field>() {{
      add(Field.create(change2));
    }};

    testPgMockCDCRecord = new TestPgMockCDCRecord("511", "0/0",
        "2018-07-09 10:16:23.815-07", changes);
    when(walRecordMock.getChanges()).thenReturn(testPgMockCDCRecord.getCDCRecordChanges());

    Assert.assertFalse(pgRunner.passesFilter(walRecordMock));

  }

  @Test
  public void testPassesDateFilter() {

    ZoneId zoneId =  ZoneId.of("America/Los_Angeles");

    /* TEST - not testing records based on date, getting StartValues.LATEST, should pass */

    // Setting table/schema filter to null to passesTableFilter() not tested here
    schemasAndTables = null;
    /* Setup change in WAL record that will be tested against table filter */
    final Map<String, Field> change1 = new HashMap<String, Field> () {{
      put("kind", Field.create("update"));
      put("schema", Field.create("public"));
      put("table", Field.create("table1"));
    }};

    List<Field> changes = new ArrayList<Field>() {{
      add(Field.create(change1));
    }};

    TestPgMockCDCRecord testPgMockCDCRecord = new TestPgMockCDCRecord("511", "0/0",
        "2018-07-09 10:16:23.815-07", changes);

    PostgresWalRunner pgRunner = new PostgresWalRunner(pgSourceMock);
    //Setting startValue to latest means not checking dates - automatic pass
    configBean.startValue = StartValues.LATEST;
    when(pgSourceMock.getWalReceiver()).thenReturn(walReceiverMock);
    when(pgSourceMock.getConfigBean()).thenReturn(configBean);
    when(walReceiverMock.getSchemasAndTables()).thenReturn(schemasAndTables);
    when(walRecordMock.getChanges()).thenReturn(testPgMockCDCRecord.getCDCRecordChanges());

    Assert.assertTrue(pgRunner.passesFilter(walRecordMock));

    /* TEST - testing recordDate > filterDate so should pass */

    pgRunner = new PostgresWalRunner(pgSourceMock);
    when(pgSourceMock.getWalReceiver()).thenReturn(walReceiverMock);
    when(walReceiverMock.getSchemasAndTables()).thenReturn(schemasAndTables);
    when(walRecordMock.getChanges()).thenReturn(testPgMockCDCRecord.getCDCRecordChanges());
    when(walRecordMock.getTimestamp()).thenReturn(testPgMockCDCRecord.getTimeStamp());


    configBean.startValue = StartValues.DATE;
    when(pgSourceMock.getConfigBean()).thenReturn(configBean);
    when(pgSourceMock.getStartDate()).thenReturn(LocalDateTime.parse("2000-01-01 01:00:00",
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
    when(pgSourceMock.getZoneId()).thenReturn(zoneId);

    Assert.assertTrue(pgRunner.passesFilter(walRecordMock));

    /* TEST - testing recordDate < filterDate so should fail */

    pgRunner = new PostgresWalRunner(pgSourceMock);
    when(pgSourceMock.getWalReceiver()).thenReturn(walReceiverMock);
    when(walReceiverMock.getSchemasAndTables()).thenReturn(schemasAndTables);
    when(walRecordMock.getChanges()).thenReturn(testPgMockCDCRecord.getCDCRecordChanges());

    configBean.startValue = StartValues.DATE;
    when(pgSourceMock.getConfigBean()).thenReturn(configBean);
    when(pgSourceMock.getStartDate()).thenReturn(LocalDateTime.parse("2020-01-01 01:00:00",
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));

    Assert.assertFalse(pgRunner.passesFilter(walRecordMock));

  }

  @Test
  public void testPassesOperationFilter() {

    /* Test when filter is DELETE, INSERT, UPDATE: (default)
    Change containing DELETE, INSERT AND UPDATE should PASS filter */

    configBean.postgresChangeTypes = new ArrayList<PostgresChangeTypeValues>() {{
      add(PostgresChangeTypeValues.DELETE);
      add(PostgresChangeTypeValues.UPDATE);
      add(PostgresChangeTypeValues.INSERT);
    }};


    /* Setup change in WAL record that will be tested against table filter */
    final Map<String, Field> changeUpdate = new HashMap<String, Field> () {{
      put("kind", Field.create("update"));
      put("schema", Field.create("public"));
      put("table", Field.create("table1"));
    }};

    final Map<String, Field> changeInsert = new HashMap<String, Field> () {{
      put("kind", Field.create("insert"));
      put("schema", Field.create("public"));
      put("table", Field.create("table_no_match"));
    }};

    final Map<String, Field> changeDelete = new HashMap<String, Field> () {{
      put("kind", Field.create("delete"));
      put("schema", Field.create("public"));
      put("table", Field.create("table_no_match"));
    }};

    List<Field> changes = new ArrayList<Field>() {{
      add(Field.create(changeUpdate));
      add(Field.create(changeInsert));
      add(Field.create(changeDelete));
    }};

    TestPgMockCDCRecord testPgMockCDCRecord = new TestPgMockCDCRecord("511", "0/0",
        "2018-07-09 10:16:23.815-07", changes);

    PostgresWalRunner pgRunner = new PostgresWalRunner(pgSourceMock);
    //Setting startValue to latest means filter only checks tables, not dates
    configBean.startValue = StartValues.LATEST;
    when(pgSourceMock.getWalReceiver()).thenReturn(walReceiverMock);
    when(pgSourceMock.getConfigBean()).thenReturn(configBean);

    when(walReceiverMock.getSchemasAndTables()).thenReturn(schemasAndTables);
    when(walRecordMock.getChanges()).thenReturn(testPgMockCDCRecord.getCDCRecordChanges());

    Assert.assertTrue(pgRunner.passesFilter(walRecordMock));


    /* Test when filter is DELETE, INSERT:
    Change containing DELETE, INSERT AND UPDATE should FAIL filter */

    configBean.postgresChangeTypes = new ArrayList<PostgresChangeTypeValues>() {{
      add(PostgresChangeTypeValues.DELETE);
      add(PostgresChangeTypeValues.INSERT);
    }};

    testPgMockCDCRecord = new TestPgMockCDCRecord("511", "0/0",
        "2018-07-09 10:16:23.815-07", changes);

    pgRunner = new PostgresWalRunner(pgSourceMock);
    //Setting startValue to latest means filter only checks tables, not dates
    configBean.startValue = StartValues.LATEST;
    when(pgSourceMock.getWalReceiver()).thenReturn(walReceiverMock);
    when(pgSourceMock.getConfigBean()).thenReturn(configBean);

    when(walReceiverMock.getSchemasAndTables()).thenReturn(schemasAndTables);
    when(walRecordMock.getChanges()).thenReturn(testPgMockCDCRecord.getCDCRecordChanges());

    Assert.assertFalse(pgRunner.passesFilter(walRecordMock));


    /* Test when filter is DELETE, INSERT:
    Change containing DELETE, INSERT should PASS filter */

    changes = new ArrayList<Field>() {{
      add(Field.create(changeInsert));
      add(Field.create(changeDelete));
    }};

    testPgMockCDCRecord = new TestPgMockCDCRecord("511", "0/0",
        "2018-07-09 10:16:23.815-07", changes);

    pgRunner = new PostgresWalRunner(pgSourceMock);
    //Setting startValue to latest means filter only checks tables, not dates
    configBean.startValue = StartValues.LATEST;
    when(pgSourceMock.getWalReceiver()).thenReturn(walReceiverMock);
    when(pgSourceMock.getConfigBean()).thenReturn(configBean);

    when(walReceiverMock.getSchemasAndTables()).thenReturn(schemasAndTables);
    when(walRecordMock.getChanges()).thenReturn(testPgMockCDCRecord.getCDCRecordChanges());

    Assert.assertTrue(pgRunner.passesFilter(walRecordMock));
  }


}
