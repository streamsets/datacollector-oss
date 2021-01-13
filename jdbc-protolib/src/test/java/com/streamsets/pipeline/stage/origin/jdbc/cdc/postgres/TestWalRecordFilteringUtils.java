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
package com.streamsets.pipeline.stage.origin.jdbc.cdc.postgres;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.Stage.ConfigIssue;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.lib.jdbc.JdbcUtil;
import com.streamsets.pipeline.lib.jdbc.PostgresHikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.connection.PostgresConnection;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.SchemaAndTable;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.SchemaTableConfigBean;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.postgresql.replication.LogSequenceNumber;

import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.mockito.Matchers.any;

public class TestWalRecordFilteringUtils {

  private PostgresCDCConfigBean configBean;
  private String username = "postgres";
  private String password = "postgres";

  private PostgresCDCSource pgSourceMock;
  private PostgresCDCWalReceiver walReceiverMock;
  private PostgresWalRecord walRecordMock;
  private List<SchemaAndTable> schemasAndTables;
  private Field baseRecordField;
  private Stage.Context contextMock;

  private void createConfigBeans() {
    PostgresHikariPoolConfigBean hikariConfigBean = new PostgresHikariPoolConfigBean();
    hikariConfigBean.connection = new PostgresConnection();
    hikariConfigBean.connection.connectionString = "jdbc:postgresql://localhost:5432/sdctest";
    hikariConfigBean.connection.useCredentials = true;
    hikariConfigBean.connection.username = () -> username;
    hikariConfigBean.connection.password = () -> password;

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

    walRecordMock = Mockito.mock(PostgresWalRecord.class);
    Mockito.when(walRecordMock.getField()).thenReturn(baseRecordField);

    pgSourceMock = Mockito.mock(PostgresCDCSource.class);
    walReceiverMock = Mockito.mock(PostgresCDCWalReceiver.class);

    Mockito.when(pgSourceMock.getWalReceiver()).thenReturn(walReceiverMock);
    Mockito.when(walReceiverMock.getSchemasAndTables()).thenReturn(schemasAndTables);

    contextMock = Mockito.mock(Stage.Context.class);
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

    final Map<String, Field> change2 = new HashMap<String, Field> () {{
      put("kind", Field.create("update"));
      put("schema", Field.create("public"));
      put("table", Field.create("not_a_valid_table"));
    }};

    final Map<String, Field> change3 = new HashMap<String, Field> () {{
      put("kind", Field.create("update"));
      put("schema", Field.create("public"));
      put("table", Field.create("table3"));
    }};

    List<Field> changes = new ArrayList<Field>() {{
      add(Field.create(change1));
      add(Field.create(change2));
      add(Field.create(change3));
    }};

    TestPgMockCDCRecord testPgMockCDCRecord = new TestPgMockCDCRecord("511", "0/0",
        "2018-07-09 10:16:23.815-07", changes);

    //Setting startValue to latest means filter only checks tables, not dates
    configBean.startValue = StartValues.LATEST;
    Mockito.when(pgSourceMock.getWalReceiver()).thenReturn(walReceiverMock);
    Mockito.when(pgSourceMock.getConfigBean()).thenReturn(configBean);

    Mockito.when(walReceiverMock.getSchemasAndTables()).thenReturn(schemasAndTables);
    Mockito.when(walRecordMock.getChanges()).thenReturn(testPgMockCDCRecord.getCDCRecordChanges());
    Mockito.when(walRecordMock.getBuffer()).thenReturn(ByteBuffer.wrap(new String("test").getBytes()));
    Mockito.when(walRecordMock.getLsn()).thenReturn(LogSequenceNumber.valueOf("0/0"));
    Mockito.when(walRecordMock.getDecoder()).thenReturn(DecoderValues.WAL2JSON);
    Mockito.when(walRecordMock.getField()).thenReturn(Field.create(testPgMockCDCRecord.getCDCRecord()));

    PostgresWalRecord filteredRecord = WalRecordFilteringUtils.passesTableFilter(walRecordMock, pgSourceMock);
    Assert.assertEquals(walRecordMock.getChanges().size(), 3);
    Assert.assertEquals(filteredRecord.getChanges().size(), 2);
  }

  @Test
  public void testValidateSchemaAndTables() throws Exception {
    /* Test that when a schema and table is configured
       and no tables are retrieved from database
       Init finds one issue
     */

    ResultSet resultSetMock = Mockito.mock(ResultSet.class);
    Mockito.when(resultSetMock.next()).thenReturn(false);

    JdbcUtil jdbcUtilMock = Mockito.mock(JdbcUtil.class);
    Mockito.when(walReceiverMock.getJdbcUtil()).thenReturn(jdbcUtilMock);
    Mockito.when(jdbcUtilMock.getTableAndViewMetadata(any(), any(), any())).thenReturn(resultSetMock);

    //Setting startValue to latest means filter only checks tables, not dates
    configBean.startValue = StartValues.LATEST;

    SchemaTableConfigBean filterRule1 = new SchemaTableConfigBean();
    filterRule1.schema = "public";
    filterRule1.table = "table1";
    filterRule1.excludePattern = null;

    Mockito.when(walReceiverMock.validateSchemaAndTables()).thenCallRealMethod();
    Mockito.when(walReceiverMock.getSchemaAndTableConfig()).thenReturn(new ArrayList<SchemaTableConfigBean>() {{
      add(filterRule1);
    }});

    Mockito.when(walReceiverMock.getContext()).thenReturn(contextMock);
    Mockito.when(contextMock.createConfigIssue(Mockito.any(), Mockito.any(), Mockito.eq(JdbcErrors.JDBC_66)))
              .thenReturn(new com.streamsets.pipeline.api.ConfigIssue() {
                  @Override
                  public String toString() {
                      return JdbcErrors.JDBC_66.getCode();
                  }
              });

    Optional<List<ConfigIssue>> issuesOptional = walReceiverMock.validateSchemaAndTables();

    walReceiverMock.configBean = configBean;

    Assert.assertTrue(issuesOptional.isPresent());
    List<ConfigIssue> issues = issuesOptional.get();
    Assert.assertEquals(issues.size(), 1);
    Assert.assertEquals(issues.get(0).toString(),JdbcErrors.JDBC_66.getCode());
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

    //Setting startValue to latest means not checking dates - automatic pass
    configBean.startValue = StartValues.LATEST;
    Mockito.when(pgSourceMock.getWalReceiver()).thenReturn(walReceiverMock);
    Mockito.when(pgSourceMock.getConfigBean()).thenReturn(configBean);
    Mockito.when(walReceiverMock.getSchemasAndTables()).thenReturn(schemasAndTables);
    Mockito.when(walRecordMock.getChanges()).thenReturn(testPgMockCDCRecord.getCDCRecordChanges());
    Mockito.when(walRecordMock.getBuffer()).thenReturn(ByteBuffer.wrap("test".getBytes()));
    Mockito.when(walRecordMock.getLsn()).thenReturn(LogSequenceNumber.valueOf("0/0"));
    Mockito.when(walRecordMock.getXid()).thenReturn("511");
    Mockito.when(walRecordMock.getDecoder()).thenReturn(DecoderValues.WAL2JSON);
    Mockito.when(walRecordMock.getField()).thenReturn(Field.create(testPgMockCDCRecord.getCDCRecord()));

    PostgresWalRecord filteredRecord = WalRecordFilteringUtils.passesStartValueFilter(walRecordMock, pgSourceMock);
    Assert.assertEquals(walRecordMock.getChanges().size(), 1);
    Assert.assertEquals(filteredRecord.getChanges().size(), 1);

    /* TEST - testing recordDate > filterDate so should pass */

    Mockito.when(pgSourceMock.getWalReceiver()).thenReturn(walReceiverMock);
    Mockito.when(walReceiverMock.getSchemasAndTables()).thenReturn(schemasAndTables);
    Mockito.when(walRecordMock.getChanges()).thenReturn(testPgMockCDCRecord.getCDCRecordChanges());
    Mockito.when(walRecordMock.getTimestamp()).thenReturn(testPgMockCDCRecord.getTimeStamp());
    Mockito.when(walRecordMock.getLsn()).thenReturn(LogSequenceNumber.valueOf("0/0"));
    Mockito.when(walRecordMock.getXid()).thenReturn("511");
    Mockito.when(walRecordMock.getDecoder()).thenReturn(DecoderValues.WAL2JSON);
    Mockito.when(walRecordMock.getField()).thenReturn(Field.create(testPgMockCDCRecord.getCDCRecord()));

    configBean.startValue = StartValues.DATE;
    Mockito.when(pgSourceMock.getConfigBean()).thenReturn(configBean);
    Mockito.when(pgSourceMock.getStartDate()).thenReturn(LocalDateTime.parse("2000-01-01 01:00:00",
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
    Mockito.when(pgSourceMock.getZoneId()).thenReturn(zoneId);

    filteredRecord = WalRecordFilteringUtils.passesStartValueFilter(walRecordMock, pgSourceMock);
    Assert.assertEquals(walRecordMock.getChanges().size(), 1);
    Assert.assertEquals(filteredRecord.getChanges().size(), 1);

    /* TEST - testing recordDate < filterDate so should fail */

    Mockito.when(pgSourceMock.getWalReceiver()).thenReturn(walReceiverMock);
    Mockito.when(walReceiverMock.getSchemasAndTables()).thenReturn(schemasAndTables);
    Mockito.when(walRecordMock.getChanges()).thenReturn(testPgMockCDCRecord.getCDCRecordChanges());
    Mockito.when(walRecordMock.getLsn()).thenReturn(LogSequenceNumber.valueOf("0/0"));
    Mockito.when(walRecordMock.getXid()).thenReturn("511");
    Mockito.when(walRecordMock.getDecoder()).thenReturn(DecoderValues.WAL2JSON);
    Mockito.when(walRecordMock.getField()).thenReturn(Field.create(testPgMockCDCRecord.getCDCRecord()));

    configBean.startValue = StartValues.DATE;
    Mockito.when(pgSourceMock.getConfigBean()).thenReturn(configBean);
    Mockito.when(pgSourceMock.getStartDate()).thenReturn(LocalDateTime.parse("2020-01-01 01:00:00",
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));

    filteredRecord = WalRecordFilteringUtils.passesStartValueFilter(walRecordMock, pgSourceMock);
    Assert.assertNull(filteredRecord);
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

    //Setting startValue to latest means filter only checks tables, not dates
    configBean.startValue = StartValues.LATEST;
    Mockito.when(pgSourceMock.getWalReceiver()).thenReturn(walReceiverMock);
    Mockito.when(pgSourceMock.getConfigBean()).thenReturn(configBean);

    Mockito.when(walReceiverMock.getSchemasAndTables()).thenReturn(schemasAndTables);
    Mockito.when(walRecordMock.getChanges()).thenReturn(testPgMockCDCRecord.getCDCRecordChanges());
    Mockito.when(walRecordMock.getBuffer()).thenReturn(ByteBuffer.wrap(new String("test").getBytes()));
    Mockito.when(walRecordMock.getLsn()).thenReturn(LogSequenceNumber.valueOf("0/0"));
    Mockito.when(walRecordMock.getXid()).thenReturn("511");
    Mockito.when(walRecordMock.getDecoder()).thenReturn(DecoderValues.WAL2JSON);
    Mockito.when(walRecordMock.getField()).thenReturn(Field.create(testPgMockCDCRecord.getCDCRecord()));

    PostgresWalRecord filteredRecord = WalRecordFilteringUtils.passesOperationFilter(walRecordMock, pgSourceMock);
    Assert.assertEquals(walRecordMock.getChanges().size(), 3);
    Assert.assertEquals(filteredRecord.getChanges().size(), 3);

    /* Test when filter is DELETE, INSERT:
    Change containing DELETE, INSERT AND UPDATE should FAIL filter */

    configBean.postgresChangeTypes = new ArrayList<PostgresChangeTypeValues>() {{
      add(PostgresChangeTypeValues.DELETE);
      add(PostgresChangeTypeValues.INSERT);
    }};

    testPgMockCDCRecord = new TestPgMockCDCRecord("511", "0/0",
        "2018-07-09 10:16:23.815-07", changes);

    //Setting startValue to latest means filter only checks tables, not dates
    configBean.startValue = StartValues.LATEST;
    Mockito.when(pgSourceMock.getWalReceiver()).thenReturn(walReceiverMock);
    Mockito.when(pgSourceMock.getConfigBean()).thenReturn(configBean);

    Mockito.when(walReceiverMock.getSchemasAndTables()).thenReturn(schemasAndTables);
    Mockito.when(walRecordMock.getChanges()).thenReturn(testPgMockCDCRecord.getCDCRecordChanges());


    filteredRecord = WalRecordFilteringUtils.passesOperationFilter(walRecordMock, pgSourceMock);
    Assert.assertEquals(filteredRecord.getLsn(), walRecordMock.getLsn());
    Assert.assertEquals(filteredRecord.getXid(), walRecordMock.getXid());
    Assert.assertEquals(walRecordMock.getChanges().size(), 3);
    Assert.assertEquals(filteredRecord.getChanges().size(), 2);

    /* Test when filter is DELETE, INSERT:
    Change containing DELETE, INSERT should PASS filter */

    changes = new ArrayList<Field>() {{
      add(Field.create(changeInsert));
      add(Field.create(changeDelete));
    }};

    testPgMockCDCRecord = new TestPgMockCDCRecord("511", "0/0",
        "2018-07-09 10:16:23.815-07", changes);

    //Setting startValue to latest means filter only checks tables, not dates
    configBean.startValue = StartValues.LATEST;
    Mockito.when(pgSourceMock.getWalReceiver()).thenReturn(walReceiverMock);
    Mockito.when(pgSourceMock.getConfigBean()).thenReturn(configBean);

    Mockito.when(walReceiverMock.getSchemasAndTables()).thenReturn(schemasAndTables);
    Mockito.when(walRecordMock.getChanges()).thenReturn(testPgMockCDCRecord.getCDCRecordChanges());

    filteredRecord = WalRecordFilteringUtils.passesOperationFilter(walRecordMock, pgSourceMock);
    Assert.assertEquals(filteredRecord.getLsn(), walRecordMock.getLsn());
    Assert.assertEquals(filteredRecord.getXid(), walRecordMock.getXid());
    Assert.assertEquals(walRecordMock.getChanges().size(), 2);
    Assert.assertEquals(filteredRecord.getChanges().size(), 2);
  }


}
