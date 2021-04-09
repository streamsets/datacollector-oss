/*
 * Copyright 2020 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.jdbc.cdc.oracle;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.ChangeTypeValues;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.SchemaTableConfigBean;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.Month;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class TestLogMinerSession {
  private static final Logger LOG = LoggerFactory.getLogger(TestLogMinerSession.class);

  @Test
  public void testBuildTableConditionTablePatterns() {
    List<SchemaTableConfigBean> tables = ImmutableList.of(
        createTableConfig("SDC", "TABLE1", ""),
        createTableConfig("SDC", "TABLE2", ""),
        createTableConfig("SDC", "%PATTERN1%", ""),
        createTableConfig("SDC", "P_TTERN2", "")
    );

    LogMinerSession.Builder builder = new LogMinerSession.Builder(Mockito.mock(Connection.class), 19);
    String condition = builder.buildTablesCondition(tables);
    Assert.assertEquals(
        "((SEG_OWNER = 'SDC' AND (TABLE_NAME LIKE '%PATTERN1%' OR TABLE_NAME LIKE 'P_TTERN2' OR " +
            "TABLE_NAME IN ('TABLE1','TABLE2'))))",
        condition
    );

  }

  @Test
  public void testBuildTableConditionSchemaPatterns() {
    List<SchemaTableConfigBean> tables = ImmutableList.of(
        createTableConfig("%SDC%", "TABLE1", ""),
        createTableConfig("_SYS_", "%PATTERN1%", ""),
        createTableConfig("_SYS_", "%PATTERN2%", "")
    );

    LogMinerSession.Builder builder = new LogMinerSession.Builder(Mockito.mock(Connection.class), 19);
    String condition = builder.buildTablesCondition(tables);
    Assert.assertEquals(
        "((SEG_OWNER LIKE '%SDC%' AND (TABLE_NAME IN ('TABLE1')))" +
            " OR (SEG_OWNER LIKE '_SYS_' AND (TABLE_NAME LIKE '%PATTERN1%' OR TABLE_NAME LIKE '%PATTERN2%')))",
        condition
    );

  }

  @Test
  public void testBuildTableConditionBig() {
    List<SchemaTableConfigBean> tables = new ArrayList<>(1010);
    List<String> tableNames = new ArrayList<>(1010);
    tables.add(createTableConfig("SYS", "%PATTERN%", ""));
    for (int i = 0; i < 1010; i++) {
      String tableName = RandomStringUtils.randomAlphanumeric(5);
      tableNames.add(Utils.format("'{}'", tableName));
      tables.add(createTableConfig("SYS", tableName, ""));
    }

    LogMinerSession.Builder builder = new LogMinerSession.Builder(Mockito.mock(Connection.class), 19);
    String condition = builder.buildTablesCondition(tables);
    Assert.assertEquals(
        Utils.format(
            "((SEG_OWNER = 'SYS' AND (TABLE_NAME LIKE '%PATTERN%' OR TABLE_NAME IN ({}) OR TABLE_NAME IN ({}))))",
            String.join(",", tableNames.subList(0, 1000)),
            String.join(",", tableNames.subList(1000, 1010))
        ),
        condition
    );
  }

  @Test
  public void testSelectLogsCollectsAllRedoSequences() throws SQLException {
    Connection mockConnection = Mockito.mock(Connection.class);
    mockGlobalQueries(mockConnection);

    LogMinerSession session = new LogMinerSession.Builder(mockConnection, 19)
        .setTablesForMining(ImmutableList.of(createTableConfig("SDC", "TABLE%", "")))
        .setTrackedOperations(ImmutableList.of(ChangeTypeValues.INSERT))
        .build();

    // Create 3 online logs, sequences 100-102:
    // - online_100.log, seq: 1/100, init: 50000 (10:15:00 22/02/15), end: 51000 (11:15:00 22/02/15)
    // - online_101.log, seq: 1/101, init: 51000 (11:15:00 22/02/15), end: 52000 (12:15:00 22/02/15)
    // - online_102.log, seq: 1/102, init: 52000 (12:15:00 22/02/15), end: 53000 (13:15:00 22/02/15) --> CURRENT ONLINE LOG
    LocalDateTime startOnline = LocalDateTime.of(2015, Month.FEBRUARY, 22, 10, 15, 00);
    List<RedoLog> onlineLogs = createLogs(3, 1, 100, 50000,
        startOnline, true, seq -> seq == 102 ? "CURRENT" : "INACTIVE", seq -> seq != 102);

    // Create 10 archived logs, sequences 93-102:
    // - archived_93.log, seq: 1/93, init: 42000 (2:15:00 22/02/15), end: 43000 (3:15:00 22/02/15)
    // - ...
    // - archived_102.log, seq: 1/102, init: 51000 (11:15:00 22/02/15), end: 52000 (12:15:00 22/02/15)
    LocalDateTime startArchived = LocalDateTime.of(2015, Month.FEBRUARY, 22, 2, 15, 00);
    List<RedoLog> archivedLogs = createLogs(10, 1, 93, 42000,
        startArchived, false, seq -> "A", seq -> true);

    List<RedoLog> dest = new ArrayList<>();
    List<RedoLog> src = new ArrayList<>();
    src.addAll(onlineLogs);
    src.addAll(archivedLogs);

    // Use a time range that overlaps with all the existing redo logs in `src`.
    LocalDateTime start = LocalDateTime.of(2015, Month.FEBRUARY, 22, 2, 45, 16);
    LocalDateTime end = LocalDateTime.of(2015, Month.FEBRUARY, 22, 23, 45, 16);
    boolean result = session.selectLogs(start, end, src, dest);
    printLogs(dest, "testFindLogs - Test a time range covering all the data");
    Assert.assertTrue(result);
    Assert.assertEquals(11, dest.size());
    Assert.assertTrue(dest.contains(onlineLogs.get(2)));  // online_102.log, the current online log
    Assert.assertTrue(dest.containsAll(archivedLogs));  // the rest of sequences are archived, so check all of them are included
  }

  @Test
  public void testSelectLogsDiscardOnlineLogs() throws SQLException {

    Connection mockConnection = Mockito.mock(Connection.class);
    mockGlobalQueries(mockConnection);

    LogMinerSession session = new LogMinerSession.Builder(mockConnection, 19)
        .setTablesForMining(ImmutableList.of(createTableConfig("SDC", "TABLE%", "")))
        .setTrackedOperations(ImmutableList.of(ChangeTypeValues.INSERT))
        .build();

    // Create 3 online logs, sequences 100-102:
    // - online_100.log, seq: 1/100, init: 50000 (10:15:00 22/02/15), end: 51000 (11:15:00 22/02/15)
    // - online_101.log, seq: 1/101, init: 51000 (11:15:00 22/02/15), end: 52000 (12:15:00 22/02/15)
    // - online_102.log, seq: 1/102, init: 52000 (12:15:00 22/02/15), end: 53000 (13:15:00 22/02/15) --> CURRENT ONLINE LOG
    LocalDateTime startOnline = LocalDateTime.of(2015, Month.FEBRUARY, 22, 10, 15, 00);
    List<RedoLog> onlineLogs = createLogs(3, 1, 100, 50000,
        startOnline, true, seq -> seq == 102 ? "CURRENT" : "INACTIVE", seq -> seq != 102);

    // Create 3 archived logs, sequences 99-101:
    // - archived_99.log, seq: 1/99, init: 49000 (9:15:00 22/02/15), end: 50000 (10:15:00 22/02/15)
    // - archived_100.log, seq: 1/100, init: 50000 (10:15:00 22/02/15), end: 51000 (11:15:00 22/02/15)
    // - archived_101.log, seq: 1/101, init: 51000 (11:15:00 22/02/15), end: 52000 (12:15:00 22/02/15)
    LocalDateTime startArchived = LocalDateTime.of(2015, Month.FEBRUARY, 22, 9, 15, 00);
    List<RedoLog> archivedLogs = createLogs(3, 1, 99, 49000,
        startArchived, false, seq -> "A", seq -> true);

    List<RedoLog> dest = new ArrayList<>();
    List<RedoLog> src = new ArrayList<>();
    src.addAll(onlineLogs);
    src.addAll(archivedLogs);

    // Use a time range that overlaps with the archived log sequences (99-101).
    LocalDateTime start = LocalDateTime.of(2015, Month.FEBRUARY, 22, 10, 14, 59);
    LocalDateTime end = LocalDateTime.of(2015, Month.FEBRUARY, 22, 12, 01, 30);
    boolean result = session.selectLogs(start, end, src, dest);
    printLogs(dest, "testFindLogs - Test only archived logs are selected");
    Assert.assertTrue(result);
    Assert.assertEquals(3, dest.size());
    Assert.assertTrue(dest.containsAll(archivedLogs));
  }

  @Test
  public void testSelectLogsOnlyPicksCurrentLog() throws SQLException {
    Connection mockConnection = Mockito.mock(Connection.class);
    mockGlobalQueries(mockConnection);

    LogMinerSession session = new LogMinerSession.Builder(mockConnection, 19)
        .setTablesForMining(ImmutableList.of(createTableConfig("SDC", "TABLE%", "")))
        .setTrackedOperations(ImmutableList.of(ChangeTypeValues.INSERT))
        .build();

    // Create 3 online logs, sequences 100-102. No archived logs in 'src'.
    // - online_100.log, seq: 1/100, init: 50000 (10:15:00 22/02/15), end: 51000 (11:15:00 22/02/15)
    // - online_101.log, seq: 1/101, init: 51000 (11:15:00 22/02/15), end: 52000 (12:15:00 22/02/15)
    // - online_102.log, seq: 1/102, init: 52000 (12:15:00 22/02/15), end: 53000 (13:15:00 22/02/15) --> CURRENT ONLINE LOG
    LocalDateTime startOnline = LocalDateTime.of(2015, Month.FEBRUARY, 22, 10, 15, 00);
    List<RedoLog> src = createLogs(3, 1, 100, 50000,
        startOnline, true, seq -> seq == 102 ? "CURRENT" : "ACTIVE", seq -> false);
    List<RedoLog> dest = new ArrayList<>();

    // Use a time interval that is after the beginning of the current online log.
    LocalDateTime start = LocalDateTime.of(2015, Month.FEBRUARY, 22, 12, 25, 30);
    LocalDateTime end = LocalDateTime.of(2015, Month.FEBRUARY, 22, 12, 45, 55);
    boolean result = session.selectLogs(start, end, src, dest);
    printLogs(dest, "testFindLogs - Test a time window reaching the current database time");
    Assert.assertTrue(result);
    Assert.assertEquals(1, dest.size());
    Assert.assertTrue(dest.contains(src.get(2)));  // online_102.log, the current online log
  }

  @Test
  public void testSelectLogsDetectsTransientStateInCurrentLog() throws SQLException {
    Connection mockConnection = Mockito.mock(Connection.class);
    mockGlobalQueries(mockConnection);

    LogMinerSession session = new LogMinerSession.Builder(mockConnection, 19)
        .setTablesForMining(ImmutableList.of(createTableConfig("SDC", "TABLE%", "")))
        .setTrackedOperations(ImmutableList.of(ChangeTypeValues.INSERT))
        .build();

    // Create 3 online logs. Redo 3 will be the new current log, but we simulate the rotation is still in progress.
    // - online_1.log, seq: 1/1, init: 0 (22:00:00 24/06/20), end: 1000 (23:00:00 24/06/20)
    // - online_2.log, seq: 1/2, init: 1000 (23:00:00 24/06/20), end: 2000 (00:00:00 25/06/20)
    // - online_3.log, seq: 1/3, init: 0 (old timestamp), end: 0 (old timestamp) --> NEXT CURRENT ONLINE LOG
    LocalDateTime startOnline = LocalDateTime.of(2020, Month.JUNE, 24, 22, 00, 00);
    List<RedoLog> src = createLogs(2, 1, 1, 0,
        startOnline, true, seq -> "ACTIVE", seq -> false);
    RedoLog nextCurrentLog = new RedoLog("online_3.log", BigDecimal.valueOf(3), BigDecimal.valueOf(1), BigDecimal.ZERO,
        LocalDateTime.of(2020, Month.JUNE, 24, 21, 00, 00),
        LocalDateTime.of(2020, Month.JUNE, 24, 22, 00, 00),
        BigDecimal.ZERO, BigDecimal.ZERO, false, false, "UNUSED", true, false
    );
    src.add(nextCurrentLog);
    List<RedoLog> dest = new ArrayList<>();

    // Use a time interval that will be covered by the next current online log (once the log rotation would be done).
    LocalDateTime start = LocalDateTime.of(2020, Month.JUNE, 25, 00, 10, 00);
    LocalDateTime end = LocalDateTime.of(2020, Month.JUNE, 25, 00, 15, 00);
    boolean result = session.selectLogs(start, end, src, dest);
    printLogs(dest, "testFindLogs - Test detection of a transient state for the current online log");
    Assert.assertFalse(result);
    Assert.assertTrue(dest.isEmpty());
  }

  @Test
  public void testSelectLogsDetectsTransientStateInArchivedLog() throws SQLException {
    Connection mockConnection = Mockito.mock(Connection.class);
    mockGlobalQueries(mockConnection);
    LogMinerSession session = new LogMinerSession.Builder(mockConnection, 19)
        .setTablesForMining(ImmutableList.of(createTableConfig("SDC", "TABLE%", "")))
        .setTrackedOperations(ImmutableList.of(ChangeTypeValues.INSERT))
        .build();

    // Create 3 online logs, sequences 100-102:
    // - online_100.log, seq: 1/100, init: 50000 (10:15:00 22/02/15), end: 51000 (11:15:00 22/02/15) --> ARCHIVED
    // - online_101.log, seq: 1/101, init: 51000 (11:15:00 22/02/15), end: 52000 (12:15:00 22/02/15) --> ARCHIVED
    // - online_102.log, seq: 1/102, init: 52000 (12:15:00 22/02/15), end: 53000 (13:15:00 22/02/15) --> CURRENT ONLINE LOG
    LocalDateTime startOnline = LocalDateTime.of(2015, Month.FEBRUARY, 22, 10, 15, 00);
    List<RedoLog> onlineLogs = createLogs(3, 1, 100, 50000,
        startOnline, true, seq -> seq == 102 ? "CURRENT" : "INACTIVE", seq -> seq != 102);

    // Create 2 archived logs, sequences 99 and 100. Archiving of sequence 101 is still in progress and therefore
    // would not be returned by the LogMiner query.
    // - archived_99.log, seq: 1/99, init: 49000 (9:15:00 22/02/15), end: 50000 (10:15:00 22/02/15)
    // - archived_100.log, seq: 1/100, init: 50000 (10:15:00 22/02/15), end: 51000 (11:15:00 22/02/15)
    // - archived_101.log --> PENDING TO COMPLETE!
    LocalDateTime startArchived = LocalDateTime.of(2015, Month.FEBRUARY, 22, 9, 15, 00);
    List<RedoLog> archivedLogs = createLogs(2, 1, 99, 49000,
        startArchived, false, seq -> "A", seq -> true);

    List<RedoLog> dest = new ArrayList<>();
    List<RedoLog> src = new ArrayList<>();
    src.addAll(onlineLogs);
    src.addAll(archivedLogs);

    // Use a time range that overlaps with all the redo log sequences in 'src' (99-102).
    LocalDateTime start = LocalDateTime.of(2015, Month.FEBRUARY, 22, 10, 14, 00);
    LocalDateTime end = LocalDateTime.of(2015, Month.FEBRUARY, 22, 12, 18, 30);
    boolean result = session.selectLogs(start, end, src, dest);
    printLogs(dest, "testFindLogs - Test detection of a transient state for an archived online log");
    Assert.assertFalse(result);
    Assert.assertTrue(dest.isEmpty());
  }

  @Test
  public void testSelectLogsCollectsAllRedoSequencesInRACDatabase() throws SQLException {
    Connection mockConnection = Mockito.mock(Connection.class);
    mockGlobalQueries(mockConnection);

    LogMinerSession session = new LogMinerSession.Builder(mockConnection, 19)
        .setTablesForMining(ImmutableList.of(createTableConfig("SDC", "TABLE%", "")))
        .setTrackedOperations(ImmutableList.of(ChangeTypeValues.INSERT))
        .build();

    // Create 3 online logs, sequences 100-102 in thread 1:
    // - online_100.log, seq: 1/100, init: 50000 (10:15:00 22/02/15), end: 51000 (11:15:00 22/02/15)
    // - online_101.log, seq: 1/101, init: 51000 (11:15:00 22/02/15), end: 52000 (12:15:00 22/02/15)
    // - online_102.log, seq: 1/102, init: 52000 (12:15:00 22/02/15), end: 53000 (13:15:00 22/02/15) --> CURRENT ONLINE LOG
    LocalDateTime startOnline1 = LocalDateTime.of(2015, Month.FEBRUARY, 22, 10, 15, 00);
    List<RedoLog> onlineLogs1 = createLogs(3, 1, 100, 50000,
        startOnline1, true, seq -> seq == 102 ? "CURRENT" : "INACTIVE", seq -> seq != 102);

    // Create 5 online logs, sequences 11-15 in thread 2, none of them has been archived yet:
    // - online_11.log, seq: 2/11, init: 49100 (09:20:00 22/02/15), end: 50100 (10:20:00 22/02/15)
    // - ...
    // - online_12.log, seq: 2/15, init: 53100 (13:20:00 22/02/15), end: 54100 (14:20:00 22/02/15) --> CURRENT ONLINE LOG
    LocalDateTime startOnline2 = LocalDateTime.of(2015, Month.FEBRUARY, 22, 9, 20, 00);
    List<RedoLog> onlineLogs2 =  createLogs(5, 2, 11, 49100,
        startOnline2, true, seq -> seq == 15 ? "CURRENT" : "ACTIVE", seq -> false);

    // Create 10 archived logs, sequences 93-102 in thread 1:
    // - archived_93.log, seq: 1/93, init: 42000 (2:15:00 22/02/15), end: 43000 (3:15:00 22/02/15)
    // - ...
    // - archived_102.log, seq: 1/102, init: 51000 (11:15:00 22/02/15), end: 52000 (12:15:00 22/02/15)
    LocalDateTime startArchived1 = LocalDateTime.of(2015, Month.FEBRUARY, 22, 2, 15, 00);
    List<RedoLog> archivedLogs1 = createLogs(10, 1, 93, 42000,
        startArchived1, false, seq -> "A", seq -> true);

    // Create 5 archived logs, sequences 6-10 in thread 2:
    // - archived_6.log, seq: 2/6, init: 44100 (04:20:00 22/02/15), end: 45100 (5:20:00 22/02/15)
    // - ...
    // - archived_10.log, seq: 2/10, init: 48100 (08:20:00 22/02/15), end: 45100 (09:20:00 22/02/15)
    LocalDateTime startArchived2 = LocalDateTime.of(2015, Month.FEBRUARY, 22, 4, 20, 00);
    List<RedoLog> archivedLogs2 = createLogs(5, 2, 6, 44100,
        startArchived2, false, seq -> "A", seq -> true);

    List<RedoLog> dest = new ArrayList<>();
    List<RedoLog> src = new ArrayList<>();
    src.addAll(onlineLogs1);
    src.addAll(onlineLogs2);
    src.addAll(archivedLogs1);
    src.addAll(archivedLogs2);

    // Use a time range that overlaps with all the existing redo logs in `src`.
    LocalDateTime start = LocalDateTime.of(2015, Month.FEBRUARY, 22, 2, 45, 16);
    LocalDateTime end = LocalDateTime.of(2015, Month.FEBRUARY, 22, 14, 45, 00);
    boolean result = session.selectLogs(start, end, src, dest);
    printLogs(dest, "testFindLogs - Test a time range covering all the data in a RAC scenario");
    Assert.assertTrue(result);
    Assert.assertEquals(21, dest.size());
    Assert.assertTrue(dest.contains(onlineLogs1.get(2)));  // online_102.log, the current online log for thread 1
    Assert.assertTrue(dest.containsAll(onlineLogs2));  // all online logs in thread 2 required, as they are not archived
    Assert.assertTrue(dest.containsAll(archivedLogs1));
    Assert.assertTrue(dest.containsAll(archivedLogs2));
  }

  @Test
  public void testSelectLogsDetectsTransientStateInRACDatabase() throws SQLException {
    Connection mockConnection = Mockito.mock(Connection.class);
    mockGlobalQueries(mockConnection);

    LogMinerSession session = new LogMinerSession.Builder(mockConnection, 19)
        .setTablesForMining(ImmutableList.of(createTableConfig("SDC", "TABLE%", "")))
        .setTrackedOperations(ImmutableList.of(ChangeTypeValues.INSERT))
        .build();

    // Create 3 online logs, sequences 100-102 in thread 1:
    // - online_100.log, seq: 1/100, init: 50000 (10:15:00 22/02/15), end: 51000 (11:15:00 22/02/15)
    // - online_101.log, seq: 1/101, init: 51000 (11:15:00 22/02/15), end: 52000 (12:15:00 22/02/15)
    // - online_102.log, seq: 1/102, init: 52000 (12:15:00 22/02/15), end: 53000 (13:15:00 22/02/15) --> CURRENT ONLINE LOG
    LocalDateTime startOnline1 = LocalDateTime.of(2015, Month.FEBRUARY, 22, 10, 15, 00);
    List<RedoLog> onlineLogs1 = createLogs(3, 1, 100, 50000,
        startOnline1, true, seq -> seq == 102 ? "CURRENT" : "INACTIVE", seq -> seq != 102);

    // Create 5 online logs, sequences 11-15 in thread 2:
    // - online_11.log, seq: 2/11, init: 49100 (09:20:00 22/02/15), end: 50100 (10:20:00 22/02/15)
    // - ...
    // - online_12.log, seq: 2/15, init: 53100 (13:20:00 22/02/15), end: 54100 (14:20:00 22/02/15) --> CURRENT ONLINE LOG
    LocalDateTime startOnline2 = LocalDateTime.of(2015, Month.FEBRUARY, 22, 9, 20, 00);
    List<RedoLog> onlineLogs2 =  createLogs(5, 2, 11, 49100,
        startOnline2, true, seq -> seq == 15 ? "CURRENT" : "INACTIVE", seq -> seq != 15);

    // Create 10 archived logs, sequences 93-102 in thread 1:
    // - archived_93.log, seq: 1/93, init: 42000 (2:15:00 22/02/15), end: 43000 (3:15:00 22/02/15)
    // - ...
    // - archived_102.log, seq: 1/102, init: 51000 (11:15:00 22/02/15), end: 52000 (12:15:00 22/02/15)
    LocalDateTime startArchived1 = LocalDateTime.of(2015, Month.FEBRUARY, 22, 2, 15, 00);
    List<RedoLog> archivedLogs1 = createLogs(10, 1, 93, 42000,
        startArchived1, false, seq -> "A", seq -> true);

    // Create 5 archived logs, sequences 6-10 in thread 2. Archived logs should contain also sequences 11-14, but their
    // archiving processes are still in progress.
    // - archived_6.log, seq: 2/6, init: 44100 (04:20:00 22/02/15), end: 45100 (5:20:00 22/02/15)
    // - ...
    // - archived_10.log, seq: 2/10, init: 48100 (08:20:00 22/02/15), end: 45100 (09:20:00 22/02/15)
    LocalDateTime startArchived2 = LocalDateTime.of(2015, Month.FEBRUARY, 22, 4, 20, 00);
    List<RedoLog> archivedLogs2 = createLogs(5, 2, 6, 44100,
        startArchived2, false, seq -> "A", seq -> true);

    List<RedoLog> dest = new ArrayList<>();
    List<RedoLog> src = new ArrayList<>();
    src.addAll(onlineLogs1);
    src.addAll(onlineLogs2);
    src.addAll(archivedLogs1);
    src.addAll(archivedLogs2);

    // Use a time range that overlaps with all the existing redo logs in `src`.
    LocalDateTime start = LocalDateTime.of(2015, Month.FEBRUARY, 22, 2, 45, 16);
    LocalDateTime end = LocalDateTime.of(2015, Month.FEBRUARY, 22, 14, 45, 00);
    boolean result = session.selectLogs(start, end, src, dest);
    printLogs(dest, "testFindLogs - Test detection of a transient state in a RAC scenario");
    Assert.assertFalse(result);
    Assert.assertTrue(dest.isEmpty());
  }

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void testErrorJDBC52Format() throws Exception
  {
    exceptionRule.expect(StageException.class);
    exceptionRule.expectMessage("JDBC_52 - Error starting LogMiner: Action: Start - Message: null - SQL State: null - Vendor Code: 0");

    Connection mockConnection = Mockito.mock(Connection.class);
    Statement mockStatement = Mockito.mock(Statement.class);
    Mockito.when(mockConnection.createStatement()).thenReturn(mockStatement);
    Mockito.when(mockConnection.createStatement().execute(Mockito.any())).thenThrow(new SQLException());

    mockGlobalQueries(mockConnection);

    SchemaTableConfigBean config = new SchemaTableConfigBean();
    config.schema = "SDC";
    config.table = "cdc";
    config.excludePattern = "";
    List<SchemaTableConfigBean> tablesForMining = ImmutableList.of(config);
    List<ChangeTypeValues> trackedOperations = ImmutableList.of(ChangeTypeValues.INSERT);

    LogMinerSession.Builder builder = new LogMinerSession.Builder(mockConnection, 18);
    builder.setTablesForMining(tablesForMining);
    builder.setTrackedOperations(trackedOperations);
    builder.setContinuousMine(true);

    LogMinerSession session = builder.build();
    session.start(LocalDateTime.now(), LocalDateTime.now());
  }

  private SchemaTableConfigBean createTableConfig(String schema, String table, String exclusion) {
    SchemaTableConfigBean config = new SchemaTableConfigBean();
    config.schema = schema;
    config.table = table;
    config.excludePattern = exclusion;
    return config;
  }

  private List<RedoLog> createLogs(int totalLogs, int thread, int initialSequence,
      int initialSCN, LocalDateTime initialDatetime, boolean online, Function<Integer, String> statusSetter,
      Function<Integer, Boolean> isArchived) {
    List<RedoLog> result = new ArrayList<>();
    int scnInterval = 1000;
    int timeInterval = 60;  // minutes
    String prefix = online ? "online" : "archived";
    BigDecimal maxSCN = BigDecimal.valueOf(Long.MAX_VALUE);  // Not the real upper bound, but ok for the tests here.

    for (int i = 0; i < totalLogs; i++) {
      int sequence = initialSequence + i;
      String path = Utils.format("{}_{}.log", prefix, sequence);
      String status = statusSetter.apply(sequence);
      LocalDateTime firstTime = initialDatetime.plusMinutes(i * timeInterval);
      LocalDateTime endTime = firstTime.plusMinutes(timeInterval);
      BigDecimal firstChange = BigDecimal.valueOf(i * scnInterval + initialSCN);
      BigDecimal nextChange = BigDecimal.valueOf((i + 1) * scnInterval + initialSCN);
      BigDecimal group = online ? BigDecimal.valueOf(i + 1) : null;
      RedoLog log = new RedoLog(path, group, BigDecimal.valueOf(thread), BigDecimal.valueOf(sequence), firstTime,
          endTime, firstChange, nextChange, false, false, status, online, isArchived.apply(sequence));
      result.add(log);
    }
    return result;
  }

  private void mockGlobalQueries(Connection mockConnection) throws SQLException{
    ResultSet mockResultSet = Mockito.mock(ResultSet.class);
    Mockito.when(mockResultSet.next()).thenReturn(true);
    PreparedStatement mockPreparedStatement = Mockito.mock(PreparedStatement.class);
    Mockito.when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
    Mockito.when(mockConnection.prepareStatement(LogMinerSession.CHECK_EMPTY_STRING_EQUALS_NULL_QUERY)).thenReturn(mockPreparedStatement);

    ResultSet incarnationsResultSet = Mockito.mock(ResultSet.class);
    Mockito.when(incarnationsResultSet.next()).thenReturn(true).thenReturn(false);
    Mockito.when(incarnationsResultSet.getBigDecimal(1)).thenReturn(BigDecimal.valueOf(11111));
    CallableStatement incarnationsPreparedStatement = Mockito.mock(CallableStatement.class);
    Mockito.when(incarnationsPreparedStatement.executeQuery()).thenReturn(incarnationsResultSet);
    Mockito.when(mockConnection.prepareCall(LogMinerSession.SELECT_CURRENT_DATABASE_INCARNATION_QUERY)).thenReturn(incarnationsPreparedStatement);
  }

  private void printLogs(List<RedoLog> logs, String header) {
    LOG.debug(">>> " + header);
    logs.stream().forEach(log -> LOG.debug("{}", log));
  }

}
