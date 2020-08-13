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
package com.streamsets.pipeline.stage.origin.jdbc;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.lineage.EndPointType;
import com.streamsets.pipeline.api.lineage.LineageEvent;
import com.streamsets.pipeline.api.lineage.LineageEventType;
import com.streamsets.pipeline.api.lineage.LineageSpecificAttribute;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.lib.jdbc.JdbcHikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.UnknownTypeAction;
import com.streamsets.pipeline.lib.jdbc.connection.JdbcConnection;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.h2.tools.SimpleResultSet;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("Duplicates")
@Ignore //SCJDBC-130. Migrate this class to STF
public class TestJdbcSource {
  private static final Logger LOG = LoggerFactory.getLogger(TestJdbcSource.class);
  private static final int BATCH_SIZE = 1000;
  private static final int CLOB_SIZE = 1000;

  private final String username = "sa";
  private final String password = "sa";
  private final String database = "test";

  private final String h2ConnectionString = "jdbc:h2:mem:" + database;
  private final String query = "SELECT * FROM TEST.TEST_TABLE WHERE P_ID > ${offset} ORDER BY P_ID ASC LIMIT 10;";
  private final String queryNonIncremental = "SELECT * FROM TEST.TEST_TABLE LIMIT 10;";
  private final String queryStoredProcedure = "CALL STOREDPROC();";
  private final String queryUnknownType = "SELECT * FROM TEST.TEST_UNKNOWN_TYPE WHERE P_ID > ${offset} ORDER BY P_ID ASC LIMIT 10;";
  private final String initialOffset = "0";
  private final String queriesPerSecond = "0";
  private final long queryInterval = 0;

  private Connection connection = null;
  // This is used for the stored procedure, do not remove

  public static ResultSet simpleResultSet() throws SQLException {
    SimpleResultSet rs = new SimpleResultSet();
    rs.addColumn("ID", Types.INTEGER, 10, 0);
    rs.addColumn("NAME", Types.VARCHAR, 255, 0);
    rs.addRow(0, "San Francisco");
    rs.addRow(1, "Brno");
    return rs;
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() throws SQLException {
    // Create a table in H2 and put some data in it for querying.
    connection = DriverManager.getConnection(h2ConnectionString, username, password);
    try (Statement statement = connection.createStatement()) {
      // Setup table
      statement.addBatch("CREATE SCHEMA IF NOT EXISTS TEST;");
      statement.addBatch(
          "CREATE TABLE IF NOT EXISTS TEST.TEST_TABLE " +
              "(p_id INT NOT NULL, first_name VARCHAR(255), last_name VARCHAR(255), UNIQUE(p_id));"
      );
      statement.addBatch(
          "CREATE TABLE IF NOT EXISTS TEST.TEST_ARRAY " +
              "(p_id INT NOT NULL, non_scalar ARRAY, UNIQUE(p_id));"
      );
      statement.addBatch(
          "CREATE TABLE IF NOT EXISTS TEST.TEST_LOB " +
              "(p_id INT NOT NULL, clob_col CLOB, blob_col BLOB, UNIQUE(p_id));"
      );
      statement.addBatch(
          "CREATE TABLE IF NOT EXISTS TEST.TEST_JDBC_NS_HEADERS " +
              "(p_id INT NOT NULL, dec DECIMAL(2, 1));"
      );
      statement.addBatch(
          "CREATE TABLE IF NOT EXISTS TEST.TEST_NULL " +
              "(p_id INT NOT NULL, name VARCHAR(255), number int, ts TIMESTAMP);"
      );
      statement.addBatch(
        "CREATE TABLE IF NOT EXISTS TEST.TEST_TIMES " +
          "(p_id INT NOT NULL, d DATE, t TIME, ts TIMESTAMP);"
      );
      statement.addBatch(
        "CREATE TABLE IF NOT EXISTS TEST.TEST_UUID " +
          "(p_id INT NOT NULL, p_uuid UUID);"
      );
      statement.addBatch(
        "CREATE TABLE IF NOT EXISTS TEST.TEST_UNKNOWN_TYPE " +
          "(p_id INT NOT NULL, geo GEOMETRY);"
      );
      statement.addBatch(
        "CREATE TABLE IF NOT EXISTS TEST.TIMESTAMP_9 " +
          "(p_id INT NOT NULL, ts TIMESTAMP(9));"
      );
      // Add some data
      statement.addBatch("INSERT INTO TEST.TEST_TABLE VALUES (1, 'Adam', 'Kunicki')");
      statement.addBatch("INSERT INTO TEST.TEST_TABLE VALUES (2, 'Jon', 'Natkins')");
      statement.addBatch("INSERT INTO TEST.TEST_TABLE VALUES (3, 'Jon', 'Daulton')");
      statement.addBatch("INSERT INTO TEST.TEST_TABLE VALUES (4, 'Girish', 'Pancha')");
      statement.addBatch("INSERT INTO TEST.TEST_ARRAY VALUES (1, (1,2,3))");
      statement.addBatch("INSERT INTO TEST.TEST_LOB VALUES  (1, 'short string for clob', RAWTOHEX('blob_val'))");
      statement.addBatch("INSERT INTO TEST.TEST_LOB VALUES  (2, 'long string for clob" +
          RandomStringUtils.randomAlphanumeric(CLOB_SIZE) + "', RAWTOHEX('blob_val" +
          RandomStringUtils.randomAlphanumeric(CLOB_SIZE) + "'))");
      statement.addBatch("INSERT INTO TEST.TEST_JDBC_NS_HEADERS VALUES  (1, 1.5)");
      statement.addBatch("INSERT INTO TEST.TEST_NULL VALUES  (1, NULL, NULL, NULL)");
      statement.addBatch("INSERT INTO TEST.TEST_TIMES VALUES  (1, '1993-09-01', '15:09:02', '1960-01-01 23:03:20')");
      statement.addBatch("CREATE ALIAS STOREDPROC FOR \"" + TestJdbcSource.class.getCanonicalName() + ".simpleResultSet\"");
      statement.addBatch("INSERT INTO TEST.TEST_UUID VALUES  (1, '80d00b8a-ffa3-45c2-93ba-d4278408552f')");
      statement.addBatch("INSERT INTO TEST.TEST_UNKNOWN_TYPE VALUES  (1, 'POINT (30 10)')");
      statement.addBatch("INSERT INTO TEST.TEST_UNKNOWN_TYPE VALUES  (2, null)");
      statement.addBatch("INSERT INTO TEST.TIMESTAMP_9 VALUES (1, '2018-08-09 19:21:36.992415')");
      statement.addBatch("INSERT INTO TEST.TIMESTAMP_9 VALUES (2, null)");
      statement.executeBatch();
    }
  }

  @After
  public void tearDown() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      // Setup table
      statement.execute("DROP TABLE IF EXISTS TEST.TEST_TABLE;");
      statement.execute("DROP TABLE IF EXISTS TEST.TEST_ARRAY;");
      statement.execute("DROP TABLE IF EXISTS TEST.TEST_LOB;");
      statement.execute("DROP TABLE IF EXISTS TEST.TEST_JDBC_NS_HEADERS;");
      statement.execute("DROP TABLE IF EXISTS TEST.TEST_NULL");
      statement.execute("DROP TABLE IF EXISTS TEST.TEST_TIMES");
      statement.execute("DROP ALIAS IF EXISTS STOREDPROC");
      statement.execute("DROP TABLE IF EXISTS TEST.TEST_UUID");
      statement.execute("DROP TABLE IF EXISTS TEST.TEST_UNKNOWN_TYPE");
      statement.execute("DROP TABLE IF EXISTS TEST.TIMESTAMP_9");
    }

    // Last open connection terminates H2
    connection.close();
  }

  private JdbcHikariPoolConfigBean createConfigBean(String connectionString, String username, String password) {
    JdbcHikariPoolConfigBean bean = Mockito.mock(JdbcHikariPoolConfigBean.class);
    bean.connection = new JdbcConnection();
    bean.connection.connectionString = connectionString;
    bean.connection.useCredentials = true;
    bean.connection.username = () -> username;
    bean.connection.password = () -> password;

    return bean;
  }

  @Test
  public void testIncrementalMode() throws Exception {
    JdbcSource origin = new JdbcSource(
        true,
        query,
        initialOffset,
        "P_ID",
        false,
        "",
        1000,
        JdbcRecordType.LIST_MAP,
        new CommonSourceConfigBean(queriesPerSecond, BATCH_SIZE, CLOB_SIZE, CLOB_SIZE),
        false,
        "",
        createConfigBean(h2ConnectionString, username, password),
        UnknownTypeAction.STOP_PIPELINE,
        queryInterval
    );
    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    runner.runInit();

    try {
      // Check that existing rows are loaded.
      StageRunner.Output output = runner.runProduce(null, 2);
      Map<String, List<Record>> recordMap = output.getRecords();
      List<Record> parsedRecords = recordMap.get("lane");

      assertEquals(2, parsedRecords.size());

      assertEquals("2", output.getNewOffset());

      // Check that the remaining rows in the initial cursor are read.
      output = runner.runProduce(output.getNewOffset(), 100);
      parsedRecords = output.getRecords().get("lane");
      assertEquals(2, parsedRecords.size());


      // Check that new rows are loaded.
      runInsertNewRows();
      output = runner.runProduce(output.getNewOffset(), 100);
      parsedRecords = output.getRecords().get("lane");
      assertEquals(2, parsedRecords.size());

      assertEquals("10", output.getNewOffset());

      // Check that older rows are not loaded.
      runInsertOldRows();
      output = runner.runProduce(output.getNewOffset(), 100);
      parsedRecords = output.getRecords().get("lane");
      assertEquals(0, parsedRecords.size());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testNonIncrementalMode() throws Exception {
    JdbcSource origin = new JdbcSource(
        false,
        queryNonIncremental,
        "",
        "",
        false,
        "",
        1000,
        JdbcRecordType.LIST_MAP,
        new CommonSourceConfigBean(queriesPerSecond, BATCH_SIZE, CLOB_SIZE, CLOB_SIZE),
        false,
        "",
        createConfigBean(h2ConnectionString, username, password),
        UnknownTypeAction.STOP_PIPELINE,
        queryInterval
    );
    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    runner.runInit();

    try {
      // Check that existing rows are loaded.
      StageRunner.Output output = runner.runProduce(null, 2);
      Map<String, List<Record>> recordMap = output.getRecords();
      List<Record> parsedRecords = recordMap.get("lane");

      assertEquals(2, parsedRecords.size());

      assertEquals("", output.getNewOffset());

      // Check that the remaining rows in the initial cursor are read.
      output = runner.runProduce(output.getNewOffset(), 100);
      parsedRecords = output.getRecords().get("lane");
      assertEquals(2, parsedRecords.size());


      // Check that new rows are loaded.
      runInsertNewRows();
      output = runner.runProduce(output.getNewOffset(), 100);
      parsedRecords = output.getRecords().get("lane");
      assertEquals(6, parsedRecords.size());

      assertEquals("", output.getNewOffset());

      // Check that older rows are loaded.
      runInsertOldRows();
      output = runner.runProduce(output.getNewOffset(), 100);
      parsedRecords = output.getRecords().get("lane");
      assertEquals(8, parsedRecords.size());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testStoredProcedure() throws Exception {
    JdbcSource origin = new JdbcSource(
        false,
        queryStoredProcedure,
        initialOffset,
        "ID",
        false,
        "",
        1000,
        JdbcRecordType.LIST_MAP,
        new CommonSourceConfigBean(queriesPerSecond, BATCH_SIZE, CLOB_SIZE, CLOB_SIZE),
        false,
        "",
        createConfigBean(h2ConnectionString, username, password),
        UnknownTypeAction.STOP_PIPELINE,
        queryInterval
    );
    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    runner.runInit();

    try {
      // Check that existing rows are loaded.
      StageRunner.Output output = runner.runProduce(null, 2);
      Map<String, List<Record>> recordMap = output.getRecords();
      List<Record> parsedRecords = recordMap.get("lane");

      assertEquals(2, parsedRecords.size());
      assertEquals(initialOffset, output.getNewOffset());

      assertEquals(0, parsedRecords.get(0).get("/ID").getValueAsLong());
      assertEquals(1, parsedRecords.get(1).get("/ID").getValueAsLong());

      assertEquals("San Francisco", parsedRecords.get(0).get("/NAME").getValueAsString());
      assertEquals("Brno", parsedRecords.get(1).get("/NAME").getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }

  private void runInsertNewRows() throws SQLException {
    try (Connection connection = DriverManager.getConnection(h2ConnectionString, username, password)) {
      try (Statement statement = connection.createStatement()) {
        // Add some data
        statement.addBatch("INSERT INTO TEST.TEST_TABLE VALUES (9, 'Arvind', 'Prabhakar')");
        statement.addBatch("INSERT INTO TEST.TEST_TABLE VALUES (10, 'Brock', 'Noland')");

        statement.executeBatch();
      }
    }
  }

  @Test
  public void testDriverNotFound() throws Exception {
    JdbcSource origin = new JdbcSource(
        true,
        query,
        initialOffset,
        "P_ID",
        false,
        "",
        1000,
        JdbcRecordType.LIST_MAP,
        new CommonSourceConfigBean(queriesPerSecond, BATCH_SIZE, CLOB_SIZE, CLOB_SIZE),
        false,
        "",
        createConfigBean("some bad connection string", username, password),
        UnknownTypeAction.STOP_PIPELINE,
        queryInterval
    );

    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1, issues.size());
  }

  @Test
  public void testBadConnectionString() throws Exception {
    // Want to get the H2 driver, but give it a bad connection string, testing fix for SDC-5025
    HikariPoolConfigBean configBean = createConfigBean("some bad connection string", username, password);
    configBean.driverClassName = "org.h2.Driver";

    JdbcSource origin = new JdbcSource(
        true,
        query,
        initialOffset,
        "P_ID",
        false,
        "",
        1000,
        JdbcRecordType.LIST_MAP,
        new CommonSourceConfigBean(queriesPerSecond, BATCH_SIZE, CLOB_SIZE, CLOB_SIZE),
        false,
        "",
        configBean,
        UnknownTypeAction.STOP_PIPELINE,
				queryInterval
    );

    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1, issues.size());
  }

  @Test
  public void testMissingWhereClause() throws Exception {
    String queryMissingWhere = "SELECT * FROM TEST.TEST_TABLE ORDER BY P_ID ASC LIMIT 10;";
    JdbcSource origin = new JdbcSource(
        true,
        queryMissingWhere,
        initialOffset,
        "P_ID",
        false,
        "",
        1000,
        JdbcRecordType.LIST_MAP,
        new CommonSourceConfigBean(queriesPerSecond, BATCH_SIZE, CLOB_SIZE, CLOB_SIZE),
        false,
        "",
        createConfigBean(h2ConnectionString, username, password),
        UnknownTypeAction.STOP_PIPELINE,
				queryInterval
    );

    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1, issues.size());
    assertTrue(issues.get(0).toString().contains("Query must include 'WHERE' clause."));
  }

  @Test
  public void testMissingOrderByClause() throws Exception {
    String queryMissingOrderBy = "SELECT * FROM TEST.TEST_TABLE WHERE P_ID > ${offset} LIMIT 10;";
    JdbcSource origin = new JdbcSource(
        true,
        queryMissingOrderBy,
        initialOffset,
        "P_ID",
        false,
        "",
        1000,
        JdbcRecordType.LIST_MAP,
        new CommonSourceConfigBean(queriesPerSecond, BATCH_SIZE, CLOB_SIZE, CLOB_SIZE),
        false,
        "",
        createConfigBean(h2ConnectionString, username, password),
        UnknownTypeAction.STOP_PIPELINE,
				queryInterval
    );

    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    for (Stage.ConfigIssue issue : issues) {
      LOG.info(issue.toString());
    }
    assertEquals(1, issues.size());
    assertTrue(issues.get(0).toString().contains("Query must include 'ORDER BY' clause."));
  }

  @Test
  public void testMissingWhereAndOrderByClause() throws Exception {
    String queryMissingWhereAndOrderBy = "SELECT * FROM TEST.TEST_TABLE;";
    JdbcSource origin = new JdbcSource(
        true,
        queryMissingWhereAndOrderBy,
        initialOffset,
        "P_ID",
        false,
        "",
        1000,
        JdbcRecordType.LIST_MAP,
        new CommonSourceConfigBean(queriesPerSecond, BATCH_SIZE, CLOB_SIZE, CLOB_SIZE),
        false,
        "",
        createConfigBean(h2ConnectionString, username, password),
        UnknownTypeAction.STOP_PIPELINE,
				queryInterval
    );

    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(2, issues.size());
  }

  @Test
  public void testInvalidQuery() throws Exception {
    String queryInvalid = "SELET * FORM TABLE WHERE P_ID > ${offset} ORDER BY P_ID LIMIT 10;";
    JdbcSource origin = new JdbcSource(
        true,
        queryInvalid,
        initialOffset,
        "P_ID",
        false,
        "",
        1000,
        JdbcRecordType.LIST_MAP,
        new CommonSourceConfigBean(queriesPerSecond, BATCH_SIZE, CLOB_SIZE, CLOB_SIZE),
        false,
        "",
        createConfigBean(h2ConnectionString, username, password),
        UnknownTypeAction.STOP_PIPELINE,
				queryInterval
    );

    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1, issues.size());
  }

  @Test
  public void testMultiLineQuery() throws Exception {
    String queryInvalid = "SELECT * FROM TEST.TEST_TABLE WHERE\nP_ID > ${offset}\nORDER BY P_ID ASC LIMIT 10;";
    JdbcSource origin = new JdbcSource(
        true,
        queryInvalid,
        initialOffset,
        "P_ID",
        false,
        "",
        1000,
        JdbcRecordType.LIST_MAP,
        new CommonSourceConfigBean(queriesPerSecond, BATCH_SIZE, CLOB_SIZE, CLOB_SIZE),
        false,
        "",
        createConfigBean(h2ConnectionString, username, password),
        UnknownTypeAction.STOP_PIPELINE,
				queryInterval
    );

    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(0, issues.size());
  }

  private void runInsertOldRows() throws SQLException {
    try (Connection connection = DriverManager.getConnection(h2ConnectionString, username, password)) {
      try (Statement statement = connection.createStatement()) {
        // Add some data
        statement.addBatch("INSERT INTO TEST.TEST_TABLE VALUES (5, 'Arvind', 'Prabhakar')");
        statement.addBatch("INSERT INTO TEST.TEST_TABLE VALUES (6, 'Brock', 'Noland')");

        statement.executeBatch();
      }
    }
  }

  @Test
  public void testCdcMode() throws Exception {
    JdbcSource origin = new JdbcSource(
        true,
        query,
        "1",
        "P_ID",
        false,
        "FIRST_NAME",
        1000,
        JdbcRecordType.LIST_MAP,
        new CommonSourceConfigBean(queriesPerSecond, BATCH_SIZE, CLOB_SIZE, CLOB_SIZE),
        false,
        "",
        createConfigBean(h2ConnectionString, username, password),
        UnknownTypeAction.STOP_PIPELINE,
				queryInterval
    );
    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    runner.runInit();

    try {
      // Check that existing rows are loaded.
      StageRunner.Output output = runner.runProduce(null, 1000);
      Map<String, List<Record>> recordMap = output.getRecords();
      List<Record> parsedRecords = recordMap.get("lane");

      assertEquals(2, parsedRecords.size());

      assertEquals("3", output.getNewOffset());

      // Check that the next 'transaction' of 1 row is read.
      output = runner.runProduce(output.getNewOffset(), 1000);
      parsedRecords = output.getRecords().get("lane");
      assertEquals(1, parsedRecords.size());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testCdcSplitTransactionMode() throws Exception {
    JdbcSource origin = new JdbcSource(
        true,
        query,
        "1",
        "P_ID",
        false,
        "FIRST_NAME",
        1,
        JdbcRecordType.LIST_MAP,
        new CommonSourceConfigBean(queriesPerSecond, BATCH_SIZE, CLOB_SIZE, CLOB_SIZE),
        false,
        "",
        createConfigBean(h2ConnectionString, username, password),
        UnknownTypeAction.STOP_PIPELINE,
				queryInterval
    );
    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    runner.runInit();

    try {
      // Check that existing rows are loaded.
      StageRunner.Output output = runner.runProduce(null, 1000);
      Map<String, List<Record>> recordMap = output.getRecords();
      List<Record> parsedRecords = recordMap.get("lane");

      assertEquals(1, parsedRecords.size());
      assertEquals("2", output.getNewOffset());

      // Check that the next 'transaction' of 1 row is read.
      output = runner.runProduce(output.getNewOffset(), 1000);
      parsedRecords = output.getRecords().get("lane");

      assertEquals(1, parsedRecords.size());
      assertEquals("3", output.getNewOffset());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testQualifiedOffsetColumnInQuery() throws Exception {
    final String query = "SELECT * FROM TEST.TEST_TABLE T WHERE T.P_ID > ${offset} ORDER BY T.P_ID ASC LIMIT 10;";

    JdbcSource origin = new JdbcSource(
        true,
        query,
        "1",
        "P_ID",
        false,
        "FIRST_NAME",
        1,
        JdbcRecordType.LIST_MAP,
        new CommonSourceConfigBean(queriesPerSecond, BATCH_SIZE, CLOB_SIZE, CLOB_SIZE),
        false,
        "",
        createConfigBean(h2ConnectionString, username, password),
        UnknownTypeAction.STOP_PIPELINE,
				queryInterval
    );
    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(0, issues.size());
  }

  @Test
  public void testDuplicateColumnLabels() throws Exception {
    final String query = "SELECT * FROM TEST.TEST_TABLE T, TEST.TEST_TABLE TB WHERE T.P_ID > ${offset} " +
        "ORDER BY T.P_ID ASC LIMIT 10;";

    JdbcSource origin = new JdbcSource(
        true,
        query,
        "1",
        "P_ID",
        false,
        "FIRST_NAME",
        1,
        JdbcRecordType.LIST_MAP,
        new CommonSourceConfigBean(queriesPerSecond, BATCH_SIZE, CLOB_SIZE, CLOB_SIZE),
        false,
        "",
        createConfigBean(h2ConnectionString, username, password),
        UnknownTypeAction.STOP_PIPELINE,
				queryInterval
    );
    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(3, issues.size());
  }

  @Test
  public void testPrefixedOffsetColumn() throws Exception {
    final String query = "SELECT * FROM TEST.TEST_TABLE T WHERE T.P_ID > ${offset} ORDER BY T.P_ID ASC LIMIT 10;";

    JdbcSource origin = new JdbcSource(
        true,
        query,
        "1",
        "T.P_ID",
        false,
        "FIRST_NAME",
        1,
        JdbcRecordType.LIST_MAP,
        new CommonSourceConfigBean(queriesPerSecond, BATCH_SIZE, CLOB_SIZE, CLOB_SIZE),
        false,
        "",
        createConfigBean(h2ConnectionString, username, password),
        UnknownTypeAction.STOP_PIPELINE,
				queryInterval
    );
    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1, issues.size());
  }

  @Test
  public void testEmptyResultSet() throws Exception {
    Statement statement = connection.createStatement();
    statement.execute("TRUNCATE TABLE TEST.TEST_TABLE");

    JdbcSource origin = new JdbcSource(
        true,
        query,
        initialOffset,
        "P_ID",
        false,
        "FIRST_NAME",
        1000,
        JdbcRecordType.LIST_MAP,
        new CommonSourceConfigBean(queriesPerSecond, BATCH_SIZE, CLOB_SIZE, CLOB_SIZE),
        false,
        "",
        createConfigBean(h2ConnectionString, username, password),
        UnknownTypeAction.STOP_PIPELINE,
				queryInterval
    );
    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    runner.runInit();

    try {
      // Check that existing rows are loaded.
      StageRunner.Output output = runner.runProduce(null, 1000);
      Map<String, List<Record>> recordMap = output.getRecords();
      List<Record> parsedRecords = recordMap.get("lane");

      assertEquals(0, parsedRecords.size());

      assertEquals(initialOffset, output.getNewOffset());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testLobColumns() throws Exception {
    String queryClob = "SELECT * FROM TEST.TEST_LOB WHERE P_ID > ${offset} ORDER BY P_ID ASC LIMIT 10;";
    JdbcSource origin = new JdbcSource(
        true,
        queryClob,
        initialOffset,
        "P_ID",
        false,
        "",
        1000,
        JdbcRecordType.LIST_MAP,
        new CommonSourceConfigBean(queriesPerSecond, BATCH_SIZE, CLOB_SIZE, CLOB_SIZE),
        false,
        "",
        createConfigBean(h2ConnectionString, username, password),
        UnknownTypeAction.STOP_PIPELINE,
				queryInterval
    );

    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    runner.runInit();

    try {
      // Check that existing rows are loaded.
      StageRunner.Output output = runner.runProduce(null, 1000);
      Map<String, List<Record>> recordMap = output.getRecords();
      List<Record> parsedRecords = recordMap.get("lane");

      assertEquals(2, parsedRecords.size());

      assertEquals("2", output.getNewOffset());

      // First record is shorter than CLOB_SIZE, so it must be as is.
      assertEquals("short string for clob", parsedRecords.get(0).get("/CLOB_COL").getValueAsString());
      assertEquals("blob_val", IOUtils.toString(parsedRecords.get(0).get("/BLOB_COL").getValueAsByteArray(), "UTF-16"));

      // Second record is longer than CLOB_SIZE, so it must be truncated.
      assertEquals(CLOB_SIZE, parsedRecords.get(1).get("/CLOB_COL").getValueAsString().length());
      assertEquals(CLOB_SIZE, parsedRecords.get(1).get("/BLOB_COL").getValueAsByteArray().length);
      assertTrue(parsedRecords.get(1).get("/CLOB_COL").getValueAsString().startsWith("long string for clob"));
      assertTrue(IOUtils.toString(parsedRecords.get(0).get("/BLOB_COL").getValueAsByteArray(), "UTF-16").startsWith("blob_val"));
    } finally {
      runner.runDestroy();
    }
  }
  @Test
  public void testJDBCNsHeaders() throws Exception{
    String queryDecimal = "SELECT * from TEST.TEST_JDBC_NS_HEADERS T WHERE T.P_ID > ${offset} ORDER BY T.P_ID ASC LIMIT 10;";
    JdbcSource origin = new JdbcSource(
        true,
        queryDecimal,
        initialOffset,
        "P_ID",
        false,
        "",
        1000,
        JdbcRecordType.LIST_MAP,
        new CommonSourceConfigBean(queriesPerSecond, BATCH_SIZE, CLOB_SIZE, CLOB_SIZE),
        true,
        "jdbc.",
        createConfigBean(h2ConnectionString, username, password),
        UnknownTypeAction.STOP_PIPELINE,
				queryInterval
    );

    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    runner.runInit();

    try {
      // Check that existing rows are loaded.
      StageRunner.Output output = runner.runProduce(null, 1000);
      Map<String, List<Record>> recordMap = output.getRecords();
      List<Record> parsedRecords = recordMap.get("lane");

      assertEquals(1, parsedRecords.size());
      assertEquals("1", output.getNewOffset());

      Record parsedRecord = parsedRecords.get(0);
      assertEquals(new BigDecimal(1.5), parsedRecord.get("/DEC").getValueAsDecimal());
      assertEquals("1", parsedRecord.getHeader().getAttribute("jdbc.DEC.scale"));
      assertEquals("2", parsedRecord.getHeader().getAttribute("jdbc.DEC.precision"));
      assertEquals(String.valueOf(Types.DECIMAL), parsedRecord.getHeader().getAttribute("jdbc.DEC.jdbcType"));
      assertEquals("TEST_JDBC_NS_HEADERS", parsedRecord.getHeader().getAttribute("jdbc.tables"));

      Field decimalField = parsedRecord.get("/DEC");
      assertEquals("1", decimalField.getAttribute(HeaderAttributeConstants.ATTR_SCALE));
      assertEquals("2", decimalField.getAttribute(HeaderAttributeConstants.ATTR_PRECISION));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testImportingNull() throws Exception {
    String query = "SELECT * from TEST.TEST_NULL T WHERE T.P_ID > ${offset} ORDER BY T.P_ID ASC LIMIT 10;";
    JdbcSource origin = new JdbcSource(
        true,
        query,
        initialOffset,
        "P_ID",
        false,
        "",
        1000,
        JdbcRecordType.LIST_MAP,
        new CommonSourceConfigBean(queriesPerSecond, BATCH_SIZE, CLOB_SIZE, CLOB_SIZE),
        false,
        null,
        createConfigBean(h2ConnectionString, username, password),
        UnknownTypeAction.STOP_PIPELINE,
				queryInterval
    );

    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    runner.runInit();

    try {
      // Check that existing rows are loaded.
      StageRunner.Output output = runner.runProduce(null, 1000);
      Map<String, List<Record>> recordMap = output.getRecords();
      List<Record> parsedRecords = recordMap.get("lane");

      assertEquals(1, parsedRecords.size());
      assertEquals("1", output.getNewOffset());

      Record parsedRecord = parsedRecords.get(0);
      assertTrue(parsedRecord.has("/NAME"));
      assertEquals(Field.Type.STRING, parsedRecord.get("/NAME").getType());
      assertNull(parsedRecord.get("/NAME").getValue());

      assertTrue(parsedRecord.has("/NUMBER"));
      assertEquals(Field.Type.INTEGER, parsedRecord.get("/NUMBER").getType());
      assertNull(parsedRecord.get("/NUMBER").getValue());

      assertTrue(parsedRecord.has("/TS"));
      assertEquals(Field.Type.DATETIME, parsedRecord.get("/TS").getType());
      assertNull(parsedRecord.get("/TS").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testTimeTypes() throws Exception {
    String query = "SELECT * from TEST.TEST_TIMES T WHERE T.P_ID > ${offset} ORDER BY T.P_ID ASC LIMIT 10;";
    JdbcSource origin = new JdbcSource(
        true,
        query,
        initialOffset,
        "P_ID",
        false,
        "",
        1000,
        JdbcRecordType.LIST_MAP,
        new CommonSourceConfigBean(queriesPerSecond, BATCH_SIZE, CLOB_SIZE, CLOB_SIZE),
        false,
        null,
        createConfigBean(h2ConnectionString, username, password),
        UnknownTypeAction.STOP_PIPELINE,
				queryInterval
    );

    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    runner.runInit();

    try {
      // Check that existing rows are loaded.
      StageRunner.Output output = runner.runProduce(null, 1000);
      Map<String, List<Record>> recordMap = output.getRecords();
      List<Record> parsedRecords = recordMap.get("lane");

      assertEquals(1, parsedRecords.size());
      assertEquals("1", output.getNewOffset());

      Record parsedRecord = parsedRecords.get(0);
      assertTrue(parsedRecord.has("/D"));
      assertEquals(Field.Type.DATE, parsedRecord.get("/D").getType());

      assertTrue(parsedRecord.has("/T"));
      assertEquals(Field.Type.TIME, parsedRecord.get("/T").getType());

      assertTrue(parsedRecord.has("/TS"));
      assertEquals(Field.Type.DATETIME, parsedRecord.get("/TS").getType());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testUUID() throws Exception {
    JdbcSource origin = new JdbcSource(
        false,
        "SELECT * FROM TEST.TEST_UUID",
        "",
        "",
        false,
        "",
        1000,
        JdbcRecordType.LIST_MAP,
        new CommonSourceConfigBean(queriesPerSecond, BATCH_SIZE, CLOB_SIZE, CLOB_SIZE),
        false,
        "",
        createConfigBean(h2ConnectionString, username, password),
        UnknownTypeAction.STOP_PIPELINE,
				queryInterval
        );
    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    runner.runInit();

    try {
      // Check that existing rows are loaded.
      StageRunner.Output output = runner.runProduce(null, 2);
      Map<String, List<Record>> recordMap = output.getRecords();
      List<Record> parsedRecords = recordMap.get("lane");

      assertEquals(1, parsedRecords.size());
      Record record = parsedRecords.get(0);
      assertNotNull(record);
      assertTrue(record.has("/P_UUID"));
      Field field = record.get("/P_UUID");
      assertNotNull(field);
      assertEquals(Field.Type.BYTE_ARRAY, field.getType());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testNoMoreDataEventIncremental() throws Exception {
    JdbcSource origin = new JdbcSource(
        true,
        query,
        "0",
        "P_ID",
        false,
        "",
        1000,
        JdbcRecordType.LIST_MAP,
        new CommonSourceConfigBean(queriesPerSecond, BATCH_SIZE, CLOB_SIZE, CLOB_SIZE),
        false,
        "",
        createConfigBean(h2ConnectionString, username, password),
        UnknownTypeAction.STOP_PIPELINE,
				queryInterval
        );
    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    runner.runInit();

    try {
      StageRunner.Output output;

      // First batch should read all 4 records
      output = runner.runProduce(null, 10);
      Assert.assertEquals(4, output.getRecords().get("lane").size());
      Assert.assertEquals(1, runner.getEventRecords().size());
      Assert.assertEquals("jdbc-query-success", runner.getEventRecords().get(0).getEventType());
      Assert.assertEquals(4, runner.getEventRecords().get(0).get("/rows").getValueAsLong());
      runner.clearEvents();

      // Second batch should generate empty batch and run "empty" query thus triggering the no-more-data event
      output = runner.runProduce(output.getNewOffset(), 10);
      Assert.assertEquals(0, output.getRecords().get("lane").size());
      Assert.assertEquals(2, runner.getEventRecords().size());
      Assert.assertEquals("jdbc-query-success", runner.getEventRecords().get(0).getEventType());
      Assert.assertEquals(0, runner.getEventRecords().get(0).get("/rows").getValueAsLong());
      Assert.assertEquals("no-more-data", runner.getEventRecords().get(1).getEventType());
      Assert.assertEquals(4, runner.getEventRecords().get(1).get("/record-count").getValueAsLong());
      runner.clearEvents();

      // Third batch will also generate empty batch, but this thime should not trigger no-more-data event
      output = runner.runProduce(output.getNewOffset(), 10);
      Assert.assertEquals(0, output.getRecords().get("lane").size());
      Assert.assertEquals(1, runner.getEventRecords().size());
      Assert.assertEquals("jdbc-query-success", runner.getEventRecords().get(0).getEventType());
      Assert.assertEquals(0, runner.getEventRecords().get(0).get("/rows").getValueAsLong());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testNoMoreDataEventNonIncremental() throws Exception {
    JdbcSource origin = new JdbcSource(
        false,
        queryNonIncremental,
        "0",
        "P_ID",
        false,
        "",
        1000,
        JdbcRecordType.LIST_MAP,
        new CommonSourceConfigBean(queriesPerSecond, BATCH_SIZE, CLOB_SIZE, CLOB_SIZE),
        false,
        "",
        createConfigBean(h2ConnectionString, username, password),
        UnknownTypeAction.STOP_PIPELINE,
				queryInterval
        );
    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    runner.runInit();

    try {
      StageRunner.Output output;

      // First batch will read the first 2 rows
      output = runner.runProduce(null, 2);
      Assert.assertEquals(2, output.getRecords().get("lane").size());
      Assert.assertEquals(0, runner.getEventRecords().size());
      runner.clearEvents();

      // Second batch will read the rest and generate both success and no-more-data events
      output = runner.runProduce(output.getNewOffset(), 10);
      Assert.assertEquals(2, output.getRecords().get("lane").size());
      Assert.assertEquals(2, runner.getEventRecords().size());
      Assert.assertEquals("jdbc-query-success", runner.getEventRecords().get(0).getEventType());
      Assert.assertEquals(4, runner.getEventRecords().get(0).get("/rows").getValueAsLong());
      Assert.assertEquals("no-more-data", runner.getEventRecords().get(1).getEventType());
      Assert.assertEquals(4, runner.getEventRecords().get(1).get("/record-count").getValueAsLong());
      runner.clearEvents();

      // Third batch will start reading from beginning since it's non-incremental mode and we'll read ll the records
      // and hence another no-more-data will be generated.
      output = runner.runProduce(output.getNewOffset(), 10);
      Assert.assertEquals(4, output.getRecords().get("lane").size());
      Assert.assertEquals(2, runner.getEventRecords().size());
      Assert.assertEquals("jdbc-query-success", runner.getEventRecords().get(0).getEventType());
      Assert.assertEquals(4, runner.getEventRecords().get(0).get("/rows").getValueAsLong());
      Assert.assertEquals("no-more-data", runner.getEventRecords().get(1).getEventType());
      Assert.assertEquals(4, runner.getEventRecords().get(1).get("/record-count").getValueAsLong());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testNoMoreDataEventWhenNoRowsNonIncremental() throws Exception {
    Statement statement = connection.createStatement();
    statement.execute("TRUNCATE TABLE TEST.TEST_TABLE");

    JdbcSource origin = new JdbcSource(
        false,
        queryNonIncremental,
        "0",
        "P_ID",
        false,
        "",
        1000,
        JdbcRecordType.LIST_MAP,
        new CommonSourceConfigBean(queriesPerSecond, BATCH_SIZE, CLOB_SIZE, CLOB_SIZE),
        false,
        "",
        createConfigBean(h2ConnectionString, username, password),
        UnknownTypeAction.STOP_PIPELINE,
				queryInterval
    );

    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    runner.runInit();

    try {
      StageRunner.Output output;

      // First batch will produce no data rows
      output = runner.runProduce(null, 2);

      Assert.assertEquals(0, output.getRecords().get("lane").size());     // no records.
      Assert.assertEquals(2, runner.getEventRecords().size());            // two events.

      Assert.assertEquals("jdbc-query-success", runner.getEventRecords().get(0).getEventType());
      Assert.assertEquals(0, runner.getEventRecords().get(0).get("/rows").getValueAsLong());
      Assert.assertEquals("no-more-data", runner.getEventRecords().get(1).getEventType());
      Assert.assertEquals(0, runner.getEventRecords().get(1).get("/record-count").getValueAsLong());

      runner.clearEvents();

      // Second batch will start reading from beginning since it's non-incremental mode and we'll read no
      // records and hence another no-more-data will be generated.
      output = runner.runProduce(output.getNewOffset(), 10);
      Assert.assertEquals(0, output.getRecords().get("lane").size());
      Assert.assertEquals(2, runner.getEventRecords().size());
      Assert.assertEquals("jdbc-query-success", runner.getEventRecords().get(0).getEventType());
      Assert.assertEquals(0, runner.getEventRecords().get(0).get("/rows").getValueAsLong());
      Assert.assertEquals("no-more-data", runner.getEventRecords().get(1).getEventType());
      Assert.assertEquals(0, runner.getEventRecords().get(1).get("/record-count").getValueAsLong());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testNoMoreDataEventWhenNoRowsIncremental() throws Exception {
    Statement statement = connection.createStatement();
    statement.execute("TRUNCATE TABLE TEST.TEST_TABLE");

    JdbcSource origin = new JdbcSource(
        true,
        query,
        "0",
        "P_ID",
        false,
        "",
        1000,
        JdbcRecordType.LIST_MAP,
        new CommonSourceConfigBean(queriesPerSecond, BATCH_SIZE, CLOB_SIZE, CLOB_SIZE),
        false,
        "",
        createConfigBean(h2ConnectionString, username, password),
        UnknownTypeAction.STOP_PIPELINE,
				queryInterval
    );

    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    runner.runInit();

    try {
      StageRunner.Output output;

      output = runner.runProduce(null, 10);
      // First batch will read no data rows
      Assert.assertEquals(0, output.getRecords().get("lane").size());

      // First batch's event is successful query.
      Assert.assertEquals(1, runner.getEventRecords().size());
      Assert.assertEquals("jdbc-query-success", runner.getEventRecords().get(0).getEventType());
      Assert.assertEquals(0, runner.getEventRecords().get(0).get("/rows").getValueAsLong());

      runner.clearEvents();
      output = runner.runProduce(output.getNewOffset(), 10);
      // second batch will return no data rows.
      Assert.assertEquals(0, output.getRecords().get("lane").size());

      // Second batch's events are successful query and no-more-data.
      Assert.assertEquals(2, runner.getEventRecords().size());
      Assert.assertEquals("jdbc-query-success", runner.getEventRecords().get(0).getEventType());
      Assert.assertEquals(0, runner.getEventRecords().get(0).get("/rows").getValueAsLong());
      Assert.assertEquals("no-more-data", runner.getEventRecords().get(1).getEventType());
      Assert.assertEquals(0, runner.getEventRecords().get(1).get("/record-count").getValueAsLong());

      runner.clearEvents();
      output = runner.runProduce(output.getNewOffset(), 10);
      // Third batch will return no data rows.
      Assert.assertEquals(0, output.getRecords().get("lane").size());

      // Third batch's event is successful query.
      Assert.assertEquals(1, runner.getEventRecords().size());
      Assert.assertEquals("jdbc-query-success", runner.getEventRecords().get(0).getEventType());
      Assert.assertEquals(0, runner.getEventRecords().get(0).get("/rows").getValueAsLong());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testLineageEvent() throws Exception {
    JdbcSource origin = new JdbcSource(
        true,
        query,
        "0",
        "P_ID",
        false,
        "",
        1000,
        JdbcRecordType.LIST_MAP,
        // Using "0" leads to SDC-6429
        new CommonSourceConfigBean("0.1", BATCH_SIZE, CLOB_SIZE, CLOB_SIZE),
        false,
        "",
        createConfigBean(h2ConnectionString, username, password),
        UnknownTypeAction.STOP_PIPELINE,
				queryInterval
    );
    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    runner.runInit();

    try {
      StageRunner.Output output;

      // First batch should read all 4 records
      output = runner.runProduce(null, 10);
      Assert.assertEquals(4, output.getRecords().get("lane").size());
      Assert.assertEquals(1, runner.getEventRecords().size());
      Assert.assertEquals("jdbc-query-success", runner.getEventRecords().get(0).getEventType());
      Assert.assertEquals(4, runner.getEventRecords().get(0).get("/rows").getValueAsLong());
      List<LineageEvent> events = runner.getLineageEvents();

      Assert.assertEquals(1, events.size());
      Assert.assertEquals(LineageEventType.ENTITY_READ, events.get(0).getEventType());
      Assert.assertEquals(query, events.get(0).getSpecificAttribute(LineageSpecificAttribute.DESCRIPTION));
      Assert.assertEquals(EndPointType.JDBC.name(), events.get(0).getSpecificAttribute(LineageSpecificAttribute.ENDPOINT_TYPE));
      Assert.assertTrue(events.get(0).getSpecificAttribute(LineageSpecificAttribute.ENTITY_NAME).contains(h2ConnectionString));
    } finally {
      runner.runDestroy();
    }
  }

  @Test(expected = StageException.class)
  public void testUnknownTypeStopPipeline() throws Exception {
    JdbcSource origin = new JdbcSource(
        true,
        queryUnknownType,
        "0",
        "P_ID",
        false,
        "",
        1000,
        JdbcRecordType.LIST_MAP,
        // Using "0" leads to SDC-6429
        new CommonSourceConfigBean("0.1", BATCH_SIZE, CLOB_SIZE, CLOB_SIZE),
        false,
        "",
        createConfigBean(h2ConnectionString, username, password),
        UnknownTypeAction.STOP_PIPELINE,
				queryInterval
        );
    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    runner.runInit();

    try {
      StageRunner.Output output;

      // First batch should read all 4 records
      output = runner.runProduce(null, 10);
      Assert.assertEquals(1, output.getRecords().get("lane").size());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testUnknownTypeToString() throws Exception {
    JdbcSource origin = new JdbcSource(
        true,
        queryUnknownType,
        "0",
        "P_ID",
        false,
        "",
        1000,
        JdbcRecordType.LIST_MAP,
        // Using "0" leads to SDC-6429
        new CommonSourceConfigBean("0.1", BATCH_SIZE, CLOB_SIZE, CLOB_SIZE),
        false,
        "",
        createConfigBean(h2ConnectionString, username, password),
        UnknownTypeAction.CONVERT_TO_STRING,
        queryInterval
        );
    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    runner.runInit();

    try {
      StageRunner.Output output;

      // First batch should read all 4 records
      output = runner.runProduce(null, 10);
      Assert.assertEquals(2, output.getRecords().get("lane").size());

      Record record = output.getRecords().get("lane").get(0);
      Assert.assertTrue(record.has("/P_ID"));
      Assert.assertTrue(record.has("/GEO"));
      Assert.assertEquals(1, record.get("/P_ID").getValueAsLong());
      Assert.assertEquals("POINT (30 10)", record.get("/GEO").getValueAsString());

      record = output.getRecords().get("lane").get(1);
      Assert.assertTrue(record.has("/P_ID"));
      Assert.assertTrue(record.has("/GEO"));
      Assert.assertEquals(2, record.get("/P_ID").getValueAsLong());
      Assert.assertEquals(Field.Type.STRING, record.get("/GEO").getType());
      assertNull(record.get("/GEO").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testQueryReplaceUpperOffset() throws Exception {
    JdbcSource origin = new JdbcSource(
        true,
        queryUnknownType,
        "0",
        "P_ID",
        false,
        "",
        1000,
        JdbcRecordType.LIST_MAP,
        // Using "0" leads to SDC-6429
        new CommonSourceConfigBean("0.1", BATCH_SIZE, CLOB_SIZE, CLOB_SIZE),
        false,
        "",
        createConfigBean(h2ConnectionString, username, password),
        UnknownTypeAction.CONVERT_TO_STRING,
        queryInterval
    );

    final String lastSourceOffset = "10";
    final String query = "${OFFSET}${offset}";

    String result = origin.prepareQuery(query, lastSourceOffset);
    Assert.assertEquals(result, lastSourceOffset+lastSourceOffset);
  }

  @Test
  public void testTimestampAsString() throws Exception {
    JdbcSource origin = new JdbcSource(
        true,
        "SELECT * from TEST.TIMESTAMP_9 T WHERE T.P_ID > ${offset} ORDER BY T.P_ID",
        initialOffset,
        "P_ID",
        false,
        "",
        1000,
        JdbcRecordType.LIST_MAP,
        new CommonSourceConfigBean(queriesPerSecond, BATCH_SIZE, CLOB_SIZE, CLOB_SIZE, true),
        false,
        "",
        createConfigBean(h2ConnectionString, username, password),
        UnknownTypeAction.STOP_PIPELINE,
				queryInterval
    );
    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    runner.runInit();

    try {
      // Check that existing rows are loaded.
      StageRunner.Output output = runner.runProduce(null, 1000);
      Map<String, List<Record>> recordMap = output.getRecords();
      List<Record> parsedRecords = recordMap.get("lane");

      assertEquals(2, parsedRecords.size());
      assertTrue(parsedRecords.get(0).has("/TS"));
      assertEquals(Field.Type.STRING, parsedRecords.get(0).get("/TS").getType());
      assertEquals("2018-08-09 19:21:36.992415", parsedRecords.get(0).get("/TS").getValueAsString());
      assertEquals(Field.Type.STRING, parsedRecords.get(1).get("/TS").getType());
      assertNull(parsedRecords.get(1).get("/TS").getValueAsString());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testNonSecureConnection() throws Exception {
    JdbcHikariPoolConfigBean bean = new JdbcHikariPoolConfigBean();
    JdbcSource origin = new JdbcSource(
        true,
        query,
        initialOffset,
        "P_ID",
        false,
        "",
        1000,
        JdbcRecordType.LIST_MAP,
        new CommonSourceConfigBean(queriesPerSecond, BATCH_SIZE, CLOB_SIZE, CLOB_SIZE),
        false,
        "",
        bean,
        UnknownTypeAction.STOP_PIPELINE,
        queryInterval
    );

    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin).addOutputLane("lane").build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1, issues.size());
    assertTrue(issues.toString().contains(JdbcErrors.JDBC_501.toString()));
  }
}
