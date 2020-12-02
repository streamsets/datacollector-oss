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
package com.streamsets.pipeline.lib.jdbc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.streamsets.pipeline.lib.operation.UnsupportedOperationAction;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;


public class TestJDBCBaseRecordWriter {

  private final String username = "sa";
  private final String password = "sa";
  private static final String connectionString = "jdbc:h2:mem:test";
  private final Stage.Context context = ContextInfoCreator.createTargetContext("a", false, OnRecordError.STOP_PIPELINE);
  private DataSource dataSource;
  private Connection connection;

  @Before
  public void setUp() throws SQLException {
    // Create a table in H2 and put some data in it for querying.
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(connectionString);
    config.setUsername(username);
    config.setPassword(password);
    config.setMaximumPoolSize(2);
    dataSource = new HikariDataSource(config);

    connection = dataSource.getConnection();
    try (Statement statement = connection.createStatement()) {
      // Setup table
      statement.addBatch("CREATE SCHEMA IF NOT EXISTS TEST;");
      statement.addBatch(
          "CREATE TABLE IF NOT EXISTS TEST.TEST_TABLE " +
              "(P_ID INT NOT NULL, "
              + "MSG VARCHAR(255), "
              + "PRIMARY KEY(P_ID));"
      );

      statement.addBatch(
          "CREATE TABLE IF NOT EXISTS TEST.TEST_TABLE2 " +
              "(P_ID INT NOT NULL, "
              + "F_BOOL BIT ,"
              + "F_SHORT SMALLINT ,"
              + "F_INT INTEGER ,"
              + "F_LONG BIGINT ,"
              + "F_FLOAT REAL ,"
              + "F_DOUBLE DOUBLE ,"
              + "F_DATE DATE ,"
              + "F_DATETIME DATE ,"
              + "F_TIME TIME ,"
              + "F_DECIMAL DECIMAL ,"
              + "F_STR VARCHAR(255) ,"
              + "F_BYTEARRAY BINARY ,"
              + "PRIMARY KEY(P_ID));"
      );
      statement.addBatch(
          "CREATE TABLE IF NOT EXISTS TEST.COMPOSITE_KEY " +
              "(P_ID INT NOT NULL, P_IDB INT NOT NULL, MSG VARCHAR(255), PRIMARY KEY(P_ID, P_IDB));"
      );
      String unprivUser = "unpriv_user";
      String unprivPassword = "unpriv_pass";
      statement.addBatch("CREATE USER IF NOT EXISTS " + unprivUser + " PASSWORD '" + unprivPassword + "';");
      statement.addBatch("GRANT SELECT ON TEST.TEST_TABLE TO " + unprivUser + ";");

      statement.executeBatch();
    }
  }

  @After
  public void tearDown() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      // Setup table
      statement.execute("DROP TABLE IF EXISTS TEST.TEST_TABLE;");
      statement.execute("DROP TABLE IF EXISTS TEST.TEST_TABLE2;");
    }
    // Last open connection terminates H2
    connection.close();
  }

  @Test
  public void testSinglePrimaryKeys() throws StageException {
    boolean caseSensitive = false;
    JdbcBaseRecordWriter writer = new JdbcGenericRecordWriter(
        connectionString,
        dataSource,
        "TEST",
        "TEST_TABLE",
        false, //rollback
        new LinkedList<JdbcFieldColumnParamMapping>(),
        JDBCOperationType.INSERT.getCode(),
        UnsupportedOperationAction.DISCARD,
        null,
        new JdbcRecordReader(),
        caseSensitive,
        Collections.emptyList(),
        true,
        context
    );

    try {
      writer.lookupPrimaryKeys();
      List<String> primaryKeyColumns = writer.getPrimaryKeyColumns();
      Assert.assertEquals(1, primaryKeyColumns.size());
      Assert.assertEquals("P_ID", primaryKeyColumns.get(0));
    } catch (Exception ex) {
      Assert.fail("lookupPrimaryKeys failed: " + ex.getMessage());
    }
  }

  @Test
  public void testCompoundPrimaryKeys() throws StageException {
    boolean caseSensitive = false;
    JdbcBaseRecordWriter writer = new JdbcGenericRecordWriter(
        connectionString,
        dataSource,
        "TEST",
        "COMPOSITE_KEY",
        false, //rollback
        new LinkedList<JdbcFieldColumnParamMapping>(),
        JDBCOperationType.INSERT.getCode(),
        UnsupportedOperationAction.DISCARD,
        null,
        new JdbcRecordReader(),
        caseSensitive,
        Collections.emptyList(),
        true,
        context
    );

    try {
      writer.lookupPrimaryKeys();
      List<String> primaryKeyColumns = writer.getPrimaryKeyColumns();
      Assert.assertEquals(2, primaryKeyColumns.size());
      Assert.assertEquals("P_ID", primaryKeyColumns.get(0));
      Assert.assertEquals("P_IDB", primaryKeyColumns.get(1));
    } catch (Exception ex) {
      Assert.fail("lookupPrimaryKeys failed: " + ex.getMessage());
    }
  }

  @Test
  public void testSetParamsToStatement() throws StageException {
    //Test filling parameters but records has invalid values
    List<JdbcFieldColumnParamMapping> columnMapping = ImmutableList.of(
        new JdbcFieldColumnParamMapping("/field1", "P_ID", "?"),
        new JdbcFieldColumnParamMapping("/field2", "MSG", "?")
    );

    List<JdbcFieldColumnMapping> generatedColumnMapping = ImmutableList.of(
        new JdbcFieldColumnMapping("/field1", "P_ID", "", DataType.INTEGER),
        new JdbcFieldColumnMapping("/field2", "MSG", "", DataType.STRING)
    );

    boolean caseSensitive = false;
    JdbcGenericRecordWriter writer = new JdbcGenericRecordWriter(
        connectionString,
        dataSource,
        "TEST",
        "TEST_TABLE",
        false, //rollback
        columnMapping,
        JDBCOperationType.INSERT.getCode(),
        UnsupportedOperationAction.DISCARD,
        generatedColumnMapping,
        new JdbcRecordReader(),
        caseSensitive,
        Collections.emptyList(),
        true,
        context
    );
    Record record = RecordCreator.create();
    Map<String, Field> fields = new HashMap<>();
    fields.put("field1", Field.create("")); // this should cause Data conversion error
    fields.put("field2", Field.create("true"));
    record.set(Field.create(fields));
    SortedMap<String, String> columnsToParameters = ImmutableSortedMap.of(
        "P_ID", "?",
        "MSG", "?"
    );

    String query = "UPDATE TEST.TEST_TABLE SET  MSG = ? WHERE P_ID = ?";

    try {
      PreparedStatement stmt = connection.prepareStatement(query);
      writer.setParameters(OperationType.UPDATE_CODE, columnsToParameters, record, connection, stmt);
      Assert.fail("Primary key should cause data conversion error while converting empty string to int");
    } catch (OnRecordErrorException ex) {
      Assert.assertEquals(
          "Received expected data conversion error",
          true,
          ex.getErrorCode().getCode().equals(JdbcErrors.JDBC_23.name())
      );
    } catch (SQLException ex) {
      Assert.fail("Wrong SQLException:" + ex.getMessage());
    }
  }

  @Test
  public void testSetParamsToStatementUseTypes() throws StageException {
    //Test filling parameters and ensure setObject() not used
    List<JdbcFieldColumnParamMapping> columnMapping = ImmutableList.of(
        new JdbcFieldColumnParamMapping("/field2", "F_BOOL", "?"),
        new JdbcFieldColumnParamMapping("/field3", "F_SHORT", "?"),
        new JdbcFieldColumnParamMapping("/field4", "F_INT", "?"),
        new JdbcFieldColumnParamMapping("/field5", "F_LONG", "?"),
        new JdbcFieldColumnParamMapping("/field6", "F_FLOAT", "?"),
        new JdbcFieldColumnParamMapping("/field7", "F_DOUBLE", "?"),
        new JdbcFieldColumnParamMapping("/field8", "F_DATE", "?"),
        new JdbcFieldColumnParamMapping("/field9", "F_DATETIME", "?"),
        new JdbcFieldColumnParamMapping("/field10", "F_TIME", "?"),
        new JdbcFieldColumnParamMapping("/field11", "F_DECIMAL", "?"),
        new JdbcFieldColumnParamMapping("/field12", "F_STR", "?"),
        new JdbcFieldColumnParamMapping("/field13", "F_BYTEARRAY", "?")
    );

    List<JdbcFieldColumnMapping> generatedColumnMapping = ImmutableList.of(
        new JdbcFieldColumnMapping("/field2", "F_BOOL", "", DataType.BOOLEAN),
        new JdbcFieldColumnMapping("/field3", "F_SHORT", "", DataType.SHORT),
        new JdbcFieldColumnMapping("/field4", "F_INT", "", DataType.INTEGER),
        new JdbcFieldColumnMapping("/field5", "F_LONG", "", DataType.LONG),
        new JdbcFieldColumnMapping("/field6", "F_FLOAT", "", DataType.FLOAT),
        new JdbcFieldColumnMapping("/field7", "F_DOUBLE", "", DataType.DOUBLE),
        new JdbcFieldColumnMapping("/field8", "F_DATE", "", DataType.DATE),
        new JdbcFieldColumnMapping("/field9", "F_DATETIME", "", DataType.DATETIME),
        new JdbcFieldColumnMapping("/field10", "F_TIME", "", DataType.TIME),
        new JdbcFieldColumnMapping("/field11", "F_DECIMAL", "", DataType.DECIMAL),
        new JdbcFieldColumnMapping("/field12", "F_STR", "", DataType.STRING),
        new JdbcFieldColumnMapping("/field13", "F_BYTEARRAY", "", DataType.BYTE_ARRAY)
    );

    boolean caseSensitive = false;
    JdbcGenericRecordWriter writer = new JdbcGenericRecordWriter(
        connectionString,
        dataSource,
        "TEST",
        "TEST_TABLE2",
        false, //rollback
        columnMapping,
        JDBCOperationType.INSERT.getCode(),
        UnsupportedOperationAction.DISCARD,
        generatedColumnMapping,
        new JdbcRecordReader(),
        caseSensitive,
        Collections.emptyList(),
        true,
        context
    );
    Record record = RecordCreator.create();
    Map<String, Field> fields = new HashMap<>();
    byte[] bytes = {1 ,2 , 3};
    fields.put("field2", Field.create(true));
    fields.put("field3", Field.create((short) 3));
    fields.put("field4", Field.create(4)); //integer
    fields.put("field5", Field.create(Long.valueOf(5)));
    fields.put("field6", Field.create(Float.valueOf(6)));
    fields.put("field7", Field.create(Double.valueOf(7)));
    fields.put("field8", Field.createDate(new Date())); //date
    fields.put("field9", Field.createDatetime(new Date())); //datetime
    fields.put("field10", Field.createTime(new Date())); //time
    fields.put("field11", Field.create(new BigDecimal(11))); //decimal
    fields.put("field12", Field.create("field12"));
    fields.put("field13", Field.create(bytes)); //bytearray

    record.set(Field.create(fields));
    SortedMap<String, String> columnsToParameters = ImmutableSortedMap.<String, String>naturalOrder()
        .put("F_BOOL", "?")
        .put("F_SHORT", "?")
        .put("F_INT", "?")
        .put("F_LONG", "?")
        .put("F_FLOAT", "?")
        .put("F_DOUBLE", "?")
        .put("F_DATE", "?")
        .put("F_DATETIME", "?")
        .put("F_TIME", "?")
        .put("F_DECIMAL", "?")
        .put("F_STR" , "?")
        .put("F_BYTEARRAY", "?")
        .build();

    String query = "INSERT INTO TEST.TEST_TABLE2 ("
        + "F_BOOL,"
        + "F_SHORT,"
        + "F_INT,"
        + "F_LONG,"
        + "F_FLOAT,"
        + "F_DOUBLE,"
        + "F_DATE,"
        + "F_DATETIME,"
        + "F_TIME,"
        + "F_DECIMAL,"
        + "F_STR,"
        + "F_BYTEARRAY)"
        + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?);";

    try {
      PreparedStatement stmt = PowerMockito.spy(connection.prepareStatement(query));
      PowerMockito.doThrow(
          new SQLFeatureNotSupportedException("SDC-7959: setObject() not supported by some drivers.")
      )
          .when(stmt)
          .setObject(
              Mockito.anyInt(),
              Mockito.anyObject(),
              Mockito.anyInt()
          );

      writer.setParameters(OperationType.INSERT_CODE, columnsToParameters, record, connection, stmt);
    } catch (OnRecordErrorException ex) {
      Assert.fail(ex.getMessage());
    } catch (SQLFeatureNotSupportedException ex) {
      Assert.fail("SDC-7959: setObject() not supported by all drivers. " + ex.getMessage());
    } catch (SQLException ex) {
      Assert.fail("Wrong SQLException:" + ex.getMessage());
    }
  }

  @Test
  public void testType() throws StageException {
    JdbcBaseRecordWriter writer = new JdbcGenericRecordWriter(
        connectionString,
        dataSource,
        "TEST",
        "TEST_TABLE",
        false, //rollback
        new LinkedList<JdbcFieldColumnParamMapping>(),
        JDBCOperationType.INSERT.getCode(),
        UnsupportedOperationAction.DISCARD,
        null,
        new JdbcRecordReader(),
        false,
        Collections.emptyList(),
        true,
        context
    );

    /* isColumnTypeNumeric() - true assertions */

    Assert.assertTrue(writer.isColumnTypeNumeric(Types.BIT));
    Assert.assertTrue(writer.isColumnTypeNumeric(Types.TINYINT));
    Assert.assertTrue(writer.isColumnTypeNumeric(Types.SMALLINT));
    Assert.assertTrue(writer.isColumnTypeNumeric(Types.INTEGER));
    Assert.assertTrue(writer.isColumnTypeNumeric(Types.BIGINT));
    Assert.assertTrue(writer.isColumnTypeNumeric(Types.DECIMAL));
    Assert.assertTrue(writer.isColumnTypeNumeric(Types.NUMERIC));
    Assert.assertTrue(writer.isColumnTypeNumeric(Types.FLOAT));
    Assert.assertTrue(writer.isColumnTypeNumeric(Types.REAL));
    Assert.assertTrue(writer.isColumnTypeNumeric(Types.DOUBLE));

    /* isColumnTypeNumeric() - false assertion */

    Assert.assertFalse(writer.isColumnTypeNumeric(Types.CHAR));
    Assert.assertFalse(writer.isColumnTypeNumeric(Types.DATE));
    Assert.assertFalse(writer.isColumnTypeNumeric(Types.TIMESTAMP_WITH_TIMEZONE));
    Assert.assertFalse(writer.isColumnTypeNumeric(Types.TIME_WITH_TIMEZONE));

    /* isColumnTypeText() - true assertions */

    Assert.assertTrue(writer.isColumnTypeText(Types.CHAR));
    Assert.assertTrue(writer.isColumnTypeText(Types.VARCHAR));
    Assert.assertTrue(writer.isColumnTypeText(Types.BLOB));
    Assert.assertTrue(writer.isColumnTypeText(Types.LONGVARCHAR));
    Assert.assertTrue(writer.isColumnTypeText(Types.NCHAR));
    Assert.assertTrue(writer.isColumnTypeText(Types.NVARCHAR));
    Assert.assertTrue(writer.isColumnTypeText(Types.LONGNVARCHAR));
    Assert.assertTrue(writer.isColumnTypeText(Types.SQLXML));
    Assert.assertTrue(writer.isColumnTypeText(Types.CLOB));
    Assert.assertTrue(writer.isColumnTypeText(Types.NCLOB));

    /* isColumnTypeText() - false assertions */

    Assert.assertFalse(writer.isColumnTypeText(Types.INTEGER));
    Assert.assertFalse(writer.isColumnTypeText(Types.DATE));
    Assert.assertFalse(writer.isColumnTypeText(Types.TIME_WITH_TIMEZONE));
    Assert.assertFalse(writer.isColumnTypeText(Types.TIMESTAMP_WITH_TIMEZONE));

    /* isColumnTypeDate() - true assertions */

    Assert.assertTrue(writer.isColumnTypeDate(Types.DATE));
    Assert.assertTrue(writer.isColumnTypeDate(Types.TIME));
    Assert.assertTrue(writer.isColumnTypeDate(Types.TIMESTAMP));

    /* isColumnTypeDate() - false assertions */

    Assert.assertFalse(writer.isColumnTypeDate(Types.INTEGER));
    Assert.assertFalse(writer.isColumnTypeDate(Types.TIMESTAMP_WITH_TIMEZONE));
    Assert.assertFalse(writer.isColumnTypeDate(Types.TIME_WITH_TIMEZONE));

    /* isColumnTypeBinary() - true assertions */

    Assert.assertTrue(writer.isColumnTypeBinary(Types.BINARY));
    Assert.assertTrue(writer.isColumnTypeBinary(Types.BLOB));
    Assert.assertTrue(writer.isColumnTypeBinary(Types.LONGVARBINARY));
    Assert.assertTrue(writer.isColumnTypeBinary(Types.VARBINARY));

    /* isColumnTypeBinary() - false assertions */

    Assert.assertFalse(writer.isColumnTypeDate(Types.INTEGER));
    Assert.assertFalse(writer.isColumnTypeDate(Types.TIMESTAMP_WITH_TIMEZONE));
    Assert.assertFalse(writer.isColumnTypeDate(Types.TIME_WITH_TIMEZONE));

  }
}
