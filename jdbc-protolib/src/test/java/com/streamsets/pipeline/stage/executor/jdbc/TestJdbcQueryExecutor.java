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
package com.streamsets.pipeline.stage.executor.jdbc;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.JdbcHikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.connection.JdbcConnection;
import com.streamsets.pipeline.sdk.ExecutorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestJdbcQueryExecutor {

  private final String JDBC_USER = "sa";
  private final String JDBC_PASSWD = "sa";
  private final String JDBC_DB = "test";
  private final String JDBC_CONNECTION = "jdbc:h2:mem:" + JDBC_DB;

  private Connection connection;

  @Before
  public void setUp() throws Exception {
    connection = DriverManager.getConnection(JDBC_CONNECTION, JDBC_USER, JDBC_PASSWD);

    try(Statement stmt = connection.createStatement()) {
      stmt.executeUpdate("CREATE TABLE `origin` (id int, name VARCHAR(50))");
    }
  }

  @After
  public void tearDown() throws Exception {
    if(connection != null) {

      try(Statement stmt = connection.createStatement()) {
        stmt.executeUpdate("DROP TABLE `origin` IF EXISTS");
        stmt.executeUpdate("DROP TABLE `copy` IF EXISTS");
        stmt.executeUpdate("DROP TABLE `el` IF EXISTS");
      }

      connection.close();
    }
  }

  @Test
  public void testExecuteSimpleQuery() throws Exception {
    JdbcQueryExecutor queryExecutor = createExecutor("CREATE TABLE copy AS SELECT * FROM origin");

    ExecutorRunner runner = new ExecutorRunner.Builder(JdbcQueryDExecutor.class, queryExecutor)
      .setOnRecordError(OnRecordError.STOP_PIPELINE)
      .build();
    runner.runInit();

    Record record = RecordCreator.create();
    record.set(Field.create("blank line"));

    runner.runWrite(ImmutableList.of(record));
    runner.runDestroy();

    assertTableStructure("copy",
      new ImmutablePair("ID", Types.INTEGER),
      new ImmutablePair("NAME", Types.VARCHAR)
    );

    assertEquals(runner.getEventRecords().get(0).getEventType(), "successful-query");
  }

  @Test
  public void testEL() throws Exception {
    JdbcQueryExecutor queryExecutor = createExecutor("CREATE TABLE ${record:value('/table')} AS SELECT * FROM origin");

    ExecutorRunner runner = new ExecutorRunner.Builder(JdbcQueryDExecutor.class, queryExecutor)
      .setOnRecordError(OnRecordError.STOP_PIPELINE)
      .build();
    runner.runInit();

    Map<String, Field> map = new HashMap<>();
    map.put("table", Field.create("el"));

    Record record = RecordCreator.create();
    record.set(Field.create(map));

    runner.runWrite(ImmutableList.of(record));
    runner.runDestroy();

    assertTableStructure("el",
      new ImmutablePair("ID", Types.INTEGER),
      new ImmutablePair("NAME", Types.VARCHAR)
    );

    assertEquals(runner.getEventRecords().get(0).getEventType(), "successful-query");
  }

  @Test
  public void testIncorrectQuery() throws Exception {
    JdbcQueryExecutor queryExecutor = createExecutor("THIS REALLY IS NOT VALID QUERY");

    ExecutorRunner runner = new ExecutorRunner.Builder(JdbcQueryDExecutor.class, queryExecutor)
      .setOnRecordError(OnRecordError.TO_ERROR)
      .build();
    runner.runInit();

    Record record = RecordCreator.create();
    record.set(Field.create("FIELD"));

    runner.runWrite(ImmutableList.of(record));
    List<Record> errors = runner.getErrorRecords();
    Assert.assertNotNull(errors);
    Assert.assertEquals(1, errors.size());
    Assert.assertEquals("FIELD", errors.get(0).get().getValueAsString());
    Assert.assertEquals(runner.getEventRecords().get(0).getEventType(), "failed-query");

    runner.runDestroy();
  }

  @Test
  public void testIncludeQueryResultCountEnabledForInsert() throws Exception{
    JdbcQueryExecutor queryExecutor = createExecutor("INSERT INTO origin(id, name) VALUES(1, 'abc') ", true);
    ExecutorRunner runner = new ExecutorRunner.Builder(JdbcQueryDExecutor.class, queryExecutor)
            .setOnRecordError(OnRecordError.STOP_PIPELINE)
            .build();
    runner.runInit();

    Record record = RecordCreator.create();
    runner.runWrite(ImmutableList.of(record));
    runner.runDestroy();

    assertEquals(runner.getEventRecords().get(0).getEventType(), "successful-query");
    assertEquals(runner.getEventRecords().get(0).get().getValueAsMap().get("query-result").getValue(), "1 row(s) affected");
  }

  @Test
  public void testIncludeQueryResultCountEnabledForSelect() throws Exception{
    JdbcQueryExecutor queryExecutor = createExecutor("SELECT COUNT(*) FROM origin", true);
    ExecutorRunner runner = new ExecutorRunner.Builder(JdbcQueryDExecutor.class, queryExecutor)
            .setOnRecordError(OnRecordError.STOP_PIPELINE)
            .build();
    runner.runInit();

    Record record = RecordCreator.create();
    runner.runWrite(ImmutableList.of(record));
    runner.runDestroy();

    assertEquals(runner.getEventRecords().get(0).getEventType(), "successful-query");
    assertEquals(runner.getEventRecords().get(0).get().getValueAsMap().get("query-result").getValue(), "1 row(s) returned");
  }

  private JdbcQueryExecutorConfig createJdbcQueryExecutorConfig(){
    JdbcQueryExecutorConfig config = new JdbcQueryExecutorConfig();
    config.hikariConfigBean = new JdbcHikariPoolConfigBean();
    config.hikariConfigBean.connection = new JdbcConnection();
    config.hikariConfigBean.connection.connectionString = JDBC_CONNECTION;
    config.hikariConfigBean.connection.useCredentials = true;
    config.hikariConfigBean.connection.username = () -> JDBC_USER;
    config.hikariConfigBean.connection.password = () -> JDBC_PASSWD;

    return config;
  }

  private JdbcQueryExecutor createExecutor(String query, boolean queryResultCount){
    JdbcQueryExecutorConfig config = createJdbcQueryExecutorConfig();
    config.queries = Collections.singletonList(query);
    config.queryResultCount = queryResultCount;

    return new JdbcQueryExecutor(config);
  }

  public JdbcQueryExecutor createExecutor(String query) {
    JdbcQueryExecutorConfig config = createJdbcQueryExecutorConfig();

    config.queries = Collections.singletonList(query);
    return new JdbcQueryExecutor(config);
  }

  public static abstract class QueryValidator {
    abstract public void validateResultSet(ResultSet rs) throws Exception;
  }

  /**
   * Simple query result validation assertion.
   */
  public  void assertQueryResult(String query, QueryValidator validator) throws Exception {
    try(
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(query);
    ) {
      validator.validateResultSet(rs);
    }
  }

  /**
   * Validate structure of the result set (column names and types).
   */
  public void assertResultSetStructure(ResultSet rs, Pair<String, Integer>... columns) throws Exception {
    ResultSetMetaData metaData = rs.getMetaData();
    Assert.assertEquals(Utils.format("Unexpected number of columns"), columns.length, metaData.getColumnCount());
    int i = 1;
    for(Pair<String, Integer> column : columns) {
      Assert.assertEquals(Utils.format("Unexpected name for column {}", i), column.getLeft(), metaData.getColumnName(i));
      Assert.assertEquals(Utils.format("Unexpected type for column {}", i), (int)column.getRight(), metaData.getColumnType(i));
      i++;
    }
  }

  /**
   * Assert structure of given table.
   */
  public void assertTableStructure(String table, final Pair<String, Integer>... columns) throws Exception {
    assertQueryResult(Utils.format("select * from {}", table), new QueryValidator() {
      @Override
      public void validateResultSet(ResultSet rs) throws Exception {
        assertResultSetStructure(rs, columns);
      }
    });
  }
}
