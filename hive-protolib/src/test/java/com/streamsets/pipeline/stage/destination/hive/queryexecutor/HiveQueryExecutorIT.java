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
package com.streamsets.pipeline.stage.destination.hive.queryexecutor;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.sdk.ExecutorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.stage.BaseHiveIT;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * To verify that the Hive queries is indeed executed properly, we're creating tables and validating their proper structure.
 */
@SuppressWarnings("unchecked")
@Ignore
public class HiveQueryExecutorIT extends BaseHiveIT {

  @Before
  public void createBaseTable() throws Exception {
    executeUpdate("CREATE TABLE `origin` (id int, name string)");
  }

  @Test
  public void testExecuteSimpleQuery() throws Exception {
    HiveQueryExecutor queryExecutor =
        new HiveQueryExecutorTestBuilder()
            .addQuery("CREATE TABLE copy AS SELECT * FROM origin")
            .build();

    ExecutorRunner runner = new ExecutorRunner.Builder(HiveQueryDExecutor.class, queryExecutor)
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .build();
    runner.runInit();

    try {
      Record record = RecordCreator.create();
      record.set(Field.create("blank line"));

      runner.runWrite(ImmutableList.of(record));

      assertTableStructure("default.copy",
          ImmutablePair.of("copy.id", Types.INTEGER),
          ImmutablePair.of("copy.name", Types.VARCHAR)
      );

      List<Record> events = runner.getEventRecords();
      assertNotNull(events);
      assertEquals(1, events.size());
      checkSuccessfulEvent(events.get(0), "CREATE TABLE copy AS SELECT * FROM origin");
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testEL() throws Exception {
    HiveQueryExecutor queryExecutor =
        new HiveQueryExecutorTestBuilder()
            .addQuery("CREATE TABLE ${record:value('/table')} AS SELECT * FROM origin")
            .build();

    ExecutorRunner runner = new ExecutorRunner.Builder(HiveQueryDExecutor.class, queryExecutor)
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .build();
    runner.runInit();

    try {
      Map<String, Field> map = new HashMap<>();
      map.put("table", Field.create("el"));

      Record record = RecordCreator.create();
      record.set(Field.create(map));

      runner.runWrite(ImmutableList.of(record));

      assertTableStructure("default.el",
          ImmutablePair.of("el.id", Types.INTEGER),
          ImmutablePair.of("el.name", Types.VARCHAR)
      );

      List<Record> events = runner.getEventRecords();
      assertNotNull(events);
      assertEquals(1, events.size());
      checkSuccessfulEvent(events.get(0), "CREATE TABLE el AS SELECT * FROM origin");
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testIncorrectQuery() throws Exception {
    HiveQueryExecutor queryExecutor = new HiveQueryExecutorTestBuilder().addQuery("INVALID").build();

    ExecutorRunner runner = new ExecutorRunner.Builder(HiveQueryDExecutor.class, queryExecutor)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();
    try {

      Record record = RecordCreator.create();
      record.set(Field.create("FIELD"));

      runner.runWrite(ImmutableList.of(record));
      List<Record> errors = runner.getErrorRecords();
      assertNotNull(errors);
      assertEquals(1, errors.size());
      assertEquals("FIELD", errors.get(0).get().getValueAsString());

      String errorMessage = errors.get(0).getHeader().getErrorMessage();
      String errorMessageFormat = ".* Failed to execute queries. Details : Failed to execute query 'INVALID'. Reason: org.apache.hive.service.cli.HiveSQLException: Error while compiling statement: FAILED: .* cannot recognize input near .*";
      assertTrue("Error message '" + errorMessage + "' doesn't conform to regexp: " + errorMessageFormat, errorMessage.matches(errorMessageFormat));

      List<Record> events = runner.getEventRecords();
      assertNotNull(events);
      assertEquals(1, events.size());
      checkFailureEvent(events.get(0), ImmutablePair.of("INVALID", Collections.<String>emptyList()));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testWrongELNoEvents() throws Exception {
    HiveQueryExecutor queryExecutor =
        new HiveQueryExecutorTestBuilder()
            //missing end bracket } in recordEL
            .addQuery("CREATE TABLE ${record:value('/tabe')1 AS SELECT * FROM origin")
            .build();

    ExecutorRunner runner = new ExecutorRunner.Builder(HiveQueryDExecutor.class, queryExecutor)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertNotNull(issues);
    assertEquals(1, issues.size());

    Stage.ConfigIssue issue = issues.get(0);
    assertTrue(Utils.format("Unexpected message: {}", issue.toString()), issue.toString().contains("QUERY_EXECUTOR_002 - Failed to evaluate queries."));
  }

  @Test
  public void testMultipleQueriesAllSuccess() throws Exception {
    HiveQueryExecutor queryExecutor =
        new HiveQueryExecutorTestBuilder()
            .addQuery("DROP VIEW IF EXISTS ${str:substring(record:value('/table'), 0, str:length(record:value('/table'))-3)}")
            .addQuery("CREATE VIEW ${str:substring(record:value('/table'), 0, str:length(record:value('/table'))-3)} AS SELECT * FROM ${record:value('/table')}")
            .build();

    ExecutorRunner runner = new ExecutorRunner.Builder(HiveQueryDExecutor.class, queryExecutor)
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .build();
    runner.runInit();

    executeUpdate("CREATE TABLE `multiplequeriessuccess_v1` (id int, name string)");
    assertTableStructure("default.multiplequeriessuccess_v1", ImmutablePair.of("multiplequeriessuccess_v1.id", Types.INTEGER), ImmutablePair.of("multiplequeriessuccess_v1.name", Types.VARCHAR));


    Map<String, Field> map = new HashMap<>();
    map.put("table", Field.create("multiplequeriessuccess_v1"));

    Record record = RecordCreator.create();
    record.set(Field.create(map));

    runner.runWrite(ImmutableList.of(record));

    assertTableStructure(
        "default.multiplequeriessuccess",
        ImmutablePair.of("multiplequeriessuccess.id", Types.INTEGER),
        ImmutablePair.of("multiplequeriessuccess.name", Types.VARCHAR)
    );

    //Version 2 of the table with new rows
    executeUpdate("CREATE TABLE `multiplequeriessuccess_v2` (id int, name string, new_column1 string, new_column2 string)");

    assertTableStructure(
        "default.multiplequeriessuccess_v2",
        ImmutablePair.of("multiplequeriessuccess_v2.id", Types.INTEGER),
        ImmutablePair.of("multiplequeriessuccess_v2.name", Types.VARCHAR),
        ImmutablePair.of("multiplequeriessuccess_v2.new_column1", Types.VARCHAR),
        ImmutablePair.of("multiplequeriessuccess_v2.new_column2", Types.VARCHAR)
    );

    map = new HashMap<>();
    map.put("table", Field.create("multiplequeriessuccess_v2"));

    record = RecordCreator.create();
    record.set(Field.create(map));

    runner.runWrite(ImmutableList.of(record));

    assertTableStructure(
        "default.multiplequeriessuccess",
        ImmutablePair.of("multiplequeriessuccess.id", Types.INTEGER),
        ImmutablePair.of("multiplequeriessuccess.name", Types.VARCHAR),
        ImmutablePair.of("multiplequeriessuccess.new_column1", Types.VARCHAR),
        ImmutablePair.of("multiplequeriessuccess.new_column2", Types.VARCHAR)
    );

    List<Record> events = runner.getEventRecords();
    assertNotNull(events);
    assertEquals(4, events.size());
    checkSuccessfulEvent(events.get(0), "DROP VIEW IF EXISTS multiplequeriessuccess");
    checkSuccessfulEvent(events.get(1), "CREATE VIEW multiplequeriessuccess AS SELECT * FROM multiplequeriessuccess_v1");
    checkSuccessfulEvent(events.get(2), "DROP VIEW IF EXISTS multiplequeriessuccess");
    checkSuccessfulEvent(events.get(3), "CREATE VIEW multiplequeriessuccess AS SELECT * FROM multiplequeriessuccess_v2");
    runner.runDestroy();
  }

  public void testMultipleQueriesFailureInMiddle(boolean stopOnFailure) throws Exception {
    HiveQueryExecutor queryExecutor =
        new HiveQueryExecutorTestBuilder()
            .addQuery("select 1")
            .addQuery("create table invalidTable")
            .addQuery("select 2")
            .stopOnQueryFailure(stopOnFailure)
            .build();

    ExecutorRunner runner = new ExecutorRunner.Builder(HiveQueryDExecutor.class, queryExecutor)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();
    try {
      runner.runWrite(ImmutableList.of(RecordCreator.create()));
      assertEquals(1, runner.getErrorRecords().size());
      assertNotNull(runner.getEventRecords());

      List<Record> eventRecords = runner.getEventRecords();
      if (stopOnFailure) {
        assertEquals(2, eventRecords.size());
        checkSuccessfulEvent(eventRecords.get(0), "select 1");
        checkFailureEvent(eventRecords.get(1), ImmutablePair.of("create table invalidTable",  Collections.singletonList("select 2")));
      } else {
        assertEquals(3, eventRecords.size());
        checkSuccessfulEvent(eventRecords.get(0), "select 1");
        checkFailureEvent(eventRecords.get(1), ImmutablePair.of("create table invalidTable", Collections.<String>emptyList()));
        checkSuccessfulEvent(eventRecords.get(2), "select 2");
      }
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testMultipleQueriesFailureInMiddleWithStopOnFailure() throws Exception {
    testMultipleQueriesFailureInMiddle(true);
  }

  @Test
  public void testMultipleQueriesFailureInMiddleWithoutStopOnFailure() throws Exception {
    testMultipleQueriesFailureInMiddle(false);
  }

  private void checkFailureEvent(Record eventRecord, Pair<String, List<String>> failedQueryWithOptionalUnexecutedQueries) {
    assertEquals(HiveQueryExecutorEvents.FAILED_QUERY_EVENT, eventRecord.getHeader().getAttribute("sdc.event.type"));
    assertTrue(eventRecord.has("/"+ HiveQueryExecutorEvents.QUERY_EVENT_FIELD));
    String failedQuery = failedQueryWithOptionalUnexecutedQueries.getLeft();
    assertEquals(failedQuery, eventRecord.get("/" + HiveQueryExecutorEvents.QUERY_EVENT_FIELD).getValueAsString());
    List<String> unexecutedQueries = failedQueryWithOptionalUnexecutedQueries.getRight();
    if (!unexecutedQueries.isEmpty()) {
      assertTrue(eventRecord.has("/" + HiveQueryExecutorEvents.UNEXECUTED_QUERIES_EVENT_FIELD));
      assertEquals(Field.Type.LIST, eventRecord.get("/" + HiveQueryExecutorEvents.UNEXECUTED_QUERIES_EVENT_FIELD).getType());
      assertEquals(unexecutedQueries.size(), eventRecord.get("/" + HiveQueryExecutorEvents.UNEXECUTED_QUERIES_EVENT_FIELD).getValueAsList().size());
      List<Field> unexecutedQueriesEventField =  eventRecord.get("/" + HiveQueryExecutorEvents.UNEXECUTED_QUERIES_EVENT_FIELD).getValueAsList();
      for (int i=0; i < unexecutedQueries.size(); i++) {
        assertEquals(unexecutedQueries.get(i), unexecutedQueriesEventField.get(i).getValueAsString());
      }
    }
  }
  private void checkSuccessfulEvent(Record eventRecord, String successfulQuery) {
    assertEquals(HiveQueryExecutorEvents.SUCCESSFUL_QUERY_EVENT, eventRecord.getHeader().getAttribute("sdc.event.type"));
    assertTrue(eventRecord.has("/"+ HiveQueryExecutorEvents.QUERY_EVENT_FIELD));
    assertEquals(successfulQuery, eventRecord.get("/" + HiveQueryExecutorEvents.QUERY_EVENT_FIELD).getValueAsString());
  }

}
