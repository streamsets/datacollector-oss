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
package com.streamsets.pipeline.stage.origin.jdbc.table;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(Parameterized.class)
public class AllTypesIT extends BaseTableJdbcSourceIT {
  private static final String CHAR_AND_BINARY_TEMPLATE  = "INSERT INTO TEST.CHAR_AND_BINARY VALUES (%s, '%s', '%s', '%s', X'%s', X'%s')";
  private static final String DATE_AND_TIME_TEMPLATE  = "INSERT INTO TEST.DATE_AND_TIME VALUES (%s, '%s', '%s', '%s', '%s')";
  private static final String DIFFERENT_INTS_TEMPLATE = "INSERT INTO TEST.DIFFERENT_INTS VALUES (%s, %s, %s, %s, %s, %s, %s)";
  private static final String FLOATING_PT_INTS_TEMPLATE  = "INSERT INTO TEST.FLOATING_PT_INTS VALUES (%s, %s, %s, %s, %s, %s)";
  private static final String OTHER_TYPES_TEMPLATE  = "INSERT INTO TEST.OTHER_TYPES VALUES (%s, %s);";
  private static final Map<String, Pair<String, ArrayList<Record>>> TABLE_TO_TEMPLATE_AND_RECORDS_MAP =
      new ImmutableMap.Builder<String, Pair<String, ArrayList<Record>>>()
          .put("CHAR_AND_BINARY", Pair.of(CHAR_AND_BINARY_TEMPLATE, new ArrayList<Record>()))
          .put("DATE_AND_TIME", Pair.of(DATE_AND_TIME_TEMPLATE, new ArrayList<Record>()))
          .put("DIFFERENT_INTS", Pair.of(DIFFERENT_INTS_TEMPLATE, new ArrayList<Record>()))
          .put("FLOATING_PT_INTS", Pair.of(FLOATING_PT_INTS_TEMPLATE, new ArrayList<Record>()))
          .put("OTHER_TYPES", Pair.of(OTHER_TYPES_TEMPLATE, new ArrayList<Record>()))
          .build();

  private final String table;

  public AllTypesIT(String table) {
    this.table = table;
  }

  @Parameterized.Parameters(name = "Table Name : {0}")
  public static Object[] data() throws Exception {
    return TABLE_TO_TEMPLATE_AND_RECORDS_MAP.keySet().toArray();
  }

  private static void createIdField(Map<String, Field> fields, AtomicInteger id_field) {
    fields.put("p_id", Field.create(Field.Type.INTEGER, id_field.incrementAndGet()));
  }

  private static void populateRecords() {
    Record record = RecordCreator.create();
    LinkedHashMap<String, Field> fields;
    AtomicInteger id_field = new AtomicInteger(0);

    //CHAR_AND_BINARY
    fields = new LinkedHashMap<>();
    createIdField(fields, id_field);
    fields.put("char1", Field.create("abcdefghij"));
    fields.put("varchar1", Field.create(UUID.randomUUID().toString()));
    fields.put("clob1", Field.create(UUID.randomUUID().toString()));
    fields.put("varbinary1", Field.create(UUID.randomUUID().toString().getBytes()));
    fields.put("blob1", Field.create(UUID.randomUUID().toString().getBytes()));
    record.set(Field.createListMap(fields));

    TABLE_TO_TEMPLATE_AND_RECORDS_MAP.get("CHAR_AND_BINARY").getRight().add(record);

    //Date and time
    record = RecordCreator.create();
    fields = new LinkedHashMap<>();
    createIdField(fields, id_field);
    Calendar calendar = Calendar.getInstance();

    calendar.set(Calendar.HOUR_OF_DAY, 0);
    calendar.set(Calendar.MINUTE, 0);
    calendar.set(Calendar.SECOND, 0);
    calendar.set(Calendar.MILLISECOND, 0);
    fields.put("date1", Field.create(Field.Type.DATE, calendar.getTime()));
    calendar.setTimeInMillis(System.currentTimeMillis());

    calendar.set(Calendar.MILLISECOND, 0);
    fields.put("timestamp1", Field.create(Field.Type.DATETIME, calendar.getTime()));
    fields.put("datetime1", Field.create(Field.Type.DATETIME, calendar.getTime()));
    calendar.setTimeInMillis(System.currentTimeMillis());

    calendar.set(Calendar.YEAR, 1970);
    calendar.set(Calendar.MONTH, Calendar.JANUARY);
    calendar.set(Calendar.DAY_OF_MONTH, 1);
    calendar.set(Calendar.MILLISECOND, 0);
    fields.put("time1", Field.create(Field.Type.TIME, calendar.getTime()));
    calendar.setTimeInMillis(System.currentTimeMillis());

    record.set(Field.createListMap(fields));
    TABLE_TO_TEMPLATE_AND_RECORDS_MAP.get("DATE_AND_TIME").getRight().add(record);

    //DIFFERENT_INTS
    record = RecordCreator.create();
    fields = new LinkedHashMap<>();
    createIdField(fields, id_field);
    fields.put("int1", Field.create(Field.Type.INTEGER, Integer.MIN_VALUE));
    fields.put("int2", Field.create(Field.Type.INTEGER, Integer.MIN_VALUE));
    fields.put("mediumint1", Field.create(Field.Type.INTEGER, Integer.MIN_VALUE));
    fields.put("tinyint1", Field.create(Field.Type.SHORT, -128));
    fields.put("smallint1", Field.create(Field.Type.SHORT, Short.MIN_VALUE));
    fields.put("bigint1", Field.create(Field.Type.LONG, Long.MIN_VALUE));
    record.set(Field.createListMap(fields));
    TABLE_TO_TEMPLATE_AND_RECORDS_MAP.get("DIFFERENT_INTS").getRight().add(record);

    record = RecordCreator.create();
    fields = new LinkedHashMap<>();
    createIdField(fields, id_field);
    fields.put("int1", Field.create(Field.Type.INTEGER, Integer.MAX_VALUE));
    fields.put("int2", Field.create(Field.Type.INTEGER, Integer.MAX_VALUE));
    fields.put("mediumint1", Field.create(Field.Type.INTEGER, Integer.MAX_VALUE));
    fields.put("tinyint1", Field.create(Field.Type.SHORT, 127));
    fields.put("smallint1", Field.create(Field.Type.SHORT, Short.MAX_VALUE));
    fields.put("bigint1", Field.create(Field.Type.LONG, Long.MAX_VALUE));
    record.set(Field.createListMap(fields));
    TABLE_TO_TEMPLATE_AND_RECORDS_MAP.get("DIFFERENT_INTS").getRight().add(record);

    //FLOATING_PT_INTS
    record = RecordCreator.create();
    fields = new LinkedHashMap<>();
    createIdField(fields, id_field);
    fields.put("decimal1", Field.create(Field.Type.DECIMAL, new BigDecimal("12.345")));
    fields.put("number1", Field.create(Field.Type.DECIMAL, new BigDecimal("0.12345")));
    fields.put("double1", Field.create(Field.Type.DOUBLE, 123.456));
    fields.put("real1", Field.create(Field.Type.FLOAT, 12.34));
    fields.put("floatdouble1", Field.create(Field.Type.DOUBLE, Double.MAX_VALUE));
    record.set(Field.createListMap(fields));
    TABLE_TO_TEMPLATE_AND_RECORDS_MAP.get("FLOATING_PT_INTS").getRight().add(record);

    record = RecordCreator.create();
    fields = new LinkedHashMap<>();
    createIdField(fields, id_field);
    fields.put("decimal1", Field.create(Field.Type.DECIMAL, new BigDecimal("-12.345")));
    fields.put("number1", Field.create(Field.Type.DECIMAL, new BigDecimal("-0.12345")));
    fields.put("double1", Field.create(Field.Type.DOUBLE, -123.456));
    fields.put("real1", Field.create(Field.Type.FLOAT, -12.34));
    fields.put("floatdouble1", Field.create(Field.Type.DOUBLE, Double.MIN_VALUE));
    record.set(Field.createListMap(fields));
    TABLE_TO_TEMPLATE_AND_RECORDS_MAP.get("FLOATING_PT_INTS").getRight().add(record);

    //OTHER_TYPES
    record = RecordCreator.create();
    fields = new LinkedHashMap<>();
    createIdField(fields, id_field);
    fields.put("boolean1", Field.create(Field.Type.BOOLEAN, true));
    record.set(Field.createListMap(fields));
    TABLE_TO_TEMPLATE_AND_RECORDS_MAP.get("OTHER_TYPES").getRight().add(record);

    record = RecordCreator.create();
    fields = new LinkedHashMap<>();
    createIdField(fields, id_field);
    fields.put("boolean1", Field.create(Field.Type.BOOLEAN, false));
    record.set(Field.createListMap(fields));
    TABLE_TO_TEMPLATE_AND_RECORDS_MAP.get("OTHER_TYPES").getRight().add(record);
  }

  @BeforeClass
  public static void setupTables() throws SQLException {
    populateRecords();
    try (Statement statement = connection.createStatement())  {
      //CHAR_AND_BINARY
      statement.addBatch(
          "CREATE TABLE TEST.CHAR_AND_BINARY " +
              "(" +
              " p_id INT NOT NULL PRIMARY KEY," +
              " char1 char(10)," +
              " varchar1 varchar(500)," +
              " clob1 CLOB(500)," +
              " varbinary1 VARBINARY(500)," +
              " blob1 BLOB(500)" +
              ")"
      );

      //DATE_AND_TIME
      statement.addBatch(
          "CREATE TABLE TEST.DATE_AND_TIME " +
              "(" +
              " p_id INT NOT NULL PRIMARY KEY," +
              " date1 DATE," +
              " timestamp1 TIMESTAMP," +
              " datetime1 DATETIME," +
              " time1 TIME" +
              ")"
      );

      //DIFFERENT_INTS
      statement.addBatch(
          "CREATE TABLE TEST.DIFFERENT_INTS " +
              "(" +
              " p_id INT NOT NULL PRIMARY KEY," +
              " int1 INT," +
              " int2 INTEGER," +
              " mediumint1 MEDIUMINT," +
              " tinyint1 TINYINT," +
              " smallint1 SMALLINT," +
              " bigint1 BIGINT" +
              ")"
      );

      //FLOATING_PT_INTS
      statement.addBatch(
          "CREATE TABLE TEST.FLOATING_PT_INTS " +
              "(" +
              " p_id INT NOT NULL PRIMARY KEY," +
              " decimal1 DECIMAL(5, 3)," +
              " number1 NUMBER(5, 5)," +
              " double1 DOUBLE(6)," +
              " real1 REAL," +
              //H2 returns float as double.
              " floatdouble1 FLOAT" +
              ")"
      );

      //OTHER_TYPES
      statement.addBatch(
          "CREATE TABLE TEST.OTHER_TYPES " +
              "(" +
              " p_id INT NOT NULL PRIMARY KEY," +
              " boolean1 BOOLEAN" +
              ")"
      );
      statement.executeBatch();
    }
  }

  @AfterClass
  public static void deleteTables() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      for (String table  : TABLE_TO_TEMPLATE_AND_RECORDS_MAP.keySet()) {
        statement.addBatch(String.format(DROP_STATEMENT_TEMPLATE, database, table));
      }
      statement.executeBatch();
    }
  }

  @Before
  public void insertRowsInTheTable() throws SQLException {
    String insertTemplate = TABLE_TO_TEMPLATE_AND_RECORDS_MAP.get(table).getLeft();
    List<Record> records = TABLE_TO_TEMPLATE_AND_RECORDS_MAP.get(table).getRight();
    insertRows(insertTemplate, records);
  }

  @Test
  public void testDataTypes() throws Exception {
    TableConfigBeanImpl tableConfigBean =  new TableJdbcSourceTestBuilder.TableConfigBeanTestBuilder()
        .tablePattern(table)
        .schema(database)
        .build();

    TableJdbcSource tableJdbcSource = new TableJdbcSourceTestBuilder(JDBC_URL, true, USER_NAME, PASSWORD)
        .tableConfigBeans(ImmutableList.of(tableConfigBean))
        .build();

    PushSourceRunner runner = new PushSourceRunner.Builder(TableJdbcDSource.class, tableJdbcSource)
        .addOutputLane("a").build();
    runner.runInit();
    JdbcPushSourceTestCallback callback = new JdbcPushSourceTestCallback(runner, 1);
    try {
      runner.runProduce(Collections.emptyMap(), 1000, callback);
      List<List<Record>> batchRecords = callback.waitForAllBatchesAndReset();

      List<Record> expectedRecords = TABLE_TO_TEMPLATE_AND_RECORDS_MAP.get(table).getRight();
      List<Record> actualRecords = batchRecords.get(0);
      checkRecords(expectedRecords, actualRecords);
    } finally {
      runner.runDestroy();
    }
  }
}
