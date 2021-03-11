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
package com.streamsets.pipeline.stage.processor.parser.sql;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.jdbc.parser.sql.ParseUtil;
import com.streamsets.pipeline.lib.jdbc.parser.sql.UnsupportedFieldTypeValues;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Matchers.eq;

public class TestSqlParserProcessor {

  private Statement st;

  @Test
  public void testResolutionIntString() throws Exception {
    SqlParserConfigBean config = new SqlParserConfigBean();
    config.resolveSchema = true;
    config.sqlField = "/sql";
    config.resultFieldPath = "/res";
    config.dbTimeZone = "UTC";
    Record r = RecordCreator.create();
    Map<String, Field> fields = new HashMap<>();
    fields.put("sql", Field.create("INSERT INTO \"SYS\".\"TEST\"(\"ID\", \"NAME\") VALUES ('10', 'Bilbo Baggins')"));
    r.set(Field.create(fields));
    SqlParserProcessor processor = new SqlParserProcessor(config);
    ResultSetMetaData rsmd = setupMocks("\"SYS\".\"TEST\"", processor);
    Mockito.when(rsmd.getColumnCount()).thenReturn(2);
    Mockito.when(rsmd.getColumnType(eq(1))).thenReturn(Types.INTEGER);
    Mockito.when(rsmd.getColumnName(eq(1))).thenReturn("ID");
    Mockito.when(rsmd.getColumnType(eq(2))).thenReturn(Types.VARCHAR);
    Mockito.when(rsmd.getColumnName(eq(2))).thenReturn("NAME");
    ProcessorRunner runner = new ProcessorRunner.Builder(SqlParserDProcessor.class, processor)
        .addOutputLane("s").setOnRecordError(OnRecordError.STOP_PIPELINE).build();
    runner.runInit();
    StageRunner.Output output = runner.runProcess(ImmutableList.of(r));
    Record result = output.getRecords().get("s").get(0);
    Assert.assertEquals(10, result.get("/res/ID").getValueAsInteger());
    Assert.assertEquals("Bilbo Baggins", result.get("/res/NAME").getValueAsString());
    assertOperationCode(result, OperationType.INSERT_CODE);
    assertTableSchema(result, "SYS", "TEST");

  }

  private void assertTableSchema(Record result, String schema, String table) {
    Assert.assertEquals(
        result.getHeader().getAttribute(SqlParserProcessor.TABLE_METADATA_TABLE_SCHEMA_CONSTANT),
        schema);
    Assert.assertEquals(
        result.getHeader().getAttribute(SqlParserProcessor.TABLE_METADATA_TABLE_NAME_CONSTANT),
        table);
    Assert.assertEquals(
        result.getHeader().getAttribute(SqlParserProcessor.TABLE),
        table);
  }

  private void assertOperationCode(Record result, int opCode) {
    Assert.assertEquals(OperationType.getLabelFromIntCode(opCode),
        OperationType.getLabelFromIntCode(
            Integer.parseInt(result.getHeader().getAttribute(OperationType.SDC_OPERATION_TYPE))));
  }

  @Test
  public void testResolutionDateDecimal() throws Exception {
    SqlParserConfigBean config = new SqlParserConfigBean();
    config.resolveSchema = true;
    config.sqlField = "/sql";
    config.resultFieldPath = "/res";
    config.dateFormat = "yyyy-MM-dd HH:mm:ss";
    config.dbTimeZone = ZoneId.systemDefault().getId();
    Record r = RecordCreator.create();
    Map<String, Field> fields = new HashMap<>();
    fields.put("sql", Field.create("INSERT INTO \"SYS\".\"TEST\"(\"ID\", \"DT\", \"DEC\") " +
        "VALUES ('10', TO_DATE('2016-11-21 11:34:09', 'YYYY-MM-DD HH24:MI:SS'), '1000.3300'"));
    r.set(Field.create(fields));
    SqlParserProcessor processor = new SqlParserProcessor(config);
    ResultSetMetaData rsmd = setupMocks("\"SYS\".\"TEST\"", processor);
    Mockito.when(rsmd.getColumnCount()).thenReturn(3);
    Mockito.when(rsmd.getColumnType(eq(1))).thenReturn(Types.INTEGER);
    Mockito.when(rsmd.getColumnName(eq(1))).thenReturn("ID");
    Mockito.when(rsmd.getColumnType(eq(2))).thenReturn(Types.DATE);
    Mockito.when(rsmd.getColumnName(eq(2))).thenReturn("DT");
    Mockito.when(rsmd.getColumnTypeName(eq(2))).thenReturn("DATE");
    Mockito.when(rsmd.getColumnType(eq(3))).thenReturn(Types.NUMERIC);
    Mockito.when(rsmd.getColumnName(eq(3))).thenReturn("DEC");
    Mockito.when(rsmd.getPrecision(eq(3))).thenReturn(10);
    Mockito.when(rsmd.getScale(eq(3))).thenReturn(4);

    ProcessorRunner runner = new ProcessorRunner.Builder(SqlParserDProcessor.class, processor)
        .addOutputLane("s").setOnRecordError(OnRecordError.STOP_PIPELINE).build();
    runner.runInit();
    StageRunner.Output output = runner.runProcess(ImmutableList.of(r));
    Record result = output.getRecords().get("s").get(0);
    Assert.assertEquals("10", result.get("/res/ID").getValueAsString());
    Assert.assertEquals(new SimpleDateFormat(config.dateFormat).parse("2016-11-21 11:34:09"),
        result.get("/res/DT").getValueAsDatetime());
    Assert.assertEquals(new BigDecimal("1000.3300"), result.get("/res/DEC").getValueAsDecimal());
    Assert.assertEquals("10", result.getHeader().getAttribute("jdbc.DEC.precision"));
    Assert.assertEquals("4", result.getHeader().getAttribute("jdbc.DEC.scale"));
    assertOperationCode(result, OperationType.INSERT_CODE);
    assertTableSchema(result, "SYS", "TEST");
  }

  @Test
  public void testNoResolution() throws Exception {
    SqlParserConfigBean config = new SqlParserConfigBean();
    config.resolveSchema = false;
    config.sqlField = "/sql";
    config.resultFieldPath = "/res";
    config.dbTimeZone = ZoneId.systemDefault().getId();
    Record r = RecordCreator.create();
    Map<String, Field> fields = new HashMap<>();
    fields.put("sql", Field.create("INSERT INTO \"SYS\".\"TEST\"(\"ID\", \"NAME\") VALUES ('10', 'Bilbo Baggins')"));
    r.set(Field.create(fields));
    SqlParserProcessor processor = new SqlParserProcessor(config);
    ProcessorRunner runner = new ProcessorRunner.Builder(SqlParserDProcessor.class, processor)
        .addOutputLane("s").setOnRecordError(OnRecordError.STOP_PIPELINE).build();
    runner.runInit();
    StageRunner.Output output = runner.runProcess(ImmutableList.of(r));
    Record result = output.getRecords().get("s").get(0);
    Assert.assertEquals(Field.Type.STRING, result.get("/res/ID").getType());
    Assert.assertEquals(10, result.get("/res/ID").getValueAsInteger());
    Assert.assertEquals("Bilbo Baggins", result.get("/res/NAME").getValueAsString());
  }

  @Test
  public void testResolutionMultipleRecords() throws Exception {
    SqlParserConfigBean config = new SqlParserConfigBean();
    config.resolveSchema = true;
    config.sqlField = "/sql";
    config.resultFieldPath = "/res";
    config.dbTimeZone = "UTC";
    Record r = RecordCreator.create();
    Map<String, Field> fields = new HashMap<>();
    fields.put("sql", Field.create("INSERT INTO \"SYS\".\"TEST\"(\"ID\", \"NAME\") VALUES ('10', 'Bilbo Baggins')"));
    r.set(Field.create(fields));
    Record r2 = RecordCreator.create();
    fields = new HashMap<>();
    fields.put("sql", Field.create("INSERT INTO \"SYS\".\"TEST\"(\"ID\", \"NAME\") VALUES ('1001', 'Smaug')"));
    r2.set(Field.create(fields));
    SqlParserProcessor processor = new SqlParserProcessor(config);
    ResultSetMetaData rsmd = setupMocks("\"SYS\".\"TEST\"", processor);
    Mockito.when(rsmd.getColumnCount()).thenReturn(2);
    Mockito.when(rsmd.getColumnType(eq(1))).thenReturn(Types.INTEGER);
    Mockito.when(rsmd.getColumnName(eq(1))).thenReturn("ID");
    Mockito.when(rsmd.getColumnType(eq(2))).thenReturn(Types.VARCHAR);
    Mockito.when(rsmd.getColumnName(eq(2))).thenReturn("NAME");
    ProcessorRunner runner = new ProcessorRunner.Builder(SqlParserDProcessor.class, processor)
        .addOutputLane("s").setOnRecordError(OnRecordError.STOP_PIPELINE).build();
    runner.runInit();
    StageRunner.Output output = runner.runProcess(ImmutableList.of(r, r2));
    Record result = output.getRecords().get("s").get(0);
    Assert.assertEquals(10, result.get("/res/ID").getValueAsInteger());
    Assert.assertEquals("Bilbo Baggins", result.get("/res/NAME").getValueAsString());
    assertOperationCode(result, OperationType.INSERT_CODE);
    assertTableSchema(result, "SYS", "TEST");

    result = output.getRecords().get("s").get(1);
    Assert.assertEquals(1001, result.get("/res/ID").getValueAsInteger());
    Assert.assertEquals("Smaug", result.get("/res/NAME").getValueAsString());
    assertOperationCode(result, OperationType.INSERT_CODE);
    assertTableSchema(result, "SYS", "TEST");
    Mockito.verify(st, Mockito.times(1))
        .executeQuery(eq(Utils.format("SELECT * FROM {} WHERE 1 = 0", "\"SYS\"" + ".\"TEST\"")));
  }

  @Test
  public void testResolutionUpdate() throws Exception {
    SqlParserConfigBean config = new SqlParserConfigBean();
    config.resolveSchema = true;
    config.sqlField = "/sql";
    config.resultFieldPath = "/res";
    config.dbTimeZone = "UTC";
    Record r = RecordCreator.create();
    Map<String, Field> fields = new HashMap<>();
    fields.put("sql", Field.create("INSERT INTO \"SYS\".\"TEST\"(\"ID\", \"NAME\") VALUES ('10', 'Bilbo Baggins')"));
    r.set(Field.create(fields));
    SqlParserProcessor processor = new SqlParserProcessor(config);
    ResultSetMetaData rsmd = setupMocks("\"SYS\".\"TEST\"", processor);
    Mockito.when(rsmd.getColumnCount()).thenReturn(2);
    Mockito.when(rsmd.getColumnType(eq(1))).thenReturn(Types.INTEGER);
    Mockito.when(rsmd.getColumnName(eq(1))).thenReturn("ID");
    Mockito.when(rsmd.getColumnType(eq(2))).thenReturn(Types.VARCHAR);
    Mockito.when(rsmd.getColumnName(eq(2))).thenReturn("NAME");
    ProcessorRunner runner = new ProcessorRunner.Builder(SqlParserDProcessor.class, processor)
        .addOutputLane("s").setOnRecordError(OnRecordError.STOP_PIPELINE).build();
    runner.runInit();
    StageRunner.Output output = runner.runProcess(ImmutableList.of(r));
    Record result = output.getRecords().get("s").get(0);
    Assert.assertEquals(10, result.get("/res/ID").getValueAsInteger());
    Assert.assertEquals("Bilbo Baggins", result.get("/res/NAME").getValueAsString());
    assertOperationCode(result, OperationType.INSERT_CODE);
    assertTableSchema(result, "SYS", "TEST");
    Mockito.verify(st, Mockito.times(1))
        .executeQuery(eq(Utils.format("SELECT * FROM {} WHERE 1 = 0", "\"SYS\"" + ".\"TEST\"")));

    r = RecordCreator.create();
    fields = new HashMap<>();
    fields.put("sql", Field.create("INSERT INTO \"SYS\".\"TEST\"(\"ID\", \"NAME\") VALUES ('50', 'Aragon')"));
    r.set(Field.create(fields));
    output = runner.runProcess(ImmutableList.of(r));
    result = output.getRecords().get("s").get(0);
    Assert.assertEquals(50, result.get("/res/ID").getValueAsInteger());
    Assert.assertEquals("Aragon", result.get("/res/NAME").getValueAsString());
    assertOperationCode(result, OperationType.INSERT_CODE);
    assertTableSchema(result, "SYS", "TEST");
    // Schema didn't change, ensure we didn't refresh schema from DB
    Mockito.verify(st, Mockito.times(1))
        .executeQuery(eq(Utils.format("SELECT * FROM {} WHERE 1 = 0", "\"SYS\"" + ".\"TEST\"")));

    r = RecordCreator.create();
    fields = new HashMap<>();
    fields.put("sql", Field.create("INSERT INTO \"SYS\".\"TEST\"(\"ID\", \"NAME\", \"ADDR\") " +
        "VALUES ('100', 'Gandalf', 'MORDOR')"));
    r.set(Field.create(fields));
    Mockito.when(rsmd.getColumnCount()).thenReturn(3);
    Mockito.when(rsmd.getColumnType(eq(3))).thenReturn(Types.VARCHAR);
    Mockito.when(rsmd.getColumnName(eq(3))).thenReturn("ADDR");
    output = runner.runProcess(ImmutableList.of(r));
    result = output.getRecords().get("s").get(0);
    Assert.assertEquals(100, result.get("/res/ID").getValueAsInteger());
    Assert.assertEquals("Gandalf", result.get("/res/NAME").getValueAsString());
    Assert.assertEquals("MORDOR", result.get("/res/ADDR").getValueAsString());
    assertOperationCode(result, OperationType.INSERT_CODE);
    assertTableSchema(result, "SYS", "TEST");
    // Schema changed, so we should have talked to the DB
    Mockito.verify(st, Mockito.times(2))
        .executeQuery(eq(Utils.format("SELECT * FROM {} WHERE 1 = 0", "\"SYS\"" + ".\"TEST\"")));
  }

  @Test
  public void testResolutionTwoTables() throws Exception {
    SqlParserConfigBean config = new SqlParserConfigBean();
    config.resolveSchema = true;
    config.sqlField = "/sql";
    config.resultFieldPath = "/res";
    config.dbTimeZone = "UTC";
    Record r = RecordCreator.create();
    Map<String, Field> fields = new HashMap<>();
    fields.put("sql", Field.create("INSERT INTO \"SYS\".\"TEST\"(\"ID\", \"NAME\") VALUES ('10', 'Bilbo Baggins')"));
    r.set(Field.create(fields));
    Record r2 = RecordCreator.create();
    fields = new HashMap<>();
    fields.put("sql", Field.create("INSERT INTO \"SDC\".\"EDGE\"(\"UN\", \"ADDR\", \"ZIP\") " +
        "VALUES ('900', 'Gondor', '94304')"));
    r2.set(Field.create(fields));
    SqlParserProcessor processor = new SqlParserProcessor(config);
    ResultSetMetaData rsmd = setupMocks("\"SYS\".\"TEST\"", processor);
    Mockito.when(rsmd.getColumnCount()).thenReturn(2);
    Mockito.when(rsmd.getColumnType(eq(1))).thenReturn(Types.INTEGER);
    Mockito.when(rsmd.getColumnName(eq(1))).thenReturn("ID");
    Mockito.when(rsmd.getColumnType(eq(2))).thenReturn(Types.VARCHAR);
    Mockito.when(rsmd.getColumnName(eq(2))).thenReturn("NAME");

    ResultSet rs = Mockito.mock(ResultSet.class);
    ResultSetMetaData rsmd2 = Mockito.mock(ResultSetMetaData.class);
    Mockito
        .when(st.executeQuery(eq(Utils.format("SELECT * FROM {} WHERE 1 = 0", "\"SDC\".\"EDGE\""))))
        .thenReturn(rs);
    Mockito.when(rs.getMetaData()).thenReturn(rsmd2);
    Mockito.when(rsmd2.getColumnCount()).thenReturn(3);
    Mockito.when(rsmd2.getColumnType(eq(1))).thenReturn(Types.INTEGER);
    Mockito.when(rsmd2.getColumnName(eq(1))).thenReturn("UN");
    Mockito.when(rsmd2.getColumnType(eq(2))).thenReturn(Types.VARCHAR);
    Mockito.when(rsmd2.getColumnName(eq(2))).thenReturn("ADDR");
    Mockito.when(rsmd2.getColumnType(eq(3))).thenReturn(Types.INTEGER);
    Mockito.when(rsmd2.getColumnName(eq(3))).thenReturn("ZIP");
    ProcessorRunner runner = new ProcessorRunner.Builder(SqlParserDProcessor.class, processor)
        .addOutputLane("s").setOnRecordError(OnRecordError.STOP_PIPELINE).build();
    runner.runInit();
    StageRunner.Output output = runner.runProcess(ImmutableList.of(r, r2));
    Record result = output.getRecords().get("s").get(0);
    Assert.assertEquals(10, result.get("/res/ID").getValueAsInteger());
    Assert.assertEquals("Bilbo Baggins", result.get("/res/NAME").getValueAsString());
    assertOperationCode(result, OperationType.INSERT_CODE);
    assertTableSchema(result, "SYS", "TEST");

    result = output.getRecords().get("s").get(1);
    Assert.assertEquals(900, result.get("/res/UN").getValueAsInteger());
    Assert.assertEquals("Gondor", result.get("/res/ADDR").getValueAsString());
    Assert.assertEquals(94304, result.get("/res/ZIP").getValueAsInteger());
    assertOperationCode(result, OperationType.INSERT_CODE);
    assertTableSchema(result, "SDC", "EDGE");

    Mockito.verify(st, Mockito.times(1))
        .executeQuery(eq(Utils.format("SELECT * FROM {} WHERE 1 = 0", "\"SYS\"" + ".\"TEST\"")));

    Mockito.verify(st, Mockito.times(1))
        .executeQuery(eq(Utils.format("SELECT * FROM {} WHERE 1 = 0", "\"SDC\"" + ".\"EDGE\"")));
  }

  @Test
  public void testUpdate() throws Exception {
    SqlParserConfigBean config = new SqlParserConfigBean();
    config.resolveSchema = true;
    config.sqlField = "/sql";
    config.resultFieldPath = "/res";
    config.dbTimeZone = "UTC";
    Record r = RecordCreator.create();
    Map<String, Field> fields = new HashMap<>();
    fields.put("sql", Field.create("UPDATE \"SYS\".\"TEST\" SET \"ID\" = '10', \"NAME\" = 'Aragon' WHERE \"ID\" = " +
        "'10' AND \"NAME\" = 'Strider'"));
    r.set(Field.create(fields));
    SqlParserProcessor processor = new SqlParserProcessor(config);
    ResultSetMetaData rsmd = setupMocks("\"SYS\".\"TEST\"", processor);
    Mockito.when(rsmd.getColumnCount()).thenReturn(2);
    Mockito.when(rsmd.getColumnType(eq(1))).thenReturn(Types.INTEGER);
    Mockito.when(rsmd.getColumnName(eq(1))).thenReturn("ID");
    Mockito.when(rsmd.getColumnType(eq(2))).thenReturn(Types.VARCHAR);
    Mockito.when(rsmd.getColumnName(eq(2))).thenReturn("NAME");
    ProcessorRunner runner = new ProcessorRunner.Builder(SqlParserDProcessor.class, processor)
        .addOutputLane("s").setOnRecordError(OnRecordError.STOP_PIPELINE).build();
    runner.runInit();
    StageRunner.Output output = runner.runProcess(ImmutableList.of(r));
    Record result = output.getRecords().get("s").get(0);
    Assert.assertEquals(10, result.get("/res/ID").getValueAsInteger());
    Assert.assertEquals("Aragon", result.get("/res/NAME").getValueAsString());
    assertOperationCode(result, OperationType.UPDATE_CODE);
    assertTableSchema(result, "SYS", "TEST");
  }

  @Test
  public void testDelete() throws Exception {
    SqlParserConfigBean config = new SqlParserConfigBean();
    config.resolveSchema = true;
    config.sqlField = "/sql";
    config.resultFieldPath = "/res";
    config.dbTimeZone = "UTC";
    Record r = RecordCreator.create();
    Map<String, Field> fields = new HashMap<>();
    fields.put("sql", Field.create("DELETE FROM \"SYS\".\"TEST\" WHERE \"ID\" = '10' AND \"NAME\" = 'Boromir'"));
    r.set(Field.create(fields));
    SqlParserProcessor processor = new SqlParserProcessor(config);
    ResultSetMetaData rsmd = setupMocks("\"SYS\".\"TEST\"", processor);
    Mockito.when(rsmd.getColumnCount()).thenReturn(2);
    Mockito.when(rsmd.getColumnType(eq(1))).thenReturn(Types.INTEGER);
    Mockito.when(rsmd.getColumnName(eq(1))).thenReturn("ID");
    Mockito.when(rsmd.getColumnType(eq(2))).thenReturn(Types.VARCHAR);
    Mockito.when(rsmd.getColumnName(eq(2))).thenReturn("NAME");
    ProcessorRunner runner = new ProcessorRunner.Builder(SqlParserDProcessor.class, processor)
        .addOutputLane("s").setOnRecordError(OnRecordError.STOP_PIPELINE).build();
    runner.runInit();
    StageRunner.Output output = runner.runProcess(ImmutableList.of(r));
    Record result = output.getRecords().get("s").get(0);
    Assert.assertEquals(10, result.get("/res/ID").getValueAsInteger());
    Assert.assertEquals("Boromir", result.get("/res/NAME").getValueAsString());
    assertOperationCode(result, OperationType.DELETE_CODE);
    assertTableSchema(result, "SYS", "TEST");
  }

  @Test
  public void testUnsupportedToError() throws Exception {
    SqlParserConfigBean config = new SqlParserConfigBean();
    config.resolveSchema = true;
    config.sqlField = "/sql";
    config.resultFieldPath = "/res";
    config.dbTimeZone = "UTC";
    config.sendUnsupportedFields = false;
    config.unsupportedFieldOp = UnsupportedFieldTypeValues.TO_ERROR;
    Record r = RecordCreator.create();
    Map<String, Field> fields = new HashMap<>();
    fields.put("sql", Field.create("INSERT INTO \"SYS\".\"TEST\"(\"ID\", \"CL\") VALUES ('10', EMPTY_CLOB())"));
    r.set(Field.create(fields));
    SqlParserProcessor processor = new SqlParserProcessor(config);
    ResultSetMetaData rsmd = setupMocks("\"SYS\".\"TEST\"", processor);
    Mockito.when(rsmd.getColumnCount()).thenReturn(2);
    Mockito.when(rsmd.getColumnType(eq(1))).thenReturn(Types.INTEGER);
    Mockito.when(rsmd.getColumnName(eq(1))).thenReturn("ID");
    Mockito.when(rsmd.getColumnType(eq(2))).thenReturn(Types.CLOB);
    Mockito.when(rsmd.getColumnName(eq(2))).thenReturn("CL");
    ProcessorRunner runner = new ProcessorRunner.Builder(SqlParserDProcessor.class, processor)
        .addOutputLane("s").setOnRecordError(OnRecordError.TO_ERROR).build();
    runner.runInit();
    StageRunner.Output output = runner.runProcess(ImmutableList.of(r));
    Assert.assertTrue(output.getRecords().get("s").isEmpty());
    Record result = runner.getErrorRecords().get(0);
    Assert.assertEquals(10, result.get("/res/ID").getValueAsInteger());
    Assert.assertNull(result.get("/res/CL"));
    assertOperationCode(result, OperationType.INSERT_CODE);
    assertTableSchema(result, "SYS", "TEST");
  }

  @Test
  public void testUnsupportedSendDataToError() throws Exception {
    SqlParserConfigBean config = new SqlParserConfigBean();
    config.resolveSchema = true;
    config.sqlField = "/sql";
    config.resultFieldPath = "/res";
    config.dbTimeZone = "UTC";
    config.sendUnsupportedFields = true;
    config.unsupportedFieldOp = UnsupportedFieldTypeValues.TO_ERROR;
    Record r = RecordCreator.create();
    Map<String, Field> fields = new HashMap<>();
    fields.put("sql", Field.create("INSERT INTO \"SYS\".\"TEST\"(\"ID\", \"CL\") VALUES ('10', EMPTY_CLOB())"));
    r.set(Field.create(fields));
    SqlParserProcessor processor = new SqlParserProcessor(config);
    ResultSetMetaData rsmd = setupMocks("\"SYS\".\"TEST\"", processor);
    Mockito.when(rsmd.getColumnCount()).thenReturn(2);
    Mockito.when(rsmd.getColumnType(eq(1))).thenReturn(Types.INTEGER);
    Mockito.when(rsmd.getColumnName(eq(1))).thenReturn("ID");
    Mockito.when(rsmd.getColumnType(eq(2))).thenReturn(Types.CLOB);
    Mockito.when(rsmd.getColumnName(eq(2))).thenReturn("CL");
    ProcessorRunner runner = new ProcessorRunner.Builder(SqlParserDProcessor.class, processor)
        .addOutputLane("s").setOnRecordError(OnRecordError.TO_ERROR).build();
    runner.runInit();
    StageRunner.Output output = runner.runProcess(ImmutableList.of(r));
    Assert.assertTrue(output.getRecords().get("s").isEmpty());
    Record result = runner.getErrorRecords().get(0);
    Assert.assertEquals(10, result.get("/res/ID").getValueAsInteger());
    Assert.assertEquals("EMPTY_CLOB()", result.get("/res/CL").getValueAsString());
    assertOperationCode(result, OperationType.INSERT_CODE);
    assertTableSchema(result, "SYS", "TEST");
  }

  @Test
  public void testUnsupportedToPipeline() throws Exception {
    SqlParserConfigBean config = new SqlParserConfigBean();
    config.resolveSchema = true;
    config.sqlField = "/sql";
    config.resultFieldPath = "/res";
    config.dbTimeZone = "UTC";
    config.sendUnsupportedFields = false;
    config.unsupportedFieldOp = UnsupportedFieldTypeValues.SEND_TO_PIPELINE;
    Record r = RecordCreator.create();
    Map<String, Field> fields = new HashMap<>();
    fields.put("sql", Field.create("INSERT INTO \"SYS\".\"TEST\"(\"ID\", \"CL\") VALUES ('10', EMPTY_CLOB())"));
    r.set(Field.create(fields));
    SqlParserProcessor processor = new SqlParserProcessor(config);
    ResultSetMetaData rsmd = setupMocks("\"SYS\".\"TEST\"", processor);
    Mockito.when(rsmd.getColumnCount()).thenReturn(2);
    Mockito.when(rsmd.getColumnType(eq(1))).thenReturn(Types.INTEGER);
    Mockito.when(rsmd.getColumnName(eq(1))).thenReturn("ID");
    Mockito.when(rsmd.getColumnType(eq(2))).thenReturn(Types.CLOB);
    Mockito.when(rsmd.getColumnName(eq(2))).thenReturn("CL");
    ProcessorRunner runner = new ProcessorRunner.Builder(SqlParserDProcessor.class, processor)
        .addOutputLane("s").setOnRecordError(OnRecordError.TO_ERROR).build();
    runner.runInit();
    StageRunner.Output output = runner.runProcess(ImmutableList.of(r));
    Assert.assertTrue(runner.getErrorRecords().isEmpty());
    Record result = output.getRecords().get("s").get(0);
    Assert.assertEquals(10, result.get("/res/ID").getValueAsInteger());
    Assert.assertNull(result.get("/res/CL"));
    assertOperationCode(result, OperationType.INSERT_CODE);
    assertTableSchema(result, "SYS", "TEST");
  }

  @Test
  public void testUnsupportedSendDataToPipeline() throws Exception {
    SqlParserConfigBean config = new SqlParserConfigBean();
    config.resolveSchema = true;
    config.sqlField = "/sql";
    config.resultFieldPath = "/res";
    config.dbTimeZone = "UTC";
    config.sendUnsupportedFields = true;
    config.unsupportedFieldOp = UnsupportedFieldTypeValues.SEND_TO_PIPELINE;
    Record r = RecordCreator.create();
    Map<String, Field> fields = new HashMap<>();
    fields.put("sql", Field.create("INSERT INTO \"SYS\".\"TEST\"(\"ID\", \"CL\") VALUES ('10', EMPTY_CLOB())"));
    r.set(Field.create(fields));
    SqlParserProcessor processor = new SqlParserProcessor(config);
    ResultSetMetaData rsmd = setupMocks("\"SYS\".\"TEST\"", processor);
    Mockito.when(rsmd.getColumnCount()).thenReturn(2);
    Mockito.when(rsmd.getColumnType(eq(1))).thenReturn(Types.INTEGER);
    Mockito.when(rsmd.getColumnName(eq(1))).thenReturn("ID");
    Mockito.when(rsmd.getColumnType(eq(2))).thenReturn(Types.CLOB);
    Mockito.when(rsmd.getColumnName(eq(2))).thenReturn("CL");
    ProcessorRunner runner = new ProcessorRunner.Builder(SqlParserDProcessor.class, processor)
        .addOutputLane("s").setOnRecordError(OnRecordError.TO_ERROR).build();
    runner.runInit();
    StageRunner.Output output = runner.runProcess(ImmutableList.of(r));
    Assert.assertTrue(runner.getErrorRecords().isEmpty());
    Record result = output.getRecords().get("s").get(0);
    Assert.assertEquals(10, result.get("/res/ID").getValueAsInteger());
    Assert.assertEquals("EMPTY_CLOB()", result.get("/res/CL").getValueAsString());
    assertOperationCode(result, OperationType.INSERT_CODE);
    assertTableSchema(result, "SYS", "TEST");
  }

  @Test
  public void testUnsupportedDiscard() throws Exception {
    SqlParserConfigBean config = new SqlParserConfigBean();
    config.resolveSchema = true;
    config.sqlField = "/sql";
    config.resultFieldPath = "/res";
    config.dbTimeZone = "UTC";
    config.unsupportedFieldOp = UnsupportedFieldTypeValues.DISCARD;
    Record r = RecordCreator.create();
    Map<String, Field> fields = new HashMap<>();
    fields.put("sql", Field.create("INSERT INTO \"SYS\".\"TEST\"(\"ID\", \"CL\") VALUES ('10', EMPTY_CLOB())"));
    r.set(Field.create(fields));
    SqlParserProcessor processor = new SqlParserProcessor(config);
    ResultSetMetaData rsmd = setupMocks("\"SYS\".\"TEST\"", processor);
    Mockito.when(rsmd.getColumnCount()).thenReturn(2);
    Mockito.when(rsmd.getColumnType(eq(1))).thenReturn(Types.INTEGER);
    Mockito.when(rsmd.getColumnName(eq(1))).thenReturn("ID");
    Mockito.when(rsmd.getColumnType(eq(2))).thenReturn(Types.CLOB);
    Mockito.when(rsmd.getColumnName(eq(2))).thenReturn("CL");
    ProcessorRunner runner = new ProcessorRunner.Builder(SqlParserDProcessor.class, processor)
        .addOutputLane("s").setOnRecordError(OnRecordError.TO_ERROR).build();
    runner.runInit();
    StageRunner.Output output = runner.runProcess(ImmutableList.of(r));
    Assert.assertTrue(runner.getErrorRecords().isEmpty());
    Assert.assertTrue(output.getRecords().get("s").isEmpty());
  }

  private ResultSetMetaData setupMocks(String schemaTable, SqlParserProcessor processor) throws SQLException {
    Connection connection = Mockito.mock(Connection.class);
    processor.connection = connection;
    st = Mockito.mock(Statement.class);
    ResultSet rs = Mockito.mock(ResultSet.class);
    ResultSetMetaData rsmd = Mockito.mock(ResultSetMetaData.class);
    Mockito.when(connection.createStatement()).thenReturn(st);
    Mockito
        .when(st.executeQuery(eq(Utils.format("SELECT * FROM {} WHERE 1 = 0", schemaTable))))
        .thenReturn(rs);
    Mockito.when(rs.getMetaData()).thenReturn(rsmd);
    return rsmd;
  }

  @Test
  public void testObjectToFieldEmptyString() throws Exception {
    Map<String, Integer> tableColumns = new HashMap<>();
    for (JDBCType jdbcTpe : JDBCType.values()) {
      switch (jdbcTpe) {
        case BIGINT:
        case BINARY:
        case LONGVARBINARY:
        case VARBINARY:
        case BIT:
        case BOOLEAN:
        case CHAR:
        case LONGNVARCHAR:
        case LONGVARCHAR:
        case NCHAR:
        case NVARCHAR:
        case VARCHAR:
        case DECIMAL:
        case NUMERIC:
        case DOUBLE:
        case FLOAT:
        case REAL:
        case INTEGER:
        case SMALLINT:
        case TINYINT:
          ParseUtil.generateField(
              true,
              jdbcTpe.getName(),
              "",
              jdbcTpe.getVendorTypeNumber(),
              null);
          ParseUtil.generateField(
              true,
              jdbcTpe.getName(),
              null,
              jdbcTpe.getVendorTypeNumber(),
              null);
          break;
        default:
          break;
      }
    }
  }
}